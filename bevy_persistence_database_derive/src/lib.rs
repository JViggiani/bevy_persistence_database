#![allow(non_snake_case)]
extern crate ctor;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Item, ItemFn, LitStr, Meta, parse_macro_input, punctuated::Punctuated, token::Comma};

// New attribute macro: single annotation for derive + registration
#[proc_macro_attribute]
pub fn persist(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse component/resource list: e.g. #[persist(component,resource)]
    let metas = parse_macro_input!(attr with Punctuated::<Meta, Comma>::parse_terminated);
    let is_comp = metas
        .iter()
        .any(|m| matches!(m, Meta::Path(p) if p.is_ident("component")));
    let is_res = metas
        .iter()
        .any(|m| matches!(m, Meta::Path(p) if p.is_ident("resource")));
    let is_rel = metas
        .iter()
        .any(|m| matches!(m, Meta::Path(p) if p.is_ident("relationship")));

    // No effect if neither
    if !is_comp && !is_res && !is_rel {
        return item;
    }

    // Parse the input item (struct or enum)
    let mut ast = parse_macro_input!(item as Item);

    // Derive Component/Resource and serde for non-unit types, skip serde for unit structs to allow manual impls
    if let Item::Struct(s) = &mut ast {
        if is_comp {
            s.attrs.push(syn::parse_quote!(
                #[derive(::bevy::prelude::Component, ::serde::Serialize, ::serde::Deserialize)]
            ));
            // Add `#[serde(transparent)]` to single-field tuple structs
            if let syn::Fields::Unnamed(fields) = &s.fields {
                if fields.unnamed.len() == 1 {
                    s.attrs.push(syn::parse_quote!(#[serde(transparent)]));
                }
            }
        } else if is_res {
            s.attrs.push(syn::parse_quote!(
                #[derive(::bevy::prelude::Resource, ::serde::Serialize, ::serde::Deserialize)]
            ));
            if let syn::Fields::Unnamed(fields) = &s.fields {
                if fields.unnamed.len() == 1 {
                    s.attrs.push(syn::parse_quote!(#[serde(transparent)]));
                }
            }
        } else {
            // Relationship: Serialize/Deserialize only needed for the bevy_many_relationship_edges
            // payload path. The native Bevy relationship path (feature off) uses structs that
            // contain Entity and must NOT derive serde.
            s.attrs.push(syn::parse_quote!(
                #[cfg_attr(feature = "bevy_many_relationship_edges", derive(::serde::Serialize, ::serde::Deserialize))]
            ));
            // #[serde(transparent)] is likewise only valid in the payload path
            if let syn::Fields::Unnamed(fields) = &s.fields {
                if fields.unnamed.len() == 1 {
                    s.attrs.push(syn::parse_quote!(
                        #[cfg_attr(feature = "bevy_many_relationship_edges", serde(transparent))]
                    ));
                }
            }
        }
    } else if let Item::Enum(e) = &mut ast {
        // enums always derive serde
        let derive_list = if is_comp {
            quote! { ::bevy::prelude::Component, ::serde::Serialize, ::serde::Deserialize }
        } else if is_res {
            quote! { ::bevy::prelude::Resource, ::serde::Serialize, ::serde::Deserialize }
        } else {
            quote! { ::serde::Serialize, ::serde::Deserialize }
        };
        e.attrs.push(syn::parse_quote!(#[derive(#derive_list)]));
    }

    // Registration boilerplate
    let name = match &ast {
        Item::Struct(s) => &s.ident,
        Item::Enum(e) => &e.ident,
        _ => unreachable!(),
    };
    let register_fn = format_ident!("__persist_register_{}", name);
    let ctor_fn = format_ident!("__persist_ctor_{}", name);
    let crate_path = get_crate_path();

    let registration = if is_comp {
        quote! {
            #[allow(non_snake_case)]
            fn #register_fn(app: &mut bevy::app::App) {
                use bevy::prelude::IntoScheduleConfigs;
                let type_id = std::any::TypeId::of::<#name>();
                let mut registered = app
                    .world_mut()
                    .resource_mut::<#crate_path::bevy::plugins::persistence_plugin::RegisteredPersistTypes>();
                if registered.types.insert(type_id) {
                    app.world_mut()
                        .resource_mut::<#crate_path::core::session::PersistenceSession>()
                        .register_component::<#name>();
                    app.add_systems(
                        bevy::app::PostUpdate,
                        #crate_path::bevy::plugins::persistence_plugin::auto_dirty_tracking_entity_system::<#name>
                            .in_set(#crate_path::bevy::plugins::persistence_plugin::PersistenceSystemSet::ChangeDetection),
                    );
                }
            }
        }
    } else if is_res {
        quote! {
            #[allow(non_snake_case)]
            fn #register_fn(app: &mut bevy::app::App) {
                use bevy::prelude::IntoScheduleConfigs;
                let type_id = std::any::TypeId::of::<#name>();
                let mut registered = app
                    .world_mut()
                    .resource_mut::<#crate_path::bevy::plugins::persistence_plugin::RegisteredPersistTypes>();
                if registered.types.insert(type_id) {
                    app.world_mut()
                        .resource_mut::<#crate_path::core::session::PersistenceSession>()
                        .register_resource::<#name>();
                    app.add_systems(
                        bevy::app::PostUpdate,
                        #crate_path::bevy::plugins::persistence_plugin::auto_dirty_tracking_resource_system::<#name>
                            .in_set(#crate_path::bevy::plugins::persistence_plugin::PersistenceSystemSet::ChangeDetection),
                    );
                }
            }
        }
    } else {
        quote! {
            #[allow(non_snake_case)]
            fn #register_fn(app: &mut bevy::app::App) {
                #[cfg(feature = "bevy_many_relationship_edges")]
                {
                    use bevy::prelude::IntoScheduleConfigs;
                    let type_id = std::any::TypeId::of::<#name>();
                    let mut registered = app
                        .world_mut()
                        .resource_mut::<#crate_path::bevy::plugins::persistence_plugin::RegisteredPersistTypes>();
                    if registered.types.insert(type_id) {
                        app.world_mut()
                            .resource_mut::<#crate_path::core::session::PersistenceSession>()
                            .register_many_relationship::<#name>(stringify!(#name));
                        app.add_systems(
                            bevy::app::PostUpdate,
                            #crate_path::bevy::plugins::persistence_plugin::auto_dirty_tracking_relationship_system::<#name>
                                .in_set(#crate_path::bevy::plugins::persistence_plugin::PersistenceSystemSet::ChangeDetection),
                        );
                    }
                }
                #[cfg(not(feature = "bevy_many_relationship_edges"))]
                {
                    use bevy::prelude::IntoScheduleConfigs;
                    let type_id = std::any::TypeId::of::<#name>();
                    let mut registered = app
                        .world_mut()
                        .resource_mut::<#crate_path::bevy::plugins::persistence_plugin::RegisteredPersistTypes>();
                    if registered.types.insert(type_id) {
                        app.world_mut()
                            .resource_mut::<#crate_path::core::session::PersistenceSession>()
                            .register_bevy_relationship::<#name>(stringify!(#name));
                        app.add_systems(
                            bevy::app::PostUpdate,
                            #crate_path::bevy::plugins::persistence_plugin::auto_dirty_tracking_bevy_relationship_system::<#name>
                                .in_set(#crate_path::bevy::plugins::persistence_plugin::PersistenceSystemSet::ChangeDetection),
                        );
                    }
                }
            }
        }
    };

    let ctor_registration = quote! {
        #[ctor::ctor]
        #[allow(non_snake_case)]
        fn #ctor_fn() {
            #crate_path::bevy::registration::COMPONENT_REGISTRY
                .lock()
                .unwrap()
                .push(#register_fn);
        }
    };

    // Implement the Persist trait, providing the name() method.
    // For native Bevy relationships (non-feature path), the struct contains Entity and cannot
    // implement Serialize/Deserialize, so we skip the Persist impl (which has serde supertraits).
    let impl_persist = if is_rel {
        quote! {
            #[cfg(feature = "bevy_many_relationship_edges")]
            impl #crate_path::core::persist::Persist for #name {
                fn name() -> &'static str {
                    stringify!(#name)
                }
            }
        }
    } else {
        quote! {
            impl #crate_path::core::persist::Persist for #name {
                fn name() -> &'static str {
                    stringify!(#name)
                }
            }
        }
    };

    // Generate field accessor methods for structs (only meaningful for payload types that have Persist).
    let mut field_methods = quote! {};
    if let Item::Struct(ref s) = ast {
        let methods = s.fields.iter().filter_map(|f| f.ident.as_ref()).map(|ident| {
            let field_str = LitStr::new(&ident.to_string(), proc_macro2::Span::call_site());
            quote! {
                pub fn #ident() -> #crate_path::core::query::filter_expression::FilterExpression {
                    #crate_path::core::query::filter_expression::FilterExpression::field(<Self as #crate_path::core::persist::Persist>::name(), #field_str)
                }
            }
        });
        let methods_body = quote! {
            impl #name {
                #(#methods)*
            }
        };
        field_methods = if is_rel {
            quote! {
                #[cfg(feature = "bevy_many_relationship_edges")]
                #methods_body
            }
        } else {
            methods_body
        };
    }

    // Combine all generated code
    TokenStream::from(quote! {
        #ast
        #registration
        #ctor_registration
        #impl_persist
        #field_methods
    })
}

#[proc_macro_attribute]
pub fn db_matrix_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;
    let orig_name = &sig.ident;

    // Preserve attributes except #[test]; we'll add #[test] to generated fns
    let passthrough_attrs: Vec<syn::Attribute> = input
        .attrs
        .into_iter()
        .filter(|a| !a.path().is_ident("test"))
        .collect();

    // Helper to build a backend-gated test
    let gen_backend_test = |suffix: &str,
                            backend_expr: proc_macro2::TokenStream,
                            cfg: proc_macro2::TokenStream| {
        let fn_name = format_ident!("{}_{}", orig_name, suffix);
        let attrs = &passthrough_attrs;

        // Runtime skip if env excludes this backend
        let skip_check = quote! {
            let wants = std::env::var("bevy_persistence_database_TEST_BACKENDS").unwrap_or_default();
            if !wants.is_empty() {
                let mut enabled = false;
                for token in wants.split(',').map(|s| s.trim().to_ascii_lowercase()).filter(|s| !s.is_empty()) {
                    if token == #suffix {
                        enabled = true;
                        break;
                    }
                }
                if !enabled {
                    eprintln!("skipping {} due to bevy_persistence_database_TEST_BACKENDS={}", stringify!(#fn_name), wants);
                    return;
                }
            }
        };

        quote! {
            #cfg
            #(#attrs)*
            #[test]
            #vis fn #fn_name() {
                #skip_check
                let backend = #backend_expr;
                let setup = || crate::common::setup_backend(backend);
                #body
            }
        }
    };

    // Optional: rust-analyzer-friendly fallback via feature, no unknown cfg used
    #[cfg(feature = "ra-fallback")]
    {
        let attrs = &passthrough_attrs;
        let expanded = quote! {
            #(#attrs)*
            #[test]
            #vis fn #orig_name() {
                // Pick one backend that is enabled; prefer postgres if available
                #[allow(unused_mut)]
                let mut backend_opt = None;
                #[cfg(feature = "postgres")]
                { backend_opt = Some(crate::common::TestBackend::Postgres); }
                #[cfg(all(not(feature = "postgres"), feature = "arango"))]
                { backend_opt = Some(crate::common::TestBackend::Arango); }
                let backend = backend_opt.expect("No backend feature enabled");
                let setup = || crate::common::setup_backend(backend);
                #body
            }
        };
        return TokenStream::from(expanded);
    }

    // Default: real per-backend tests (no cfg(rust_analyzer) usage)
    #[cfg(not(feature = "ra-fallback"))]
    {
        let arango_test = gen_backend_test(
            "db_arango",
            quote!(crate::common::TestBackend::Arango),
            quote!(#[cfg(feature = "arango")]),
        );

        let postgres_test = gen_backend_test(
            "db_postgres",
            quote!(crate::common::TestBackend::Postgres),
            quote!(#[cfg(feature = "postgres")]),
        );

        let expanded = quote! {
            #arango_test
            #postgres_test
        };
        return TokenStream::from(expanded);
    }
}

fn get_crate_path() -> proc_macro2::TokenStream {
    use proc_macro_crate::{FoundCrate, crate_name};

    // First check if we're in the bevy_persistence_database crate
    if let Ok(FoundCrate::Itself) = crate_name("bevy_persistence_database") {
        return quote!(crate);
    }

    // Then check if bevy_persistence_database is a dependency
    if let Ok(FoundCrate::Name(name)) = crate_name("bevy_persistence_database") {
        let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
        return quote!(::#ident);
    }

    // If neither case matched, default to bevy_persistence_database
    quote!(::bevy_persistence_database)
}
