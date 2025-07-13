extern crate ctor;

use proc_macro::TokenStream;
use quote::{quote, format_ident};
use syn::{
    parse_macro_input,
    punctuated::Punctuated,
    token::Comma,
    DeriveInput, Item, Meta, Data, Fields,
};

fn get_crate_path() -> proc_macro2::TokenStream {
    use proc_macro_crate::{crate_name, FoundCrate};

    match crate_name("bevy_arangodb_core") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#ident)
        }
        Err(_) => quote!(::bevy_arangodb_core), // Fallback for cases like docs.rs
    }
}

#[proc_macro_derive(Persist)]
pub fn persist_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let crate_path = get_crate_path();

    // --- DEBUG: dump the incoming DeriveInput ---
    eprintln!("--- derive(Persist) for {} ---", name);
    eprintln!("attrs count = {}", input.attrs.len());
    for attr in &input.attrs {
        eprintln!("  attr.path = {:?}", attr.path());
        // print the Meta instead of a non‐existent tokens field
        eprintln!("  attr.meta = {:?}", attr.meta);
    }

    // --- field‐accessors impl ---
    let field_accessors_impl = if let Data::Struct(s) = &input.data {
        if let Fields::Named(fields) = &s.fields {
            let methods = fields.named.iter().map(|f| {
                let fname = f.ident.as_ref().unwrap();
                let fname_str = fname.to_string();
                quote! {
                    #[allow(dead_code)]
                    pub fn #fname() -> #crate_path::query_dsl::Expression {
                        #crate_path::query_dsl::Expression::Field {
                            component_name: <#name as #crate_path::Persist>::name(),
                            field_name: #fname_str,
                        }
                    }
                }
            });
            quote! { impl #name { #(#methods)* } }
        } else { quote!{} }
    } else { quote!{} };

    // --- Persist trait impl ---
    let persist_impl = quote! {
        impl #crate_path::Persist for #name {
            fn name() -> &'static str {
                std::any::type_name::<#name>()
            }
        }
    };

    let expanded = quote! {
        // Persist impl
        #persist_impl
        // field accessors
        #field_accessors_impl
    };

    TokenStream::from(expanded)
}

// New attribute macro: single annotation for derive + registration
#[proc_macro_attribute]
pub fn persist(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse component/resource list: e.g. #[persist(component,resource)]
    let metas = parse_macro_input!(attr with Punctuated::<Meta, Comma>::parse_terminated);
    let is_comp = metas.iter().any(|m| matches!(m, Meta::Path(p) if p.is_ident("component")));
    let is_res  = metas.iter().any(|m| matches!(m, Meta::Path(p) if p.is_ident("resource")));

    // No effect if neither
    if !is_comp && !is_res {
        return item;
    }

    // Parse the input item (struct or enum)
    let mut ast = parse_macro_input!(item as Item);

    // Attach appropriate derives
    let derive_list = if is_comp {
        quote! { Component, Persist }
    } else {
        quote! { Resource, Persist }
    };
    match &mut ast {
        Item::Struct(s) => s.attrs.push(syn::parse_quote!(#[derive(#derive_list)])),
        Item::Enum(e)   => e.attrs.push(syn::parse_quote!(#[derive(#derive_list)])),
        _ => {}
    }

    // Registration boilerplate
    let name       = match &ast {
        Item::Struct(s) => &s.ident,
        Item::Enum(e)   => &e.ident,
        _ => unreachable!(),
    };
    let register_fn = format_ident!("__persist_register_{}", name);
    let ctor_fn     = format_ident!("__persist_ctor_{}", name);
    let crate_path  = get_crate_path();

    let registration = if is_comp {
        quote! {
            #[allow(dead_code)]
            fn #register_fn(app: &mut bevy::app::App) {
                app.world
                    .resource_mut::<#crate_path::ArangoSession>()
                    .register_component::<#name>();
                app.add_systems(
                    bevy::app::PostUpdate,
                    #crate_path::bevy_plugin::auto_dirty_tracking_system::<#name>,
                );
            }
        }
    } else {
        quote! {
            #[allow(dead_code)]
            fn #register_fn(app: &mut bevy::app::App) {
                app.world
                    .resource_mut::<#crate_path::ArangoSession>()
                    .register_resource::<#name>();
            }
        }
    };

    let push = quote! {
        #[ctor::ctor]
        fn #ctor_fn() {
            #crate_path::registration::COMPONENT_REGISTRY
                .lock()
                .unwrap()
                .push(#register_fn);
        }
    };

    TokenStream::from(quote! {
        #ast
        #registration
        #push
    })
}