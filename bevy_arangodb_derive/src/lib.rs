use proc_macro::TokenStream;
use quote::{quote, ToTokens, format_ident};
use syn::{parse_macro_input, Data, DeriveInput, Fields};

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

    // --- Generate the field accessors inside an `impl` block for the struct ---
    let field_accessors_impl = if let Data::Struct(s) = &input.data {
        if let Fields::Named(fields) = &s.fields {
            let field_methods = fields.named.iter().map(|f| {
                let field_name = f.ident.as_ref().unwrap();
                let field_name_str = field_name.to_string();
                quote! {
                    #[allow(dead_code)]
                    pub fn #field_name() -> #crate_path::query_dsl::Expression {
                        #crate_path::query_dsl::Expression::Field {
                            component_name: <#name as #crate_path::Persist>::name(),
                            field_name: #field_name_str,
                        }
                    }
                }
            });
            quote! { impl #name { #(#field_methods)* } }
        } else {
            quote! {}
        }
    } else {
        quote! {}
    };

    // --- Generate the Persist trait implementation ---
    let persist_impl = quote! {
        impl #crate_path::Persist for #name {
            fn name() -> &'static str {
                std::any::type_name::<#name>()
            }
        }
    };

    // Determine if this type is a Bevy Component
    let is_component = input.attrs.iter().any(|attr| {
        attr.path().is_ident("component")
            || (attr.path().is_ident("derive")
                && attr.meta.to_token_stream().to_string().contains("Component"))
    });

    // --- Generate auto‐dirty‐tracking registration for components ---
    let registration_impl = if is_component {
        let fn_ident = format_ident!("register_component_system_{}", name);
        quote! {
            #[allow(dead_code)]
            fn #fn_ident(app: &mut bevy::app::App) {
                app.add_systems(
                    bevy::app::PostUpdate,
                    #crate_path::bevy_plugin::auto_dirty_tracking_system::<#name>,
                );
            }
            inventory::submit! {
                #crate_path::bevy_plugin::AppRegister(#fn_ident)
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #persist_impl
        #field_accessors_impl
        #registration_impl
    };

    TokenStream::from(expanded)
}
