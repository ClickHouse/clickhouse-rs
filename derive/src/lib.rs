use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use serde_derive_internals::{
    attr::{Container, Default as SerdeDefault, Field},
    Ctxt,
};
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields};

struct Attributes {
    crate_name: syn::Path,
}

impl From<&[syn::Attribute]> for Attributes {
    fn from(attrs: &[syn::Attribute]) -> Self {
        const ATTRIBUTE_NAME: &str = "row";
        const ATTRIBUTE_SYNTAX: &str = "#[row(crate = ...)]";

        const CRATE_NAME: &str = "crate";
        const DEFAULT_CRATE_NAME: &str = "clickhouse";

        let mut crate_name = None;
        for attr in attrs {
            if attr.path().is_ident(ATTRIBUTE_NAME) {
                let row = attr.parse_args::<syn::Expr>().unwrap();
                let syn::Expr::Assign(syn::ExprAssign { left, right, .. }) = row else {
                    panic!("expected `{}`", ATTRIBUTE_SYNTAX);
                };
                if left.to_token_stream().to_string() != CRATE_NAME {
                    panic!("expected `{}`", ATTRIBUTE_SYNTAX);
                }
                let syn::Expr::Path(syn::ExprPath { path, .. }) = *right else {
                    panic!("expected `{}`", ATTRIBUTE_SYNTAX);
                };
                crate_name = Some(path);
            }
        }
        let crate_name = crate_name.unwrap_or_else(|| {
            syn::Path::from(syn::Ident::new(
                DEFAULT_CRATE_NAME,
                proc_macro2::Span::call_site(),
            ))
        });
        Self { crate_name }
    }
}

fn column_names(data: &DataStruct, cx: &Ctxt, container: &Container) -> TokenStream {
    match &data.fields {
        Fields::Named(fields) => {
            let rename_rule = container.rename_all_rules().deserialize;
            let column_names_iter = fields
                .named
                .iter()
                .enumerate()
                .map(|(index, field)| Field::from_ast(cx, index, field, None, &SerdeDefault::None))
                .filter(|field| !field.skip_serializing() && !field.skip_deserializing())
                .map(|field| {
                    rename_rule
                        .apply_to_field(field.name().serialize_name())
                        .to_string()
                });

            quote! {
                &[#( #column_names_iter,)*]
            }
        }
        Fields::Unnamed(_) => {
            quote! { &[] }
        }
        Fields::Unit => panic!("`Row` cannot be derived for unit structs"),
    }
}

// TODO: support wrappers `Wrapper(Inner)` and `Wrapper<T>(T)`.
// TODO: support the `nested` attribute.
// TODO: support the `crate` attribute.
#[proc_macro_derive(Row, attributes(row))]
pub fn row(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let cx = Ctxt::new();
    let Attributes { crate_name } = Attributes::from(input.attrs.as_slice());
    let container = Container::from_ast(&cx, &input);
    let name = input.ident;

    let column_names = match &input.data {
        Data::Struct(data) => column_names(data, &cx, &container),
        Data::Enum(_) | Data::Union(_) => panic!("`Row` can be derived only for structs"),
    };

    // TODO: do something more clever?
    let _ = cx.check().expect("derive context error");

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        #[automatically_derived]
        impl #impl_generics #crate_name::Row for #name #ty_generics #where_clause {
            const COLUMN_NAMES: &'static [&'static str] = #column_names;
        }
    };

    proc_macro::TokenStream::from(expanded)
}
