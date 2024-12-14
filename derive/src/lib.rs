mod attributes;

use proc_macro2::TokenStream;
use quote::quote;
use serde_derive_internals::{
    attr::{Container, Default as SerdeDefault, Field},
    Ctxt,
};
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields};

struct Attributes {
    row: attributes::Row,
}

impl From<&[syn::Attribute]> for Attributes {
    fn from(attrs: &[syn::Attribute]) -> Self {
        let mut row = None;
        for attr in attrs {
            if let Ok(r) = attributes::Row::try_from(attr) {
                row = Some(r);
            }
        }
        Self {
            row: row.unwrap_or_default(),
        }
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
    let Attributes {
        row: attributes::Row { crate_path },
    } = Attributes::from(input.attrs.as_slice());
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
        impl #impl_generics #crate_path::Row for #name #ty_generics #where_clause {
            const COLUMN_NAMES: &'static [&'static str] = #column_names;
        }
    };

    proc_macro::TokenStream::from(expanded)
}
