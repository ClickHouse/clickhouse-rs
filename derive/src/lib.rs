use proc_macro2::TokenStream;
use quote::quote;
use serde_derive_internals::attr::{Container, RenameRule};
use serde_derive_internals::{
    attr::{Default as SerdeDefault, Field},
    Ctxt,
};
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields};

fn column_names(data: &DataStruct, cx: Ctxt, container: &Container) -> TokenStream {
    match &data.fields {
        Fields::Named(fields) => {
            let rename_rule = container.rename_all_rules().deserialize;
            let column_names_iter = fields
                .named
                .iter()
                .enumerate()
                .map(|(index, field)| Field::from_ast(&cx, index, field, None, &SerdeDefault::None))
                .filter(|field| !field.skip_serializing() && !field.skip_deserializing())
                .map(|field| {
                    rename_rule
                        .apply_to_field(field.name().serialize_name())
                        .to_string()
                });

            let tokens = quote! {
                &[#( #column_names_iter,)*]
            };

            // TODO: do something more clever?
            let _ = cx.check();
            tokens
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
#[proc_macro_derive(Row)]
pub fn row(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let cx = Ctxt::new();
    let container = Container::from_ast(&cx, &input);
    let name = input.ident;

    let column_names = match &input.data {
        Data::Struct(data) => column_names(data, cx, &container),
        Data::Enum(_) | Data::Union(_) => panic!("`Row` can be derived only for structs"),
    };

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // TODO: replace `clickhouse` with `::clickhouse` here.
    let expanded = quote! {
        #[automatically_derived]
        impl #impl_generics clickhouse::Row for #name #ty_generics #where_clause {
            const COLUMN_NAMES: &'static [&'static str] = #column_names;
        }
    };

    proc_macro::TokenStream::from(expanded)
}
