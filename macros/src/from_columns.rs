use crate::attributes::Attributes;
use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::DeriveInput;
use syn::spanned::Spanned;

pub fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let Attributes { crate_path } = input.attrs[..].try_into()?;

    // **Not** a breaking change here since this is a new derive.
    let crate_path = crate_path.unwrap_or_else(|| syn::parse_str("::clickhouse").expect("BUG: default path should parse"));

    let fields = match &input.data {
        syn::Data::Struct(struct_data) => {
            &struct_data.fields
        }
        _ => return Err(syn::Error::new_spanned(input, "only `struct` is currently accepted")),
    };

    let struct_name = input.ident;
    let block_ident = syn::parse_str::<syn::Ident>("block")
        .expect("BUG: `block_ident` should parse");

    let field_name = fields.iter()
        .map(|field| field.ident.as_ref().ok_or_else(|| syn::Error::new_spanned(field, "tuple structs are not currently supported")))
        .collect::<syn::Result<Vec<_>>>()?;

    let field_iter = fields.iter()
        .map(|field| {
            // We checked for field names once already
            let field_name = field.ident.as_ref().unwrap();
            let field_name_s = field_name.to_string();

            let field_ty = &field.ty;

            quote_spanned! { field.span() =>
                let #field_name = #block_ident.iter::<#field_ty>(#field_name_s);
            }
        });

    Ok(quote! {
        impl<'b> #crate_path::native::from_columns::FromColumns<'b> for #struct_name {
            fn from_columns<O: #crate_path::native::from_columns::UninitBuf<Self>>(
                block: &'b #crate_path::native::Block,
                mut out_buf: O,
            ) -> Result<O::Init, Box<dyn ::std::error::Error>> {
                let max_len = out_buf.check_capacity(block.num_rows());

                #(#field_iter)*

                for _ in 0 .. max_len {
                    #(
                        let Some(#field_name) = #field_name.next().tranpose()? else {
                            break;
                        };
                    )*

                    out_buf.push(
                        Self { #(#field_name),* }
                    );
                }

                Ok(out_buf.into_init())
            }
        }
    })
}
