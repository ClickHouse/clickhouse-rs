use syn::meta::ParseNestedMeta;

#[derive(Default)]
pub struct Attributes {
    pub crate_path: Option<syn::Path>,
}

impl TryFrom<&[syn::Attribute]> for Attributes {
    type Error = syn::Error;

    fn try_from(attrs: &[syn::Attribute]) -> syn::Result<Self> {
        for attr in attrs {
            if attr.path().is_ident("clickhouse") {
                let mut out = Attributes::default();

                attr.parse_nested_meta(|meta| parse_nested_meta(meta, &mut out))?;

                return Ok(out);
            }
        }

        Ok(Self::default())
    }
}

/// Called for each meta-item inside the `#[clickhouse(...)]` attribute.
fn parse_nested_meta(meta: ParseNestedMeta<'_>, out: &mut Attributes) -> syn::Result<()> {
    // #[clickhouse(crate = "<path>")]
    if meta.path.is_ident("crate") {
        out.crate_path = Some(meta
            // Expect and eat the `=` token
            .value()?
            // Expect a string literal like Serde: https://serde.rs/container-attrs.html#crate
            .parse::<syn::LitStr>()?
            // Parse the literal content as `Path`
            .parse()?);
    } else {
        return Err(meta.error("unexpected `#[clickhouse(...)]` argument"));
    }

    Ok(())
}
