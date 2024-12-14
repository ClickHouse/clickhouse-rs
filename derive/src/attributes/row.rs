use quote::ToTokens;

pub const ATTRIBUTE_NAME: &str = "row";
pub const ATTRIBUTE_SYNTAX: &str = "#[row(crate = ...)]";

pub const CRATE_PATH: &str = "crate";
pub const DEFAULT_CRATE_PATH: &str = "::clickhouse";

pub struct Row {
    pub crate_path: syn::Path,
}

impl Default for Row {
    fn default() -> Self {
        let default_crate_path = syn::parse_str::<syn::Path>(DEFAULT_CRATE_PATH).unwrap();
        Self {
            crate_path: default_crate_path,
        }
    }
}

impl<'a> TryFrom<&'a syn::Attribute> for Row {
    type Error = &'a syn::Attribute;

    fn try_from(attr: &'a syn::Attribute) -> Result<Self, Self::Error> {
        if attr.path().is_ident(ATTRIBUTE_NAME) {
            let row = attr.parse_args::<syn::Expr>().unwrap();
            let syn::Expr::Assign(syn::ExprAssign { left, right, .. }) = row else {
                panic!("expected `{}`", ATTRIBUTE_SYNTAX);
            };
            if left.to_token_stream().to_string() != CRATE_PATH {
                panic!("expected `{}`", ATTRIBUTE_SYNTAX);
            }
            let syn::Expr::Path(syn::ExprPath { path, .. }) = *right else {
                panic!("expected `{}`", ATTRIBUTE_SYNTAX);
            };
            Ok(Self { crate_path: path })
        } else {
            return Err(attr);
        }
    }
}

