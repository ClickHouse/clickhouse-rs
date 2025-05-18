#[non_exhaustive]
#[derive(Clone)]
pub enum StructValidationMode {
    FirstRow,
    EachRow,
    Disabled,
}

impl Default for StructValidationMode {
    fn default() -> Self {
        Self::FirstRow
    }
}

impl std::fmt::Display for StructValidationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FirstRow => write!(f, "FirstRow"),
            Self::EachRow => write!(f, "EachRow"),
            Self::Disabled => write!(f, "Disabled"),
        }
    }
}
