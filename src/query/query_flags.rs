/// Query interpolation flags for controlling SQL template processing.
///
/// This struct provides compile-time constants that control how SQL templates
/// are processed, specifically for `?` parameter binding and `?fields` substitution.
#[derive(Clone)]
pub struct QI;

impl QI {
    /// No interpolation features enabled. `?` becomes `NULL`, `?fields` is skipped. Implemented only for test purposes
    pub const NONE: u8 = 0;

    /// Enable `?fields` substitution with struct field names.
    pub const FIELDS: u8 = 0b0001;

    /// Enable `?` parameter binding with `.bind()` method.
    pub const BIND: u8 = 0b0010;

    /// Default flags used by `.query()` method.
    ///
    /// By default, only `?fields` substitution is enabled. Parameter binding (`?`)
    /// is opt-in via `Client::query_with_flags`.
    pub const DEFAULT: u8 = QI::FIELDS;

    /// All interpolation features enabled.
    pub const ALL: u8 = QI::FIELDS | QI::BIND;

    /// Compile-time flag checking functions
    #[inline(always)]
    pub const fn has_fields(flags: u8) -> bool {
        (flags & Self::FIELDS) != 0
    }

    #[inline(always)]
    pub const fn has_bind(flags: u8) -> bool {
        (flags & Self::BIND) != 0
    }
}
