//! Observability callbacks for native protocol queries.

use super::protocol::{ProfileInfo, Progress};

/// Observability callbacks for native protocol queries.
///
/// When set, the cursor invokes these as packets arrive from the server.
/// When unset (the default), packets are consumed silently — zero overhead.
pub(crate) struct QueryCallbacks {
    pub(crate) on_progress: Option<Box<dyn Fn(&Progress) + Send + Sync>>,
    pub(crate) on_profile_info: Option<Box<dyn Fn(&ProfileInfo) + Send + Sync>>,
}

impl Default for QueryCallbacks {
    fn default() -> Self {
        Self {
            on_progress: None,
            on_profile_info: None,
        }
    }
}
