//! Troubleshooting log of one invocation.

use std::io::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const LOG_PATH_ENV: &str = "CLICKHOUSE_CLIENT_CLI_LOG";

/// Collects troubleshooting lines for one invocation and appends them to the
/// file named by `CLICKHOUSE_CLIENT_CLI_LOG`. Without that variable, it does
/// nothing. The lines are written in one write, so the lines of parallel
/// invocations do not mix.
pub(crate) struct InvocationLog {
    path: Option<String>,
    lines: String,
}

impl InvocationLog {
    pub(crate) fn new(path: Option<String>) -> Self {
        let mut log = InvocationLog {
            path: path.filter(|p| !p.is_empty()),
            lines: String::new(),
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| format!("{}.{:06}", d.as_secs(), d.subsec_micros()))
            .unwrap_or_default();
        log.add("=== clickhouse-client invocation", &now);
        log
    }

    pub(crate) fn add(&mut self, key: &str, value: &str) {
        if self.path.is_none() {
            return;
        }
        self.lines.push_str(key);
        self.lines.push('=');
        self.lines.push_str(&value.replace('\n', "\\n"));
        self.lines.push('\n');
    }

    /// Appends the lines to the log file. A log that cannot be written must
    /// not change the result of a test, so a write error is ignored here; the
    /// missing lines in the log show it.
    pub(crate) fn flush(&mut self) {
        let Some(path) = &self.path else {
            return;
        };
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
        {
            let _ = f.write_all(self.lines.as_bytes());
        }
        self.lines.clear();
    }
}

impl Drop for InvocationLog {
    fn drop(&mut self) {
        self.flush();
    }
}
