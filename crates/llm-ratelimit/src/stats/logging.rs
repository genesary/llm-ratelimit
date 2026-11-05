//! gostats' logging sink (`logging_sink.go`): one JSON line per stat on stderr, shaped like a
//! zap production log, encoded as Go's `encoding/json` encodes it.

use std::io::Write;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct LoggingSink {
    out: Mutex<Box<dyn Write + Send>>,
    now: Box<dyn Fn() -> SystemTime + Send + Sync>,
}

impl LoggingSink {
    /// Writes to stderr, stamped with the system clock.
    pub fn stderr() -> Self {
        Self::new(Box::new(std::io::stderr()), Box::new(SystemTime::now))
    }

    pub fn new(out: Box<dyn Write + Send>, now: Box<dyn Fn() -> SystemTime + Send + Sync>) -> Self {
        Self {
            out: Mutex::new(out),
            now,
        }
    }

    pub fn flush_counter(&self, name: &str, value: u64) {
        self.log(Some(name), "counter", value as f64);
    }

    pub fn flush_gauge(&self, name: &str, value: u64) {
        self.log(Some(name), "gauge", value as f64);
    }

    pub fn flush_timer(&self, name: &str, value: f64) {
        self.log(Some(name), "timer", value);
    }

    /// The line closing every flush.
    pub fn flush(&self) {
        self.log(None, "all stats", 0.0);
    }

    pub fn warn(&self, message: &str) {
        self.message("warn", message);
    }

    pub fn error(&self, message: &str) {
        self.message("error", message);
    }

    fn log(&self, name: Option<&str>, kind: &str, value: f64) {
        // Go marshals the map with its keys sorted.
        let mut fields = String::new();
        if let Some(name) = name {
            fields += &format!("\"name\":{},", go_json_string(name));
        }
        fields += &format!("\"type\":{},\"value\":\"{value:.6}\"", go_json_string(kind));
        self.write("debug", &format!("flushing {kind}"), &fields);
    }

    fn message(&self, level: &str, message: &str) {
        self.write(level, message, "");
    }

    fn write(&self, level: &str, message: &str, fields: &str) {
        let since_epoch = (self.now)()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seconds = since_epoch as f64 / 1e9;
        let line = format!(
            "{{\"level\":\"{level}\",\"ts\":{seconds:.6},\"logger\":\"gostats.loggingsink\",\"msg\":{},\"json\":{{{fields}}}}}\n",
            go_json_string(message)
        );
        // Like gostats, a failed write is ignored.
        let _ = self.out.lock().unwrap().write_all(line.as_bytes());
    }
}

/// A JSON string as Go's `encoding/json` writes it: HTML characters and the JavaScript line
/// separators are escaped, control characters as `\u00XX` unless they have a short form.
fn go_json_string(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out += "\\\"",
            '\\' => out += "\\\\",
            '\n' => out += "\\n",
            '\r' => out += "\\r",
            '\t' => out += "\\t",
            '\u{8}' => out += "\\b",
            '\u{c}' => out += "\\f",
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => out += &format!("\\u{:04x}", c as u32),
            c if (c as u32) < 0x20 => out += &format!("\\u{:04x}", c as u32),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
