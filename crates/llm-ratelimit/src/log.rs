//! Upstream's logger: logrus v1.9.3's standard logger as `configureLogger` sets it up
//! (`LOG_LEVEL`, and `LOG_FORMAT=json` for its JSON formatter with `@timestamp` and
//! `@message`), writing to stderr. Times are in Go's local time zone.

use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{LazyLock, Mutex};

/// A logrus level; a lower one is more severe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Panic = 0,
    Fatal = 1,
    Error = 2,
    Warn = 3,
    Info = 4,
    Debug = 5,
    Trace = 6,
}

impl Level {
    /// `ParseLevel`, case-insensitively.
    pub fn parse(level: &str) -> Result<Self, String> {
        Ok(match level.to_lowercase().as_str() {
            "panic" => Level::Panic,
            "fatal" => Level::Fatal,
            "error" => Level::Error,
            "warn" | "warning" => Level::Warn,
            "info" => Level::Info,
            "debug" => Level::Debug,
            "trace" => Level::Trace,
            _ => {
                return Err(format!(
                    "not a valid logrus Level: {}",
                    crate::go::quote(level)
                ));
            }
        })
    }

    fn name(self) -> &'static str {
        match self {
            Level::Panic => "panic",
            Level::Fatal => "fatal",
            Level::Error => "error",
            Level::Warn => "warning",
            Level::Info => "info",
            Level::Debug => "debug",
            Level::Trace => "trace",
        }
    }

    /// The colour of the level in a terminal.
    fn color(self) -> u8 {
        match self {
            Level::Debug | Level::Trace => 37,
            Level::Warn => 33,
            Level::Error | Level::Fatal | Level::Panic => 31,
            Level::Info => 36,
        }
    }
}

static LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);
static JSON: AtomicBool = AtomicBool::new(false);
/// One line at a time, as logrus' logger mutex.
static OUTPUT: Mutex<()> = Mutex::new(());
/// logrus' `baseTimestamp`, when the package is initialised.
static START: LazyLock<std::time::Instant> = LazyLock::new(std::time::Instant::now);
/// Whether stderr is a terminal, checked once.
static TERMINAL: LazyLock<bool> = LazyLock::new(|| std::io::stderr().is_terminal());
static LOCAL: LazyLock<jiff::tz::TimeZone> = LazyLock::new(local_time_zone);

/// Marks when the process starts, for the elapsed seconds a terminal shows.
pub fn init() {
    LazyLock::force(&START);
}

/// `configureLogger` once the level is parsed: the level, and the JSON formatter when
/// `LOG_FORMAT` is `json` in any case.
pub fn configure(level: Level, format: &str) {
    LEVEL.store(level as u8, Ordering::SeqCst);
    JSON.store(format.to_lowercase() == "json", Ordering::SeqCst);
}

/// Whether a line at `level` is written.
pub fn enabled(level: Level) -> bool {
    level as u8 <= LEVEL.load(Ordering::SeqCst)
}

pub fn info(message: &str) {
    log(Level::Info, message);
}

pub fn warn(message: &str) {
    log(Level::Warn, message);
}

pub fn error(message: &str) {
    log(Level::Error, message);
}

/// `logger.Debugf`: the line, formatted only when the level lets it through.
#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        if $crate::log::enabled($crate::log::Level::Debug) {
            $crate::log::log($crate::log::Level::Debug, &format!($($arg)*));
        }
    };
}

/// `Fatal`: the line if the level lets it through, then exit 1 whatever the level.
pub fn fatal(message: &str) -> ! {
    log(Level::Fatal, message);
    std::process::exit(1);
}

/// What upstream would panic on, which stops the process here: a fatal line written
/// whatever the level, then exit 1 (docs/parity.md §7).
pub fn panic_exit(message: &str) -> ! {
    write(Level::Fatal, message);
    std::process::exit(1);
}

/// What Go's standard logger writes (`log.Print`): the local date and time, then the
/// message, on stderr whatever the level.
pub fn stdlib(message: &str) {
    let now = jiff::Timestamp::now().to_zoned(LOCAL.clone());
    let line = format!("{} {message}\n", now.strftime("%Y/%m/%d %H:%M:%S"));
    let _guard = OUTPUT.lock().unwrap();
    let _ = std::io::stderr().write_all(line.as_bytes());
}

/// A line at `level`, if enabled.
pub fn log(level: Level, message: &str) {
    if enabled(level) {
        write(level, message);
    }
}

fn write(level: Level, message: &str) {
    let now = jiff::Timestamp::now().to_zoned(LOCAL.clone());
    let line = match (JSON.load(Ordering::SeqCst), *TERMINAL) {
        (true, _) => json_line(level, message, &now),
        (false, true) => colored_line(level, message),
        (false, false) => text_line(level, message, &now),
    };
    let _guard = OUTPUT.lock().unwrap();
    // As logrus, a failed write is ignored.
    let _ = std::io::stderr().write_all(line.as_bytes());
}

/// The text formatter's line when stderr is not a terminal: `time`, `level`, then `msg`
/// unless empty, each value quoted when it has a character logrus does not leave bare.
fn text_line(level: Level, message: &str, now: &jiff::Zoned) -> String {
    let mut line = format!(
        "time={} level={}",
        text_value(&rfc3339(now, false)),
        text_value(level.name())
    );
    if !message.is_empty() {
        line += &format!(" msg={}", text_value(message));
    }
    line.push('\n');
    line
}

/// `needsQuoting` then `%q`.
fn text_value(value: &str) -> String {
    let bare = value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "-._/@^+".contains(c));
    match bare {
        true => value.to_string(),
        false => crate::go::quote(value),
    }
}

/// The text formatter's line in a terminal: the coloured level's first four letters, the
/// seconds since start, then the message (without a trailing newline) padded to 44
/// characters.
fn colored_line(level: Level, message: &str) -> String {
    let message = message.strip_suffix('\n').unwrap_or(message);
    let level_text = level.name().to_uppercase();
    let elapsed = START.elapsed().as_secs();
    format!(
        "\x1b[{}m{}\x1b[0m[{elapsed:04}] {message:<44} \n",
        level.color(),
        &level_text[..4]
    )
}

/// The JSON formatter's line: `@message`, `@timestamp` (RFC 3339 with nanoseconds) and
/// `level`, keys sorted.
fn json_line(level: Level, message: &str, now: &jiff::Zoned) -> String {
    format!(
        "{{\"@message\":{},\"@timestamp\":{},\"level\":{}}}\n",
        crate::go::json_string(message.as_bytes()),
        crate::go::json_string(rfc3339(now, true).as_bytes()),
        crate::go::json_string(level.name().as_bytes())
    )
}

/// Go's `time.RFC3339` (or `RFC3339Nano`, with its fraction's trailing zeros trimmed): `Z`
/// for a zero offset.
fn rfc3339(now: &jiff::Zoned, nanos: bool) -> String {
    let mut text = now.strftime("%Y-%m-%dT%H:%M:%S").to_string();
    let fraction = now.subsec_nanosecond();
    if nanos && fraction != 0 {
        let digits = format!("{fraction:09}");
        text += &format!(".{}", digits.trim_end_matches('0'));
    }
    let offset = now.offset().seconds();
    if offset == 0 {
        text.push('Z');
    } else {
        let sign = if offset < 0 { '-' } else { '+' };
        let offset = offset.unsigned_abs();
        text += &format!("{sign}{:02}:{:02}", offset / 3600, offset / 60 % 60);
    }
    text
}

/// Go's `initLocal`: `TZ` unset means `/etc/localtime`; empty or `UTC` means UTC; else the
/// file it names (an absolute path, after a leading `:`), or the zone of that name in the
/// platform's zone directories. Anything that does not load is UTC.
fn local_time_zone() -> jiff::tz::TimeZone {
    let load = |path: &str| {
        let data = std::fs::read(path).ok()?;
        jiff::tz::TimeZone::tzif(path, &data).ok()
    };
    let zone = match std::env::var_os("TZ") {
        None => load("/etc/localtime"),
        Some(tz) => {
            let tz = tz.to_string_lossy().into_owned();
            let tz = tz.strip_prefix(':').unwrap_or(&tz);
            if tz.starts_with('/') {
                load(tz)
            } else if !tz.is_empty() && tz != "UTC" {
                [
                    "/usr/share/zoneinfo/",
                    "/usr/share/lib/zoneinfo/",
                    "/usr/lib/locale/TZ/",
                    "/etc/zoneinfo",
                ]
                .iter()
                .find_map(|source| load(&format!("{source}/{tz}")))
            } else {
                None
            }
        }
    };
    zone.unwrap_or(jiff::tz::TimeZone::UTC)
}

/// How a failure stops the process: upstream's `logger.Fatal` (a fatal line the level may
/// hide), or what upstream panics on (a fatal line always written, docs/parity.md §7).
#[derive(Debug, Clone, PartialEq)]
pub enum Failure {
    Fatal(String),
    Panic(String),
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Failure::Panic(message)
    }
}

impl Failure {
    /// Writes the line and exits 1.
    pub fn exit(self) -> ! {
        match self {
            Failure::Fatal(message) => fatal(&message),
            Failure::Panic(message) => panic_exit(&message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_quoted_like_logrus() {
        assert_eq!(text_value("plain-a.b_c/d@e^f+g"), "plain-a.b_c/d@e^f+g");
        assert_eq!(text_value("two words"), "\"two words\"");
        assert_eq!(text_value("a\"b\n"), "\"a\\\"b\\n\"");
        assert_eq!(text_value("é"), "\"é\"");
    }

    #[test]
    fn levels_parse_like_logrus() {
        assert_eq!(Level::parse("WARNING"), Ok(Level::Warn));
        assert_eq!(Level::parse("Debug"), Ok(Level::Debug));
        assert_eq!(
            Level::parse("bogus"),
            Err("not a valid logrus Level: \"bogus\"".into())
        );
    }

    #[test]
    fn terminal_lines_pad_and_trim() {
        let line = colored_line(Level::Warn, "hello\n");
        assert!(line.starts_with("\x1b[33mWARN\x1b[0m[000"), "{line:?}");
        assert!(line.ends_with(&format!("] {:<44} \n", "hello")), "{line:?}");
    }
}
