//! envoyproxy/ratelimit's DogStatsD sink (`src/godogstats`) over datadog-go v5.5.0's client, as
//! upstream configures it (UDP, no client-side aggregation): `name:value|c` lines with
//! `|#tags` and the container ID (`|c:`), packed into datagrams of at most 1432 bytes and sent
//! every 100 milliseconds. Unlike datadog-go, no client telemetry is sent (docs/parity.md
//! section 7).

use super::origin;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// datadog-go's `OptimalUDPPayloadSize`.
const MAX_DATAGRAM: usize = 1432;
/// datadog-go's `DefaultBufferFlushInterval`.
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);
/// datadog-go's `DefaultUDPBufferPoolSize`: datagrams waiting to be sent.
const QUEUE: usize = 2048;

/// What configures the DogStatsD sink besides its address.
#[derive(Debug, Clone)]
pub struct DogStatsdSettings {
    /// `DD_ENTITY_ID`: sent as the `dd.internal.entity_id` tag, and turns origin detection off.
    pub entity_id: String,
    /// `DD_ENV`, `DD_SERVICE`, `DD_VERSION`: sent as `env:`, `service:`, `version:` tags.
    pub env: String,
    pub service: String,
    pub version: String,
    /// `DD_ORIGIN_DETECTION_ENABLED` (on unless set to a false value): find the container ID.
    pub origin_detection: bool,
    /// `USE_DOG_STATSD_MOGRIFIERS`, in order.
    pub mogrifiers: Vec<Mogrifier>,
}

impl Default for DogStatsdSettings {
    fn default() -> Self {
        Self {
            entity_id: String::new(),
            env: String::new(),
            service: String::new(),
            version: String::new(),
            origin_detection: true,
            mogrifiers: Vec::new(),
        }
    }
}

/// A rewrite of matching names into a name and tags (`DOG_STATSD_MOGRIFIER_<NAME>_*`).
#[derive(Debug, Clone)]
pub struct Mogrifier {
    pattern: regex::Regex,
    name: String,
    /// Sorted by key; upstream's order is Go's random map order.
    tags: BTreeMap<String, String>,
}

impl Mogrifier {
    /// The mogrifier `key`, with upstream's validation and messages. `name` and the tag values
    /// may refer to the pattern's groups as `$0`, `$1`, …
    pub fn new(
        key: &str,
        pattern: &str,
        name: &str,
        tags: BTreeMap<String, String>,
    ) -> Result<Self, String> {
        if pattern.is_empty() {
            return Err(format!("no PATTERN specified for mogrifier {key}"));
        }
        let compiled = go_regex(pattern)
            .map_err(|e| format!("failed to compile pattern for {key}: {pattern}: {e}"))?;
        if name.is_empty() {
            return Err(format!("no NAME specified for mogrifier {key}"));
        }
        for (tag, value) in &tags {
            if tag.is_empty() {
                return Err(format!(
                    "no key specified for tag {tag} for mogrifier {key}"
                ));
            }
            if value.is_empty() {
                return Err(format!(
                    "no value specified for tag {tag} for mogrifier {key}"
                ));
            }
        }
        Ok(Self {
            pattern: compiled,
            name: name.to_string(),
            tags,
        })
    }

    /// The new name and tags of `name`, if the pattern matches it.
    fn apply(&self, name: &str) -> Option<(String, Vec<String>)> {
        let captures = self.pattern.captures(name)?;
        let groups: Vec<&str> = captures
            .iter()
            .map(|group| group.map_or("", |m| m.as_str()))
            .collect();
        let tags = self
            .tags
            .iter()
            .map(|(tag, value)| format!("{tag}:{}", substitute(value, &groups)))
            .collect();
        Some((substitute(&self.name, &groups), tags))
    }
}

/// Upstream's `makePatternHandler`: each `$<digits>` becomes that group, or stays as it is
/// when there is no such group.
fn substitute(template: &str, groups: &[&str]) -> String {
    let mut out = String::new();
    let mut rest = template;
    while let Some(at) = rest.find('$') {
        out += &rest[..at];
        let digits = rest[at + 1..]
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count();
        let reference = &rest[at..at + 1 + digits];
        match reference[1..]
            .parse::<usize>()
            .ok()
            .and_then(|i| groups.get(i))
        {
            Some(group) if digits > 0 => out += group,
            _ => out += reference,
        }
        rest = &rest[at + 1 + digits..];
    }
    out + rest
}

/// Go's `\\s`: ASCII white space, without the vertical tab.
pub(crate) const ASCII_SPACE: &str = "\\t\\n\\x0C\\r ";

/// A Go `regexp` pattern compiled for Rust's engine, with Go's ASCII `\d`, `\s`, `\w` and `\b`.
pub(crate) fn go_regex(pattern: &str) -> Result<regex::Regex, regex::Error> {
    regex::Regex::new(&ascii_perl_classes(pattern))
}

/// Go's RE2 classes `\d`, `\s`, `\w` and `\b` are ASCII-only; Rust's are Unicode. Rewrites them
/// (and their negations) into ASCII forms, outside and inside brackets.
fn ascii_perl_classes(pattern: &str) -> String {
    let mut out = String::new();
    let mut in_class = false;
    let mut class_start = false;
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        let at_class_start = std::mem::take(&mut class_start);
        match c {
            '\\' => {
                let Some(next) = chars.next() else {
                    out.push(c);
                    break;
                };
                let set = match next.to_ascii_lowercase() {
                    'd' => Some("0-9"),
                    'w' => Some("0-9A-Za-z_"),
                    's' => Some(ASCII_SPACE),
                    _ => None,
                };
                match (next, set) {
                    ('d' | 'w' | 's', Some(set)) if in_class => out += set,
                    ('d' | 'w' | 's', Some(set)) => out += &format!("[{set}]"),
                    (_, Some(set)) => out += &format!("[^{set}]"),
                    ('b' | 'B', _) if !in_class => out += &format!("(?-u:\\{next})"),
                    _ => {
                        out.push('\\');
                        out.push(next);
                    }
                }
            }
            '[' if !in_class => {
                in_class = true;
                class_start = true;
                out.push(c);
                if chars.peek() == Some(&'^') {
                    out.push(chars.next().unwrap());
                }
            }
            ']' if in_class && !at_class_start => {
                in_class = false;
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

/// The address datadog-go dials for `host:port` (Go's `net.ResolveUDPAddr`): the host and port
/// split as `net.SplitHostPort` does, and a name resolved to its first IPv4 address unless the
/// host was bracketed.
fn resolve_udp(address: &str) -> Result<std::net::SocketAddr, String> {
    let (host, port, bracketed) = match address.strip_prefix('[') {
        Some(rest) => {
            let (host, port) = rest
                .split_once("]:")
                .ok_or_else(|| format!("address {address}: missing port in address"))?;
            (host, port, true)
        }
        None => {
            let (host, port) = address
                .rsplit_once(':')
                .ok_or_else(|| format!("address {address}: missing port in address"))?;
            if host.contains(':') {
                return Err(format!("address {address}: too many colons in address"));
            }
            (host, port, false)
        }
    };
    let port: u16 = port
        .parse()
        .map_err(|_| format!("address {address}: invalid port"))?;
    let addresses: Vec<std::net::SocketAddr> =
        std::net::ToSocketAddrs::to_socket_addrs(&(host, port))
            .map_err(|e| e.to_string())?
            .collect();
    let preferred = addresses.iter().find(|a| a.is_ipv6() == bracketed);
    preferred
        .or(addresses.first())
        .copied()
        .ok_or_else(|| format!("no address for {host}"))
}

pub struct DogStatsdSink {
    pending: Arc<Mutex<Vec<u8>>>,
    queue: mpsc::Sender<Vec<u8>>,
    /// `dd.internal.entity_id`, `env`, `service`, `version`, when set.
    global_tags: Vec<String>,
    container_id: Option<String>,
    mogrifiers: Vec<Mogrifier>,
}

impl DogStatsdSink {
    /// Sends to `host:port` over UDP; fails like datadog-go's `New` when the address does not
    /// resolve. The container ID is found once, here.
    pub fn new(host: &str, port: u16, settings: DogStatsdSettings) -> Result<Self, String> {
        let container_id = (settings.entity_id.is_empty() && settings.origin_detection)
            .then(origin::container_id)
            .flatten();
        Self::with_container_id(host, port, settings, container_id)
    }

    /// The same, with `container_id` instead of origin detection.
    pub fn with_container_id(
        host: &str,
        port: u16,
        settings: DogStatsdSettings,
        container_id: Option<String>,
    ) -> Result<Self, String> {
        let address = resolve_udp(&format!("{host}:{port}"))?;
        let local = if address.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = std::net::UdpSocket::bind(local).map_err(|e| e.to_string())?;
        socket.connect(address).map_err(|e| e.to_string())?;
        socket.set_nonblocking(true).map_err(|e| e.to_string())?;
        let socket = UdpSocket::from_std(socket).map_err(|e| e.to_string())?;

        let pending = Arc::new(Mutex::new(Vec::new()));
        let (queue, mut queued) = mpsc::channel::<Vec<u8>>(QUEUE);
        tokio::spawn(async move {
            // Send errors are dropped, as datadog-go's default error handler does.
            while let Some(datagram) = queued.recv().await {
                let _ = socket.send(&datagram).await;
            }
        });
        let ticking = Arc::downgrade(&pending);
        let ticker_queue = queue.clone();
        tokio::spawn(async move {
            let start = tokio::time::Instant::now() + FLUSH_INTERVAL;
            let mut ticks = tokio::time::interval_at(start, FLUSH_INTERVAL);
            loop {
                ticks.tick().await;
                let Some(pending) = ticking.upgrade() else {
                    return;
                };
                let datagram = std::mem::take(&mut *pending.lock().unwrap());
                if !datagram.is_empty() {
                    // A full queue drops the datagram, as datadog-go does.
                    let _ = ticker_queue.try_send(datagram);
                }
            }
        });

        let global_tags = [
            ("dd.internal.entity_id", &settings.entity_id),
            ("env", &settings.env),
            ("service", &settings.service),
            ("version", &settings.version),
        ]
        .into_iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(tag, value)| format!("{tag}:{value}"))
        .collect();
        Ok(Self {
            pending,
            queue,
            global_tags,
            container_id,
            mogrifiers: settings.mogrifiers,
        })
    }

    pub fn flush_counter(&self, name: &str, value: u64) {
        // As upstream: `int64(value)`.
        self.write_metric(name, &format!("{}|c", value as i64));
    }

    /// A line for the metric `name` (mogrified) of `value` (with its type), its tags and the
    /// container's ID.
    fn write_metric(&self, name: &str, value: &str) {
        let (name, tags) = self.mogrify(name);
        let mut line = format!("{name}:{value}");
        let tags: Vec<String> = self
            .global_tags
            .iter()
            .cloned()
            .chain(tags)
            .map(|tag| tag.replace('\n', ""))
            .collect();
        if !tags.is_empty() {
            line += "|#";
            line += &tags.join(",");
        }
        if let Some(id) = &self.container_id {
            line += "|c:";
            line += id;
        }
        line.push('\n');
        self.write_line(line.as_bytes());
    }

    /// `Gauge(name, float64(value))`: the value as a float, in its shortest form.
    pub fn flush_gauge(&self, name: &str, value: u64) {
        self.write_metric(name, &format!("{}|g", value as f64));
    }

    /// `Timing(name, time.Duration(milliseconds) * time.Millisecond)`: whole milliseconds, with
    /// six decimals.
    pub fn flush_timer(&self, name: &str, milliseconds: f64) {
        let milliseconds = milliseconds.trunc();
        self.write_metric(
            name,
            &format!("{}|ms", crate::go::format_fixed(milliseconds, 6)),
        );
    }

    /// Adds `line` to the datagram being filled, sending that one first when the line does not
    /// fit. A line longer than a datagram is dropped.
    fn write_line(&self, line: &[u8]) {
        let mut pending = self.pending.lock().unwrap();
        if pending.len() + line.len() > MAX_DATAGRAM {
            // Picked up by the next tick otherwise; queued now, like datadog-go's worker.
            let datagram = std::mem::take(&mut *pending);
            if !datagram.is_empty() {
                let _ = self.queue.try_send(datagram);
            }
        }
        if line.len() <= MAX_DATAGRAM {
            pending.extend_from_slice(line);
        }
    }

    /// `separateTags` then the first matching mogrifier: the name without its `EXTRA_TAGS`,
    /// and those tags as `key:value` followed by the mogrifier's.
    fn mogrify(&self, name: &str) -> (String, Vec<String>) {
        let (name, mut tags) = match name.split_once(".__") {
            None => (name.to_string(), Vec::new()),
            Some((short, tags)) => (
                short.to_string(),
                tags.split(".__")
                    .filter_map(|pair| {
                        let tag = pair.split_once('=');
                        if tag.is_none() {
                            crate::debug!(
                                "godogstats sink found malformed extra tag: {pair}, string: {name}"
                            );
                        }
                        tag
                    })
                    .map(|(key, value)| format!("{key}:{value}"))
                    .collect(),
            ),
        };
        match self.mogrifiers.iter().find_map(|m| m.apply(&name)) {
            Some((renamed, extra)) => {
                tags.extend(extra);
                (renamed, tags)
            }
            None => (name, tags),
        }
    }
}
