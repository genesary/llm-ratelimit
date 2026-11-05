//! What Go prints for a message with `%v` or `%+v`: its `String()`, protobuf-go's single-line
//! text format (`prototext` with `AllowPartial` and `EmitUnknown`). Fields in declaration
//! order, proto3 defaults left out; a single space between fields, where Go adds a second one
//! depending on the build (`detrand`, docs/parity.md §7).

use crate::protojson::{CODES, RESPONSE_UNITS, UNITS};
use llm_ratelimit_proto::envoy::config::core::v3::HeaderValue;
use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor, rate_limit_descriptor::RateLimitOverride,
};
use llm_ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse,
    rate_limit_response::{DescriptorStatus, Quota, RateLimit, quota::ExpirationSpecifier},
};
use prost_types::{Struct, Value, value::Kind as ValueKind};

/// `text.Encoder` in single-line mode.
struct Encoder {
    out: String,
    /// The last token was a scalar or a closing `}`: a space comes before the next name.
    after_value: bool,
}

impl Encoder {
    fn new() -> Self {
        Self {
            out: String::new(),
            after_value: false,
        }
    }

    fn name(&mut self, name: &str) {
        if self.after_value {
            self.out.push(' ');
        }
        self.out.push_str(name);
        self.out.push(':');
        self.after_value = false;
    }

    fn scalar(&mut self, name: &str, value: &str) {
        self.name(name);
        self.out.push_str(value);
        self.after_value = true;
    }

    fn string(&mut self, name: &str, value: &[u8]) {
        self.name(name);
        append_string(&mut self.out, value);
        self.after_value = true;
    }

    fn message(&mut self, name: &str, body: impl FnOnce(&mut Self)) {
        self.name(name);
        self.out.push('{');
        self.after_value = false;
        body(self);
        self.out.push('}');
        self.after_value = true;
    }

    /// A proto3 string or bytes field: left out when empty.
    fn text(&mut self, name: &str, value: &[u8]) {
        if !value.is_empty() {
            self.string(name, value);
        }
    }

    /// A proto3 integer field: left out when 0.
    fn number(&mut self, name: &str, value: u64) {
        if value != 0 {
            self.scalar(name, &value.to_string());
        }
    }

    /// A proto3 signed integer field: left out when 0.
    fn signed(&mut self, name: &str, value: i64) {
        if value != 0 {
            self.scalar(name, &value.to_string());
        }
    }

    /// A proto3 enum field: left out when 0; its name, or its number when it has none.
    fn enumeration(&mut self, name: &str, value: i32, names: &[(&str, i32)]) {
        if value != 0 {
            let text = names
                .iter()
                .find(|(_, n)| *n == value)
                .map_or(value.to_string(), |(name, _)| name.to_string());
            self.scalar(name, &text);
        }
    }

    /// A wrapper (`google.protobuf.UInt32Value`, `UInt64Value`) when set.
    fn wrapper(&mut self, name: &str, value: Option<u64>) {
        if let Some(value) = value {
            self.message(name, |e| e.number("value", value));
        }
    }
}

/// `appendString`: `"`, `\`, control characters and invalid UTF-8 escaped, as are the C1
/// control characters (`\u0080` to `\u009f`); other characters as they are.
fn append_string(out: &mut String, value: &[u8]) {
    out.push('"');
    for chunk in value.utf8_chunks() {
        for c in chunk.valid().chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if c < ' ' || c == '\u{7f}' => out.push_str(&format!("\\x{:02x}", c as u32)),
                c if ('\u{80}'..='\u{9f}').contains(&c) => {
                    out.push_str(&format!("\\u{:04x}", c as u32))
                }
                c => out.push(c),
            }
        }
        for byte in chunk.invalid() {
            out.push_str(&format!("\\x{byte:02x}"));
        }
    }
    out.push('"');
}

/// `appendFloat` for a double: `strconv.FormatFloat(n, 'g', -1, 64)`, with `nan` and `inf`.
fn format_double(n: f64) -> String {
    match n {
        n if n.is_nan() => "nan".into(),
        n if n.is_infinite() && n > 0.0 => "inf".into(),
        n if n.is_infinite() => "-inf".into(),
        n => crate::go::format_float(n),
    }
}

/// `%+v` of a request.
pub fn request(request: &RateLimitRequest) -> String {
    let mut e = Encoder::new();
    e.text("domain", request.domain.as_bytes());
    for descriptor in &request.descriptors {
        e.message("descriptors", |e| self::descriptor(e, descriptor));
    }
    e.number("hits_addend", request.hits_addend.into());
    e.out
}

fn descriptor(e: &mut Encoder, descriptor: &RateLimitDescriptor) {
    for entry in &descriptor.entries {
        e.message("entries", |e| {
            e.text("key", entry.key.as_bytes());
            e.text("value", entry.value.as_bytes());
        });
    }
    if let Some(limit) = &descriptor.limit {
        e.message("limit", |e| limit_override(e, limit));
    }
    e.wrapper("hits_addend", descriptor.hits_addend);
    if descriptor.is_negative_hits {
        e.scalar("is_negative_hits", "true");
    }
}

/// A client-sent limit; `unit_multiplier`, unknown to upstream, is an extension
/// (docs/parity.md §7).
fn limit_override(e: &mut Encoder, limit: &RateLimitOverride) {
    e.number("requests_per_unit", limit.requests_per_unit.into());
    e.enumeration("unit", limit.unit, UNITS);
    e.wrapper("unit_multiplier", limit.unit_multiplier.map(u64::from));
}

/// `%v` of a client-sent limit, as the metadata writes it.
pub fn limit(limit: &RateLimitOverride) -> String {
    let mut e = Encoder::new();
    limit_override(&mut e, limit);
    e.out
}

/// `%+v` of a response.
pub fn response(response: &RateLimitResponse) -> String {
    let mut e = Encoder::new();
    e.enumeration("overall_code", response.overall_code, CODES);
    for s in &response.statuses {
        e.message("statuses", |e| status(e, s));
    }
    for header in &response.response_headers_to_add {
        e.message("response_headers_to_add", |e| header_value(e, header));
    }
    for header in &response.request_headers_to_add {
        e.message("request_headers_to_add", |e| header_value(e, header));
    }
    e.text("raw_body", &response.raw_body);
    if let Some(metadata) = &response.dynamic_metadata {
        e.message("dynamic_metadata", |e| struct_value(e, metadata));
    }
    if let Some(q) = &response.quota {
        e.message("quota", |e| quota(e, q));
    }
    e.out
}

/// `%+v` of a slice of statuses: each one's text, space-separated, in brackets.
pub fn statuses(statuses: &[DescriptorStatus]) -> String {
    let texts: Vec<String> = statuses
        .iter()
        .map(|s| {
            let mut e = Encoder::new();
            status(&mut e, s);
            e.out
        })
        .collect();
    format!("[{}]", texts.join(" "))
}

fn status(e: &mut Encoder, status: &DescriptorStatus) {
    e.enumeration("code", status.code, CODES);
    if let Some(limit) = &status.current_limit {
        e.message("current_limit", |e| rate_limit(e, limit));
    }
    e.number("limit_remaining", status.limit_remaining.into());
    if let Some(d) = &status.duration_until_reset {
        e.message("duration_until_reset", |e| seconds(e, d.seconds, d.nanos));
    }
    if let Some(q) = &status.quota {
        e.message("quota", |e| quota(e, q));
    }
}

fn rate_limit(e: &mut Encoder, limit: &RateLimit) {
    e.text("name", limit.name.as_bytes());
    e.number("requests_per_unit", limit.requests_per_unit.into());
    e.enumeration("unit", limit.unit, RESPONSE_UNITS);
    e.wrapper("unit_multiplier", limit.unit_multiplier.map(u64::from));
}

fn quota(e: &mut Encoder, quota: &Quota) {
    e.number("requests", quota.requests.into());
    // A oneof member is printed when set, even at its default.
    if let Some(ExpirationSpecifier::ValidUntil(t)) = &quota.expiration_specifier {
        e.message("valid_until", |e| seconds(e, t.seconds, t.nanos));
    }
    e.text("id", quota.id.as_bytes());
}

/// `google.protobuf.Duration` or `Timestamp`.
fn seconds(e: &mut Encoder, seconds: i64, nanos: i32) {
    e.signed("seconds", seconds);
    e.signed("nanos", nanos.into());
}

fn header_value(e: &mut Encoder, header: &HeaderValue) {
    e.text("key", header.key.as_bytes());
    e.text("value", header.value.as_bytes());
    e.text("raw_value", &header.raw_value);
}

/// `google.protobuf.Struct`: its map's entries by key.
fn struct_value(e: &mut Encoder, value: &Struct) {
    for (key, value) in &value.fields {
        e.message("fields", |e| {
            e.string("key", key.as_bytes());
            e.message("value", |e| self::value(e, value));
        });
    }
}

/// `google.protobuf.Value`: its oneof member, printed even at its default.
fn value(e: &mut Encoder, value: &Value) {
    match &value.kind {
        None => {}
        Some(ValueKind::NullValue(n)) => {
            let text = match n {
                0 => "NULL_VALUE".to_string(),
                n => n.to_string(),
            };
            e.scalar("null_value", &text);
        }
        Some(ValueKind::NumberValue(n)) => e.scalar("number_value", &format_double(*n)),
        Some(ValueKind::StringValue(s)) => e.string("string_value", s.as_bytes()),
        Some(ValueKind::BoolValue(b)) => e.scalar("bool_value", if *b { "true" } else { "false" }),
        Some(ValueKind::StructValue(s)) => e.message("struct_value", |e| struct_value(e, s)),
        Some(ValueKind::ListValue(list)) => e.message("list_value", |e| {
            for v in &list.values {
                e.message("values", |e| self::value(e, v));
            }
        }),
    }
}
