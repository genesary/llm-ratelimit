//! google.golang.org/protobuf's `protojson` (v1.36.11) for the types `/json` reads and writes:
//! the tokenizer of `internal/encoding/json` and the rules of `encoding/protojson`, for the
//! request (decoded) and the response (encoded). Errors are not Go's texts: upstream only logs
//! them, and answers every decoding error alike.

use base64::Engine;
use llm_ratelimit_proto::envoy::config::core::v3::HeaderValue;
use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor,
    rate_limit_descriptor::{Entry, RateLimitOverride},
};
use llm_ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse,
    rate_limit_response::{DescriptorStatus, Quota, RateLimit, quota::ExpirationSpecifier},
};
use prost_types::{Duration, Struct, Timestamp, Value, value::Kind as ValueKind};

/// A token kind (`json.Kind`), as bits so that kinds combine.
type Kind = u16;
const EOF: Kind = 1 << 0;
const NULL: Kind = 1 << 1;
const BOOL: Kind = 1 << 2;
const NUMBER: Kind = 1 << 3;
const STRING: Kind = 1 << 4;
const NAME: Kind = 1 << 5;
const OBJECT_OPEN: Kind = 1 << 6;
const OBJECT_CLOSE: Kind = 1 << 7;
const ARRAY_OPEN: Kind = 1 << 8;
const ARRAY_CLOSE: Kind = 1 << 9;
const COMMA: Kind = 1 << 10;
const SCALAR: Kind = NULL | BOOL | NUMBER | STRING;

#[derive(Clone, Debug)]
struct Token {
    kind: Kind,
    /// The number's text (`Number`), or the string's value (`String`, `Name`).
    text: String,
    boolean: bool,
}

type Result<T> = std::result::Result<T, String>;

/// `json.Decoder`: tokens in a valid sequence, one value at the top.
struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
    open: Vec<Kind>,
    /// The kind of the last token read; 0 before the first.
    last: Kind,
    peeked: Option<Result<Token>>,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            position: 0,
            open: Vec::new(),
            last: 0,
            peeked: None,
        }
    }

    fn peek(&mut self) -> Result<Token> {
        if self.peeked.is_none() {
            self.peeked = Some(self.next_token());
        }
        self.peeked.clone().unwrap()
    }

    fn read(&mut self) -> Result<Token> {
        match self.peeked.take() {
            Some(token) => token,
            None => self.next_token(),
        }
    }

    /// `Decoder.Read`: the next token, if it may come here.
    fn next_token(&mut self) -> Result<Token> {
        loop {
            let mut token = self.parse_next()?;
            let unexpected = || Err("unexpected token".to_string());
            match token.kind {
                EOF if !self.open.is_empty() => return Err("unexpected EOF".into()),
                NULL | BOOL | NUMBER | OBJECT_OPEN | ARRAY_OPEN if !self.is_value_next() => {
                    return unexpected();
                }
                STRING if !self.is_value_next() => {
                    // A field name.
                    if self.last & (OBJECT_OPEN | COMMA) == 0 {
                        return unexpected();
                    }
                    if self.input.get(self.position) != Some(&b':') {
                        return Err("missing \":\" after field name".into());
                    }
                    token.kind = NAME;
                    self.consume(1);
                }
                OBJECT_CLOSE
                    if self.last & (NAME | COMMA) != 0
                        || self.open.last() != Some(&OBJECT_OPEN) =>
                {
                    return unexpected();
                }
                ARRAY_CLOSE if self.last == COMMA || self.open.last() != Some(&ARRAY_OPEN) => {
                    return unexpected();
                }
                COMMA
                    if self.open.is_empty()
                        || self.last & (SCALAR | OBJECT_CLOSE | ARRAY_CLOSE) == 0 =>
                {
                    return unexpected();
                }
                _ => {}
            }
            match token.kind {
                OBJECT_OPEN | ARRAY_OPEN => self.open.push(token.kind),
                OBJECT_CLOSE | ARRAY_CLOSE => {
                    self.open.pop();
                }
                _ => {}
            }
            self.last = token.kind;
            if token.kind != COMMA {
                return Ok(token);
            }
        }
    }

    fn is_value_next(&self) -> bool {
        match self.open.last() {
            None => self.last == 0,
            Some(&OBJECT_OPEN) => self.last & NAME != 0,
            Some(_) => self.last & (ARRAY_OPEN | COMMA) != 0,
        }
    }

    /// Skips `n` bytes, then whitespace.
    fn consume(&mut self, n: usize) {
        self.position += n;
        while let Some(b' ' | b'\n' | b'\r' | b'\t') = self.input.get(self.position) {
            self.position += 1;
        }
    }

    /// `Decoder.parseNext`: the next token, whatever comes before it.
    fn parse_next(&mut self) -> Result<Token> {
        self.consume(0);
        let rest = &self.input[self.position..];
        let token = |kind, text: &str| Token {
            kind,
            text: text.to_string(),
            boolean: false,
        };
        let Some(&first) = rest.first() else {
            return Ok(token(EOF, ""));
        };
        let (token, size) = match first {
            b'n' if match_with_delimiter(b"null", rest) => (token(NULL, ""), 4),
            b't' if match_with_delimiter(b"true", rest) => {
                let mut token = token(BOOL, "");
                token.boolean = true;
                (token, 4)
            }
            b'f' if match_with_delimiter(b"false", rest) => (token(BOOL, ""), 5),
            b'-' | b'0'..=b'9' => match parse_number(rest) {
                Some(n) => (token(NUMBER, std::str::from_utf8(&rest[..n]).unwrap()), n),
                None => return Err("invalid value".into()),
            },
            b'"' => {
                let (value, n) = parse_string(rest)?;
                (token(STRING, &value), n)
            }
            b'{' => (token(OBJECT_OPEN, ""), 1),
            b'}' => (token(OBJECT_CLOSE, ""), 1),
            b'[' => (token(ARRAY_OPEN, ""), 1),
            b']' => (token(ARRAY_CLOSE, ""), 1),
            b',' => (token(COMMA, ""), 1),
            _ => return Err("invalid value".into()),
        };
        self.consume(size);
        Ok(token)
    }
}

/// Whether `input` starts with `word` followed by a delimiter or the end.
fn match_with_delimiter(word: &[u8], input: &[u8]) -> bool {
    input.starts_with(word) && !input.get(word.len()).is_some_and(|&c| is_not_delimiter(c))
}

fn is_not_delimiter(c: u8) -> bool {
    matches!(c, b'-' | b'+' | b'.' | b'_') || c.is_ascii_alphanumeric()
}

/// The parts of a JSON number (`numberParts`): `None` if `input` does not start with one.
struct NumberParts<'a> {
    negative: bool,
    integer: &'a [u8],
    fraction: &'a [u8],
    exponent: &'a [u8],
    size: usize,
}

/// `parseNumberParts`: the parts of the number `input` starts with. Go's grammar takes an
/// `e` (and its sign) into the number even when no digit follows.
fn number_parts(input: &[u8]) -> Option<NumberParts<'_>> {
    let digits = |s: &[u8]| s.iter().take_while(|c| c.is_ascii_digit()).count();
    let mut i = 0;
    let negative = input.first() == Some(&b'-');
    if negative {
        i += 1;
    }
    let integer = match input.get(i) {
        Some(b'0') => {
            i += 1;
            &input[i..i]
        }
        Some(b'1'..=b'9') => {
            let start = i;
            i += 1 + digits(&input[i + 1..]);
            &input[start..i]
        }
        _ => return None,
    };
    let mut fraction: &[u8] = &[];
    if input.get(i) == Some(&b'.') && input.get(i + 1).is_some_and(u8::is_ascii_digit) {
        let start = i + 1;
        i = start + digits(&input[start..]);
        fraction = &input[start..i];
    }
    let mut exponent: &[u8] = &[];
    if input.len() >= i + 2 && matches!(input[i], b'e' | b'E') {
        i += 1;
        let start = i;
        if matches!(input[i], b'+' | b'-') {
            i += 1;
            if i == input.len() {
                return None;
            }
        }
        i += digits(&input[i..]);
        exponent = &input[start..i];
    }
    let fraction_end = fraction
        .iter()
        .rposition(|&c| c != b'0')
        .map_or(0, |p| p + 1);
    Some(NumberParts {
        negative,
        integer,
        fraction: &fraction[..fraction_end],
        exponent,
        size: i,
    })
}

/// `parseNumber`: the length of the number `input` starts with, which a delimiter or the
/// end must follow.
fn parse_number(input: &[u8]) -> Option<usize> {
    let size = number_parts(input)?.size;
    match input.get(size) {
        Some(&c) if is_not_delimiter(c) => None,
        _ => Some(size),
    }
}

/// `normalizeToIntString`: the integer a number denotes, without exponent, if it is one.
fn integer_text(number: &str) -> Option<String> {
    let parts = number_parts(number.as_bytes())?;
    if parts.integer.is_empty() && parts.fraction.is_empty() {
        return Some("0".into());
    }
    let exponent: i64 = match parts.exponent {
        [] => 0,
        // A sign alone (`1e+,` is a number token) does not parse.
        text => decimal(std::str::from_utf8(text).ok()?, 32)?,
    };
    let digits = if exponent >= 0 {
        let exponent = exponent as usize;
        if parts.fraction.len() > exponent || parts.integer.len() + exponent > 20 {
            return None;
        }
        let mut digits = [parts.integer, parts.fraction].concat();
        digits.resize(parts.integer.len() + exponent, b'0');
        digits
    } else {
        if !parts.fraction.is_empty() {
            return None;
        }
        let index = parts.integer.len() as i64 + exponent;
        if index < 0 {
            return None;
        }
        let (kept, shifted) = parts.integer.split_at(index as usize);
        if shifted.iter().any(|&c| c != b'0') {
            return None;
        }
        kept.to_vec()
    };
    let digits = String::from_utf8(digits).unwrap();
    Some(match parts.negative {
        true => format!("-{digits}"),
        false => digits,
    })
}

/// `Decoder.parseString`: the string at the start of `input` and its length with the quotes.
fn parse_string(input: &[u8]) -> Result<(String, usize)> {
    let mut out = String::new();
    let mut i = 1;
    let hex4 = |at: usize| -> Option<u32> {
        let digits = input.get(at..at + 4)?;
        digits
            .iter()
            .all(u8::is_ascii_hexdigit)
            .then(|| u32::from_str_radix(std::str::from_utf8(digits).unwrap(), 16).unwrap())
    };
    let invalid = || Err("invalid string".to_string());
    loop {
        let Some(&byte) = input.get(i) else {
            return Err("unexpected EOF".into());
        };
        match byte {
            b'"' => return Ok((out, i + 1)),
            0..0x20 => return invalid(),
            b'\\' => {
                let Some(&escape) = input.get(i + 1) else {
                    return Err("unexpected EOF".into());
                };
                let simple = match escape {
                    b'"' | b'\\' | b'/' => Some(escape as char),
                    b'b' => Some('\u{8}'),
                    b'f' => Some('\u{c}'),
                    b'n' => Some('\n'),
                    b'r' => Some('\r'),
                    b't' => Some('\t'),
                    b'u' => None,
                    _ => return invalid(),
                };
                if let Some(c) = simple {
                    out.push(c);
                    i += 2;
                    continue;
                }
                if input.len() < i + 6 {
                    return Err("unexpected EOF".into());
                }
                let Some(unit) = hex4(i + 2) else {
                    return invalid();
                };
                i += 6;
                let c = match unit {
                    0xD800..=0xDFFF => {
                        if input.len() < i + 6 {
                            return Err("unexpected EOF".into());
                        }
                        // `utf16.DecodeRune`: a high then a low surrogate, else U+FFFD.
                        let low = hex4(i + 2);
                        let paired =
                            matches!(unit, 0xD800..=0xDBFF) && matches!(low, Some(0xDC00..=0xDFFF));
                        if input[i] != b'\\' || input[i + 1] != b'u' || !paired {
                            return invalid();
                        }
                        i += 6;
                        0x10000 + ((unit - 0xD800) << 10) + (low.unwrap() - 0xDC00)
                    }
                    unit => unit,
                };
                out.push(char::from_u32(c).unwrap());
            }
            _ => {
                // One UTF-8 character, which must be valid.
                let width = match byte {
                    0x00..0x80 => 1,
                    0xC0..0xE0 => 2,
                    0xE0..0xF0 => 3,
                    _ => 4,
                };
                let Some(c) = input
                    .get(i..i + width)
                    .and_then(|bytes| std::str::from_utf8(bytes).ok())
                else {
                    return invalid();
                };
                out.push_str(c);
                i += width;
            }
        }
    }
}

/// An unsigned integer field (`unmarshalUint`) of `bits`: a number, or a string holding one.
fn unsigned(token: &Token, bits: u32) -> Option<u64> {
    let number = number_token(token)?;
    let value: u64 = integer_text(&number)?.parse().ok()?;
    (bits == 64 || value >> bits == 0).then_some(value)
}

/// `strconv.ParseInt(text, 10, bits)`: an optional sign, then decimal digits.
fn decimal(text: &str, bits: u32) -> Option<i64> {
    let value: i64 = text.parse().ok()?;
    let limit = 1i64 << (bits - 1);
    (bits == 64 || (-limit..limit).contains(&value)).then_some(value)
}

/// A signed integer (`unmarshalInt`, and enum numbers) of `bits`.
fn signed(token: &Token, bits: u32) -> Option<i64> {
    if token.kind != NUMBER {
        return None;
    }
    decimal(&integer_text(&token.text)?, bits)
}

/// The number of a `Number` token, or of a `String` token holding one: no surrounding
/// space, and only its first token counts (`"1 2"` is 1).
fn number_token(token: &Token) -> Option<String> {
    match token.kind {
        NUMBER => Some(token.text.clone()),
        STRING => {
            let text = &token.text;
            if text.trim_matches(char::is_whitespace).len() != text.len() {
                return None;
            }
            let size = parse_number(text.as_bytes())?;
            Some(text[..size].to_string())
        }
        _ => None,
    }
}

/// A field of the message being decoded: its JSON name, then its proto name.
type Names = (&'static str, &'static str);

/// `unmarshalMessage`'s loop: each field once (under either name), `null` leaving it unset.
fn message(
    decoder: &mut Decoder,
    fields: &[Names],
    mut field: impl FnMut(&mut Decoder, usize) -> Result<()>,
) -> Result<()> {
    if decoder.read()?.kind != OBJECT_OPEN {
        return Err("unexpected token".into());
    }
    let mut seen = vec![false; fields.len()];
    loop {
        let token = decoder.read()?;
        match token.kind {
            OBJECT_CLOSE => return Ok(()),
            NAME => {}
            _ => return Err("unexpected token".into()),
        }
        let name = token.text.as_str();
        let Some(index) = fields
            .iter()
            .position(|(json, _)| *json == name)
            .or_else(|| fields.iter().position(|(_, proto)| *proto == name))
        else {
            return Err(format!("unknown field {name:?}"));
        };
        if std::mem::replace(&mut seen[index], true) {
            return Err(format!("duplicate field {name:?}"));
        }
        if decoder.peek()?.kind == NULL {
            decoder.read()?;
            continue;
        }
        field(decoder, index)?;
    }
}

/// `unmarshalList` of messages: no `null` element.
fn list<T>(
    decoder: &mut Decoder,
    mut element: impl FnMut(&mut Decoder) -> Result<T>,
) -> Result<Vec<T>> {
    if decoder.read()?.kind != ARRAY_OPEN {
        return Err("unexpected token".into());
    }
    let mut elements = Vec::new();
    loop {
        if decoder.peek()?.kind == ARRAY_CLOSE {
            decoder.read()?;
            return Ok(elements);
        }
        elements.push(element(decoder)?);
    }
}

fn string(decoder: &mut Decoder) -> Result<String> {
    let token = decoder.read()?;
    match token.kind {
        STRING => Ok(token.text),
        _ => Err("invalid string".into()),
    }
}

fn uint(decoder: &mut Decoder, bits: u32) -> Result<u64> {
    unsigned(&decoder.read()?, bits).ok_or_else(|| "invalid integer".to_string())
}

fn boolean(decoder: &mut Decoder) -> Result<bool> {
    let token = decoder.read()?;
    match token.kind {
        BOOL => Ok(token.boolean),
        _ => Err("invalid bool".into()),
    }
}

/// An enum (`unmarshalEnum`): a value's name, or any 32-bit number.
fn enumeration(decoder: &mut Decoder, names: &[(&str, i32)]) -> Result<i32> {
    let token = decoder.read()?;
    let number = match token.kind {
        STRING => names
            .iter()
            .find(|(name, _)| *name == token.text)
            .map(|(_, number)| *number),
        _ => signed(&token, 32).map(|n| n as i32),
    };
    number.ok_or_else(|| "invalid enum".into())
}

/// The request's units (`envoy.type.v3.RateLimitUnit`), which stop at `YEAR`.
pub(crate) const UNITS: &[(&str, i32)] = &[
    ("UNKNOWN", 0),
    ("SECOND", 1),
    ("MINUTE", 2),
    ("HOUR", 3),
    ("DAY", 4),
    ("MONTH", 5),
    ("YEAR", 6),
];

fn entry(decoder: &mut Decoder) -> Result<Entry> {
    let mut entry = Entry::default();
    message(decoder, &[("key", "key"), ("value", "value")], |d, f| {
        match f {
            0 => entry.key = string(d)?,
            _ => entry.value = string(d)?,
        }
        Ok(())
    })?;
    Ok(entry)
}

fn limit(decoder: &mut Decoder) -> Result<RateLimitOverride> {
    let mut limit = RateLimitOverride::default();
    let fields = [
        ("requestsPerUnit", "requests_per_unit"),
        ("unit", "unit"),
        ("unitMultiplier", "unit_multiplier"),
    ];
    message(decoder, &fields, |d, f| {
        match f {
            0 => limit.requests_per_unit = uint(d, 32)? as u32,
            1 => limit.unit = enumeration(d, UNITS)?,
            // A wrapper (`google.protobuf.UInt32Value`): the bare value.
            _ => limit.unit_multiplier = Some(uint(d, 32)? as u32),
        }
        Ok(())
    })?;
    Ok(limit)
}

fn descriptor(decoder: &mut Decoder) -> Result<RateLimitDescriptor> {
    let mut descriptor = RateLimitDescriptor::default();
    let fields = [
        ("entries", "entries"),
        ("limit", "limit"),
        ("hitsAddend", "hits_addend"),
        ("isNegativeHits", "is_negative_hits"),
    ];
    message(decoder, &fields, |d, f| {
        match f {
            0 => descriptor.entries = list(d, entry)?,
            1 => descriptor.limit = Some(limit(d)?),
            // A wrapper (`google.protobuf.UInt64Value`): the bare value.
            2 => descriptor.hits_addend = Some(uint(d, 64)?),
            _ => descriptor.is_negative_hits = boolean(d)?,
        }
        Ok(())
    })?;
    Ok(descriptor)
}

/// `protojson.Unmarshal` of a `RateLimitRequest`: the whole input is one object.
pub fn decode_request(input: &[u8]) -> Result<RateLimitRequest> {
    let mut decoder = Decoder::new(input);
    let mut request = RateLimitRequest::default();
    let fields = [
        ("domain", "domain"),
        ("descriptors", "descriptors"),
        ("hitsAddend", "hits_addend"),
    ];
    message(&mut decoder, &fields, |d, f| {
        match f {
            0 => request.domain = string(d)?,
            1 => request.descriptors = list(d, descriptor)?,
            _ => request.hits_addend = uint(d, 32)? as u32,
        }
        Ok(())
    })?;
    // Any token but the end is out of sequence after the object: reading it fails.
    decoder.read()?;
    Ok(request)
}

/// `json.Encoder` without indentation: values separated by commas, with no space (Go adds
/// one after every comma or none, depending on the build).
struct Encoder {
    out: String,
    /// Whether the next value follows another in the same object or array.
    after_value: bool,
}

impl Encoder {
    fn separate(&mut self) {
        if self.after_value {
            self.out.push(',');
        }
    }

    fn open(&mut self, bracket: char) {
        self.separate();
        self.out.push(bracket);
        self.after_value = false;
    }

    fn close(&mut self, bracket: char) {
        self.out.push(bracket);
        self.after_value = true;
    }

    fn name(&mut self, name: &str) {
        self.separate();
        append_string(&mut self.out, name);
        self.out.push(':');
        self.after_value = false;
    }

    /// A scalar, already in JSON.
    fn raw(&mut self, value: &str) {
        self.separate();
        self.out.push_str(value);
        self.after_value = true;
    }

    fn string(&mut self, value: &str) {
        self.separate();
        append_string(&mut self.out, value);
        self.after_value = true;
    }
}

/// `appendString`: only `"`, `\` and control characters are escaped.
fn append_string(out: &mut String, value: &str) {
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// `appendFloat` for a double: the shortest digits, in `e` form below 1e-6 or from 1e21.
/// The number is finite: `google.protobuf.Value` refuses others.
fn format_double(n: f64) -> String {
    let abs = n.abs();
    if abs != 0.0 && !(1e-6..1e21).contains(&abs) {
        // Go: at least two exponent digits and a sign, then `e-0X` shortened to `e-X`.
        let text = format!("{n:e}");
        let (mantissa, exponent) = text.split_once('e').unwrap();
        let exponent: i32 = exponent.parse().unwrap();
        let sign = if exponent < 0 { '-' } else { '+' };
        let digits = exponent.unsigned_abs();
        return match (sign, digits) {
            ('-', 0..10) => format!("{mantissa}e-{digits}"),
            _ => format!("{mantissa}e{sign}{digits:02}"),
        };
    }
    format!("{n}")
}

/// An enum value: its name, or its number when it has none.
fn enum_value(e: &mut Encoder, number: i32, names: &[(&str, i32)]) {
    match names.iter().find(|(_, n)| *n == number) {
        Some((name, _)) => e.string(name),
        None => e.raw(&number.to_string()),
    }
}

pub(crate) const CODES: &[(&str, i32)] = &[("UNKNOWN", 0), ("OK", 1), ("OVER_LIMIT", 2)];
/// The response's units (`RateLimitResponse.RateLimit.Unit`), which also have `WEEK`.
pub(crate) const RESPONSE_UNITS: &[(&str, i32)] = &[
    ("UNKNOWN", 0),
    ("SECOND", 1),
    ("MINUTE", 2),
    ("HOUR", 3),
    ("DAY", 4),
    ("WEEK", 7),
    ("MONTH", 5),
    ("YEAR", 6),
];

/// Bytes in standard base64, padded.
fn bytes_value(e: &mut Encoder, bytes: &[u8]) {
    e.string(&base64::engine::general_purpose::STANDARD.encode(bytes));
}

fn struct_value(e: &mut Encoder, value: &Struct) -> Result<()> {
    e.open('{');
    for (key, value) in &value.fields {
        e.name(key);
        known_value(e, value)?;
    }
    e.close('}');
    Ok(())
}

/// `google.protobuf.Value`: its kind must be set, and a number finite.
fn known_value(e: &mut Encoder, value: &Value) -> Result<()> {
    match &value.kind {
        None => return Err("google.protobuf.Value: none of the oneof fields is set".into()),
        Some(ValueKind::NullValue(_)) => e.raw("null"),
        Some(ValueKind::NumberValue(n)) if !n.is_finite() => {
            return Err(format!(
                "google.protobuf.Value.number_value: invalid {n} value"
            ));
        }
        Some(ValueKind::NumberValue(n)) => e.raw(&format_double(*n)),
        Some(ValueKind::StringValue(s)) => e.string(s),
        Some(ValueKind::BoolValue(b)) => e.raw(if *b { "true" } else { "false" }),
        Some(ValueKind::StructValue(s)) => struct_value(e, s)?,
        Some(ValueKind::ListValue(list)) => {
            e.open('[');
            for value in &list.values {
                known_value(e, value)?;
            }
            e.close(']');
        }
    }
    Ok(())
}

/// `google.protobuf.Duration`: seconds with 0, 3, 6 or 9 fractional digits.
fn duration(e: &mut Encoder, d: &Duration) -> Result<()> {
    let (mut seconds, mut nanos) = (d.seconds, d.nanos as i64);
    if !(-315_576_000_000..=315_576_000_000).contains(&seconds) {
        return Err("google.protobuf.Duration: seconds out of range".into());
    }
    if !(-999_999_999..=999_999_999).contains(&nanos) {
        return Err("google.protobuf.Duration: nanos out of range".into());
    }
    if (seconds > 0 && nanos < 0) || (seconds < 0 && nanos > 0) {
        return Err("google.protobuf.Duration: signs of seconds and nanos do not match".into());
    }
    let sign = if seconds < 0 || nanos < 0 { "-" } else { "" };
    (seconds, nanos) = (seconds.abs(), nanos.abs());
    e.string(&format!("{sign}{seconds}{}s", fraction(nanos)));
    Ok(())
}

/// `.` and 3, 6 or 9 digits of `nanos`, or nothing for none.
fn fraction(nanos: i64) -> String {
    let mut digits = format!(".{nanos:09}");
    for _ in 0..2 {
        if digits.ends_with("000") {
            digits.truncate(digits.len() - 3);
        }
    }
    if digits == ".000" {
        digits.clear();
    }
    digits
}

/// `google.protobuf.Timestamp`: RFC 3339 in UTC.
fn timestamp(e: &mut Encoder, t: &Timestamp) -> Result<()> {
    if !(-62_135_596_800..=253_402_300_799).contains(&t.seconds) {
        return Err("google.protobuf.Timestamp: seconds out of range".into());
    }
    if !(0..=999_999_999).contains(&t.nanos) {
        return Err("google.protobuf.Timestamp: nanos out of range".into());
    }
    let (days, second) = (t.seconds.div_euclid(86_400), t.seconds.rem_euclid(86_400));
    // Days to a civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    e.string(&format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}{}Z",
        second / 3600,
        second / 60 % 60,
        second % 60,
        fraction(t.nanos as i64)
    ));
    Ok(())
}

fn quota(e: &mut Encoder, q: &Quota) -> Result<()> {
    e.open('{');
    if q.requests != 0 {
        e.name("requests");
        e.raw(&q.requests.to_string());
    }
    if let Some(ExpirationSpecifier::ValidUntil(t)) = &q.expiration_specifier {
        e.name("validUntil");
        timestamp(e, t)?;
    }
    if !q.id.is_empty() {
        e.name("id");
        e.string(&q.id);
    }
    e.close('}');
    Ok(())
}

fn rate_limit(e: &mut Encoder, limit: &RateLimit) {
    e.open('{');
    if !limit.name.is_empty() {
        e.name("name");
        e.string(&limit.name);
    }
    if limit.requests_per_unit != 0 {
        e.name("requestsPerUnit");
        e.raw(&limit.requests_per_unit.to_string());
    }
    if limit.unit != 0 {
        e.name("unit");
        enum_value(e, limit.unit, RESPONSE_UNITS);
    }
    // Our extension, which Go's type does not have.
    if let Some(multiplier) = limit.unit_multiplier {
        e.name("unitMultiplier");
        e.raw(&multiplier.to_string());
    }
    e.close('}');
}

fn status(e: &mut Encoder, status: &DescriptorStatus) -> Result<()> {
    e.open('{');
    if status.code != 0 {
        e.name("code");
        enum_value(e, status.code, CODES);
    }
    if let Some(limit) = &status.current_limit {
        e.name("currentLimit");
        rate_limit(e, limit);
    }
    if status.limit_remaining != 0 {
        e.name("limitRemaining");
        e.raw(&status.limit_remaining.to_string());
    }
    if let Some(d) = &status.duration_until_reset {
        e.name("durationUntilReset");
        duration(e, d)?;
    }
    if let Some(q) = &status.quota {
        e.name("quota");
        quota(e, q)?;
    }
    e.close('}');
    Ok(())
}

fn headers(e: &mut Encoder, name: &str, headers: &[HeaderValue]) {
    if headers.is_empty() {
        return;
    }
    e.name(name);
    e.open('[');
    for header in headers {
        e.open('{');
        if !header.key.is_empty() {
            e.name("key");
            e.string(&header.key);
        }
        if !header.value.is_empty() {
            e.name("value");
            e.string(&header.value);
        }
        if !header.raw_value.is_empty() {
            e.name("rawValue");
            bytes_value(e, &header.raw_value);
        }
        e.close('}');
    }
    e.close(']');
}

/// `protojson.Marshal` of a `RateLimitResponse`: fields in declaration order, defaults left
/// out; an error for what Go refuses to write (a non-finite number in the metadata).
pub fn encode_response(response: &RateLimitResponse) -> Result<String> {
    let mut e = Encoder {
        out: String::new(),
        after_value: false,
    };
    e.open('{');
    if response.overall_code != 0 {
        e.name("overallCode");
        enum_value(&mut e, response.overall_code, CODES);
    }
    if !response.statuses.is_empty() {
        e.name("statuses");
        e.open('[');
        for s in &response.statuses {
            status(&mut e, s)?;
        }
        e.close(']');
    }
    headers(
        &mut e,
        "responseHeadersToAdd",
        &response.response_headers_to_add,
    );
    headers(
        &mut e,
        "requestHeadersToAdd",
        &response.request_headers_to_add,
    );
    if !response.raw_body.is_empty() {
        e.name("rawBody");
        bytes_value(&mut e, &response.raw_body);
    }
    if let Some(metadata) = &response.dynamic_metadata {
        e.name("dynamicMetadata");
        struct_value(&mut e, metadata)?;
    }
    if let Some(q) = &response.quota {
        e.name("quota");
        quota(&mut e, q)?;
    }
    e.close('}');
    Ok(e.out)
}
