//! The propagators upstream sets: W3C TraceContext, then B3 (single or multiple headers),
//! which wins when both are valid. Only extraction is done: the service makes no traced
//! call. Baggage, which nothing reads, is not extracted.

use super::SpanContext;

/// The remote parent a request's headers carry, if any.
pub fn extract(headers: &http::HeaderMap) -> Option<SpanContext> {
    let get = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
    };
    let b3 = b3(
        get("b3"),
        get("x-b3-traceid"),
        get("x-b3-spanid"),
        get("x-b3-parentspanid"),
        get("x-b3-sampled"),
        get("x-b3-flags"),
    );
    b3.or_else(|| trace_context(get("traceparent"), get("tracestate")))
}

/// `TraceContext.Extract`: `version-traceid-spanid-flags` in lower-case hex; version 0 has
/// nothing more and only the sampled and random flags, which are kept.
fn trace_context(traceparent: &str, tracestate: &str) -> Option<SpanContext> {
    if traceparent.is_empty() {
        return None;
    }
    let mut rest = traceparent;
    let mut part = |n: usize| {
        let (part, left) = rest.split_once('-').unwrap_or((rest, ""));
        rest = left;
        decode_hex(part, n)
    };
    let version = part(2)?[0];
    if version == 0xff {
        return None;
    }
    let trace_id: [u8; 16] = part(32)?.try_into().ok()?;
    let span_id: [u8; 8] = part(16)?.try_into().ok()?;
    let flags = part(2)?[0];
    if version == 0 && (!rest.is_empty() || flags > 3) {
        return None;
    }
    if trace_id == [0; 16] || span_id == [0; 8] {
        return None;
    }
    Some(SpanContext {
        trace_id,
        span_id,
        flags: flags & 3,
        trace_state: parse_trace_state(tracestate).unwrap_or_default(),
        remote: true,
    })
}

/// `n` lower-case hex digits.
fn decode_hex(part: &str, n: usize) -> Option<Vec<u8>> {
    if part.len() != n
        || !part
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    (0..n)
        .step_by(2)
        .map(|i| u8::from_str_radix(&part[i..i + 2], 16).ok())
        .collect()
}

/// `TraceIDFromHex`: 32 lower-case hex digits, not all zero.
fn trace_id(hex: &str) -> Option<[u8; 16]> {
    let id: [u8; 16] = decode_hex(hex, 32)?.try_into().ok()?;
    (id != [0; 16]).then_some(id)
}

/// `SpanIDFromHex`: 16 lower-case hex digits, not all zero.
fn span_id(hex: &str) -> Option<[u8; 8]> {
    let id: [u8; 8] = decode_hex(hex, 16)?.try_into().ok()?;
    (id != [0; 8]).then_some(id)
}

/// `ParseTraceState` then `String()`: comma-separated `key=value` members (empty ones
/// skipped, the key's leading and the value's trailing blanks trimmed), at most 32, no key
/// twice; an invalid list is no trace state.
fn parse_trace_state(value: &str) -> Option<String> {
    let mut members: Vec<(&str, &str)> = Vec::new();
    for member in value.split(',') {
        if member.is_empty() {
            continue;
        }
        let (key, value) = member.split_once('=')?;
        let key = key.trim_start_matches([' ', '\t']);
        let value = value.trim_end_matches([' ', '\t']);
        if !valid_key(key) || !valid_value(value) || members.iter().any(|(k, _)| *k == key) {
            return None;
        }
        members.push((key, value));
        if members.len() > 32 {
            return None;
        }
    }
    let members: Vec<String> = members.iter().map(|(k, v)| format!("{k}={v}")).collect();
    Some(members.join(","))
}

fn valid_key(key: &str) -> bool {
    let remain = |rest: &str| {
        rest.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_-*/".contains(&b))
    };
    let part = |key: &str, n: usize| {
        key.as_bytes()
            .first()
            .is_some_and(|b| b.is_ascii_lowercase())
            && key.len() - 1 <= n
            && remain(&key[1..])
    };
    match key.split_once('@') {
        None => part(key, 255),
        Some((tenant, system)) => {
            tenant
                .as_bytes()
                .first()
                .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                && tenant.len() - 1 <= 240
                && remain(&tenant[1..])
                && part(system, 13)
        }
    }
}

fn valid_value(value: &str) -> bool {
    let bytes = value.as_bytes();
    let char_ok = |b: u8| (0x20..=0x7e).contains(&b) && b != b',' && b != b'=';
    match bytes.split_last() {
        Some((last, rest)) => {
            bytes.len() <= 256
                && rest.iter().all(|b| char_ok(*b))
                && *last != b' '
                && char_ok(*last)
        }
        None => false,
    }
}

/// B3's `Extract`: the single header when it is valid, else the multiple headers.
fn b3(
    single: &str,
    trace: &str,
    span: &str,
    parent: &str,
    sampled: &str,
    flags: &str,
) -> Option<SpanContext> {
    if !single.is_empty()
        && let Some(context) = b3_single(single)
    {
        return Some(context);
    }
    b3_multiple(trace, span, parent, sampled, flags)
}

/// `extractMultiple`: `X-B3-Sampled` is `0`, `1`, `true` or `false` (absent: deferred, so
/// not sampled); `X-B3-Flags: 1` (debug) samples; a 64-bit trace ID is padded.
fn b3_multiple(
    trace: &str,
    span: &str,
    parent: &str,
    sampled: &str,
    flags: &str,
) -> Option<SpanContext> {
    let mut sampled = match sampled.to_lowercase().as_str() {
        "0" | "false" | "" => false,
        "1" | "true" => true,
        _ => return None,
    };
    if flags == "1" {
        sampled = true;
    }
    if trace.is_empty() || span.is_empty() {
        return None;
    }
    let padded = match trace.len() {
        16 => format!("0000000000000000{trace}"),
        _ => trace.to_string(),
    };
    let trace_id = trace_id(&padded)?;
    let span_id = span_id(span)?;
    if !parent.is_empty() {
        self::span_id(parent)?;
    }
    Some(remote(trace_id, span_id, sampled))
}

/// `extractSingle`: `traceid-spanid[-sampling[-parentspanid]]`, the trace ID 64 or 128
/// bits; the sampling `1`, `0`, `d` (debug, so sampled) or absent (deferred).
fn b3_single(header: &str) -> Option<SpanContext> {
    let len = header.len();
    if len < 16 + 1 + 16 || !header.is_ascii() {
        return None;
    }
    let (trace, mut pos) = match (header.as_bytes()[16], header.as_bytes().get(32)) {
        (b'-', _) => (format!("0000000000000000{}", &header[..16]), 16),
        (_, Some(b'-')) => (header[..32].to_string(), 32),
        _ => return None,
    };
    let trace_id = trace_id(&trace)?;
    pos += 1;
    if len < pos + 16 {
        return None;
    }
    let span_id = span_id(&header[pos..pos + 16])?;
    pos += 16;
    let mut sampling = "";
    if len > pos {
        if len == pos + 1 {
            return None;
        }
        pos += 1;
        match len - pos {
            1 => sampling = &header[pos..],
            // `{sampling}-{parentspanid}`: the separator is not checked.
            18 => {
                sampling = &header[pos..pos + 1];
                self::span_id(&header[pos + 2..])?;
            }
            _ => return None,
        }
    }
    let sampled = match sampling {
        "" | "0" => false,
        "1" | "d" => true,
        _ => return None,
    };
    Some(remote(trace_id, span_id, sampled))
}

fn remote(trace_id: [u8; 16], span_id: [u8; 8], sampled: bool) -> SpanContext {
    SpanContext {
        trace_id,
        span_id,
        flags: u8::from(sampled),
        trace_state: String::new(),
        remote: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const SPAN: &str = "00f067aa0ba902b7";

    #[test]
    fn traceparents_parse_like_go() {
        let parsed = trace_context(&format!("00-{TRACE}-{SPAN}-03"), " a=1 , b@c=2,,").unwrap();
        assert_eq!(parsed.flags, 3);
        assert_eq!(parsed.trace_state, "a=1,b@c=2");
        assert!(trace_context(&format!("00-{TRACE}-{SPAN}-04"), "").is_none());
        assert!(trace_context(&format!("00-{TRACE}-{SPAN}-01-x"), "").is_none());
        assert!(trace_context(&format!("01-{TRACE}-{SPAN}-01-x"), "").is_some());
        assert!(trace_context(&format!("00-{}-{SPAN}-01", TRACE.to_uppercase()), "").is_none());
        let bad_state = trace_context(&format!("00-{TRACE}-{SPAN}-01"), "A=1").unwrap();
        assert_eq!(bad_state.trace_state, "");
    }

    #[test]
    fn b3_parses_like_go() {
        let single = b3_single(&format!("{}-{SPAN}-1", &TRACE[16..])).unwrap();
        assert_eq!(&single.trace_id[..8], &[0; 8]);
        assert!(single.sampled());
        assert!(!b3_single(&format!("{TRACE}-{SPAN}")).unwrap().sampled());
        assert!(
            b3_single(&format!("{TRACE}-{SPAN}-d-{SPAN}"))
                .unwrap()
                .sampled()
        );
        assert!(b3_single(&format!("{TRACE}-{SPAN}-")).is_none());
        assert!(b3_single(&format!("{TRACE}-{SPAN}-{SPAN}")).is_none());
        assert!(b3_multiple(TRACE, SPAN, "", "true", "").unwrap().sampled());
        assert!(b3_multiple(TRACE, SPAN, "", "", "1").unwrap().sampled());
        assert!(b3_multiple(TRACE, "", "", "1", "").is_none());
        assert!(b3_multiple(TRACE, SPAN, "", "yes", "").is_none());
    }
}
