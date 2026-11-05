//! Go standard library functions shared across the crate, ported for their exact behaviour:
//! parsing (`strconv`, `time.ParseDuration`), formatting floats and quoting, with Go's error
//! texts. (Narrower ports stay next to their single use, e.g. yaml.v2's integer resolution.)

use std::fmt::Write;

/// `strconv.Quote`: `\"` and `\\`, the C escapes, `\xNN` for other ASCII control
/// characters, `\uNNNN` or `\UNNNNNNNN` for non-printable non-ASCII characters (printable as
/// Go's `unicode.IsPrint`: letters, marks, numbers, punctuation and symbols).
pub fn quote(s: &str) -> String {
    static PRINTABLE: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"^[\p{L}\p{M}\p{N}\p{P}\p{S}]$").unwrap());
    let mut out = String::from('"');
    for c in s.chars() {
        match c {
            '"' => out += "\\\"",
            '\\' => out += "\\\\",
            '\x07' => out += "\\a",
            '\x08' => out += "\\b",
            '\x0c' => out += "\\f",
            '\n' => out += "\\n",
            '\r' => out += "\\r",
            '\t' => out += "\\t",
            '\x0b' => out += "\\v",
            c if (c as u32) < 0x20 || c == '\x7f' => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c if c.is_ascii() || PRINTABLE.is_match(c.encode_utf8(&mut [0; 4])) => out.push(c),
            c if (c as u32) < 0x10000 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => {
                let _ = write!(out, "\\U{:08x}", c as u32);
            }
        }
    }
    out.push('"');
    out
}

/// `strings.ToUpper`: Go maps each character to one character only (`ß` stays `ß`).
pub fn to_upper(s: &str) -> String {
    s.chars()
        .map(|c| {
            let mut upper = c.to_uppercase();
            match (upper.next(), upper.next()) {
                (Some(u), None) => u,
                _ => c,
            }
        })
        .collect()
}

/// `strconv.FormatFloat(v, 'g', -1, 64)`, which is also `fmt`'s `%v`: the shortest digits,
/// in exponent form below 1e-4 and from 1e6 (`1e+06`); `+Inf`, `-Inf`, `NaN`.
pub fn format_float(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    // Shortest round-trip digits and decimal exponent, from Rust's `{:e}` formatting.
    let sci = format!("{v:e}");
    let (mantissa, exp) = sci.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();
    // With the shortest representation, Go switches to %e at exponent 6.
    if !(-4..6).contains(&exp) {
        let exp_sign = if exp < 0 { '-' } else { '+' };
        return format!("{mantissa}e{exp_sign}{:02}", exp.abs());
    }
    format!("{v}")
}

/// `strconv.FormatFloat(v, 'f', precision, 64)`.
pub fn format_fixed(v: f64, precision: usize) -> String {
    match v {
        v if v.is_nan() => "NaN".into(),
        v if v.is_infinite() => if v > 0.0 { "+Inf" } else { "-Inf" }.into(),
        v => format!("{v:.precision$}"),
    }
}

/// `path.Clean` (and `filepath.Clean` on Unix): the shortest equivalent path, lexically.
pub fn clean(path: &str) -> String {
    if path.is_empty() {
        return ".".into();
    }
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." if parts.last().is_some_and(|p| *p != "..") => {
                parts.pop();
            }
            ".." if rooted => {}
            part => parts.push(part),
        }
    }
    let cleaned = parts.join("/");
    match (rooted, cleaned.is_empty()) {
        (true, _) => format!("/{cleaned}"),
        (false, true) => ".".into(),
        (false, false) => cleaned,
    }
}

/// `filepath.Join` on Unix: the non-empty elements joined with `/`, then cleaned; empty when
/// all are empty.
pub fn join(elements: &[&str]) -> String {
    let joined = elements
        .iter()
        .filter(|e| !e.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join("/");
    if joined.is_empty() {
        return String::new();
    }
    clean(&joined)
}

/// `filepath.Dir` on Unix: everything before the last `/`, cleaned.
pub fn dir(path: &str) -> String {
    let i = path.rfind('/').map_or(0, |i| i + 1);
    clean(&path[..i])
}

/// `filepath.Base` on Unix: the last element, without trailing `/`; `.` for an empty path,
/// `/` for one of slashes only.
pub fn base(path: &str) -> String {
    if path.is_empty() {
        return ".".into();
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".into();
    }
    trimmed.rsplit('/').next().unwrap_or(trimmed).to_string()
}

/// `time.Duration.String`: `1h2m3.5s`, `1.5s`, `250ms`, `1.5µs`, `0s`.
pub fn format_duration(nanos: i64) -> String {
    /// `fmtFrac`: the fraction of `v` (in units of 10^-prec), without trailing zeros.
    fn fraction(v: u64, prec: u32) -> (String, u64) {
        let digits = format!("{:0width$}", v % 10u64.pow(prec), width = prec as usize);
        let digits = digits.trim_end_matches('0');
        let text = match digits.is_empty() {
            true => String::new(),
            false => format!(".{digits}"),
        };
        (text, v / 10u64.pow(prec))
    }
    let u = nanos.unsigned_abs();
    let sign = if nanos < 0 { "-" } else { "" };
    if u == 0 {
        return "0s".into();
    }
    if u < 1_000_000_000 {
        let (prec, unit) = match u {
            _ if u < 1_000 => (0, "ns"),
            _ if u < 1_000_000 => (3, "\u{b5}s"),
            _ => (6, "ms"),
        };
        let (frac, whole) = fraction(u, prec);
        return format!("{sign}{whole}{frac}{unit}");
    }
    let (frac, seconds) = fraction(u, 9);
    let (minutes, hours) = (seconds / 60, seconds / 3600);
    match (hours, minutes) {
        (0, 0) => format!("{sign}{seconds}{frac}s"),
        (0, _) => format!("{sign}{minutes}m{}{frac}s", seconds % 60),
        _ => format!("{sign}{hours}h{}m{}{frac}s", minutes % 60, seconds % 60),
    }
}

/// `net.SplitHostPort`: the host (without brackets) and port of `host:port`, or Go's error.
pub fn split_host_port(address: &str) -> Result<(String, String), String> {
    let error = |reason: &str| format!("address {address}: {reason}");
    let Some(colon) = address.rfind(':') else {
        return Err(error("missing port in address"));
    };
    let (host, port) = (&address[..colon], &address[colon + 1..]);
    if let Some(inner) = host.strip_prefix('[') {
        let inner = inner
            .strip_suffix(']')
            .ok_or_else(|| error("missing ']' in address"))?;
        if inner.contains(['[', ']']) {
            return Err(error("unexpected '[' in address"));
        }
        return Ok((inner.to_string(), port.to_string()));
    }
    if host.contains(':') {
        return Err(error("too many colons in address"));
    }
    if host.contains(['[', ']']) {
        return Err(error("unexpected ']' in address"));
    }
    Ok((host.to_string(), port.to_string()))
}

/// `url.PathUnescape` (or `url.QueryUnescape` with `query`, where `+` is a space): `None` for
/// a malformed `%` escape, as Go refuses it.
pub fn unescape(s: &str, query: bool) -> Option<String> {
    unescape_bytes(s, query).map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
}

/// [`unescape`], keeping the bytes, which Go's strings need not be UTF-8.
pub fn unescape_bytes(s: &str, query: bool) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        match bytes[i] {
            b'%' => {
                let high = bytes.get(i + 1).copied().and_then(hex)?;
                let low = bytes.get(i + 2).copied().and_then(hex)?;
                out.push((high * 16 + low) as u8);
                i += 3;
            }
            b'+' if query => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Some(out)
}

/// `url.URL.EscapedPath` of an unescaped path: bytes outside `[A-Za-z0-9$&+,-./:;=@_~]` as
/// `%XX`.
pub fn escape_path(path: &[u8]) -> String {
    let mut out = String::with_capacity(path.len());
    for &b in path {
        if b.is_ascii_alphanumeric() || b"$&+,-./:;=@_~".contains(&b) {
            out.push(b as char);
        } else {
            out += &format!("%{b:02X}");
        }
    }
    out
}

/// Why a `strconv` parser failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumError {
    Syntax,
    Range,
}

/// A `*strconv.NumError`'s text.
fn num_error(function: &str, text: &str, error: NumError) -> String {
    let reason = match error {
        NumError::Syntax => "invalid syntax",
        NumError::Range => "value out of range",
    };
    format!("strconv.{function}: parsing {}: {reason}", quote(text))
}

/// `strconv.ParseBool`.
pub fn parse_bool(text: &str) -> Result<bool, String> {
    match text {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(num_error("ParseBool", text, NumError::Syntax)),
    }
}

/// `strconv.Atoi`: a decimal integer with an optional sign, without underscores.
pub fn atoi(text: &str) -> Result<i64, String> {
    let error = |e| num_error("Atoi", text, e);
    let (negative, digits) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    if digits.is_empty() {
        return Err(error(NumError::Syntax));
    }
    // From 19 characters Go goes through `ParseInt`, which stops at the first overflow.
    let mut magnitude: u64 = 0;
    for c in digits.chars() {
        let digit = c.to_digit(10).ok_or(error(NumError::Syntax))?;
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|m| m.checked_add(u64::from(digit)))
            .ok_or(error(NumError::Range))?;
    }
    match negative {
        false if magnitude < 1 << 63 => Ok(magnitude as i64),
        true if magnitude <= 1 << 63 => Ok((magnitude as i64).wrapping_neg()),
        _ => Err(error(NumError::Range)),
    }
}

/// Whether the underscores in a number are where Go allows them: only between digits, or
/// between a base prefix and a digit (`underscoreOK`).
fn underscores_ok(s: &str) -> bool {
    let s = s.strip_prefix(['+', '-']).unwrap_or(s);
    let bytes = s.as_bytes();
    let mut saw = b'^';
    let mut i = 0;
    let mut hex = false;
    if bytes.len() >= 2 && bytes[0] == b'0' && matches!(bytes[1] | 0x20, b'b' | b'o' | b'x') {
        i = 2;
        saw = b'0';
        hex = bytes[1] | 0x20 == b'x';
    }
    for &c in &bytes[i..] {
        if c.is_ascii_digit() || (hex && (c | 0x20).is_ascii_hexdigit()) {
            saw = b'0';
        } else if c == b'_' {
            if saw != b'0' {
                return false;
            }
            saw = b'_';
        } else {
            if saw == b'_' {
                return false;
            }
            saw = b'!';
        }
    }
    saw != b'_'
}

/// `strconv.ParseInt(text, 0, bits)`: a sign, then a `0x`, `0o`, `0b` or `0` (octal) prefix,
/// with `_` between digits.
pub fn parse_int(text: &str, bits: u32) -> Result<i64, String> {
    let error = |e| num_error("ParseInt", text, e);
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let magnitude = parse_uint(unsigned).map_err(error)?;
    let cutoff = 1u64 << (bits - 1);
    match negative {
        false if magnitude < cutoff => Ok(magnitude as i64),
        true if magnitude <= cutoff => Ok((magnitude as i64).wrapping_neg()),
        _ => Err(error(NumError::Range)),
    }
}

/// `strconv.ParseInt(text, 10, bits)`: a sign, then decimal digits only.
pub fn parse_int10(text: &str, bits: u32) -> Result<i64, String> {
    let error = |e| num_error("ParseInt", text, e);
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let magnitude = parse_decimal(unsigned).map_err(error)?;
    let cutoff = 1u64 << (bits - 1);
    match negative {
        false if magnitude < cutoff => Ok(magnitude as i64),
        true if magnitude <= cutoff => Ok((magnitude as i64).wrapping_neg()),
        _ => Err(error(NumError::Range)),
    }
}

/// `strconv.ParseUint(text, 10, 64)`.
pub fn parse_uint10(text: &str) -> Result<u64, String> {
    parse_decimal(text).map_err(|e| num_error("ParseUint", text, e))
}

/// Decimal digits, none other: Go's base-10 parse.
fn parse_decimal(text: &str) -> Result<u64, NumError> {
    if text.is_empty() {
        return Err(NumError::Syntax);
    }
    let mut value: u64 = 0;
    for c in text.bytes() {
        if !c.is_ascii_digit() {
            return Err(NumError::Syntax);
        }
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(c - b'0')))
            .ok_or(NumError::Range)?;
    }
    Ok(value)
}

/// `strconv.ParseUint(text, 0, bits)`.
pub fn parse_uint_bits(text: &str, bits: u32) -> Result<u64, String> {
    let error = |e| num_error("ParseUint", text, e);
    let value = parse_uint(text).map_err(error)?;
    if bits < 64 && value >> bits != 0 {
        return Err(error(NumError::Range));
    }
    Ok(value)
}

/// `strconv.ParseUint(text, 0, 64)`.
fn parse_uint(text: &str) -> Result<u64, NumError> {
    if text.is_empty() {
        return Err(NumError::Syntax);
    }
    let bytes = text.as_bytes();
    let (base, digits) = match bytes {
        [b'0', p, _, ..] if *p | 0x20 == b'b' => (2, &text[2..]),
        [b'0', p, _, ..] if *p | 0x20 == b'o' => (8, &text[2..]),
        [b'0', p, _, ..] if *p | 0x20 == b'x' => (16, &text[2..]),
        [b'0', ..] => (8, &text[1..]),
        _ => (10, text),
    };
    let mut value: u64 = 0;
    let mut underscores = false;
    for c in digits.chars() {
        if c == '_' {
            underscores = true;
            continue;
        }
        let digit = c.to_digit(base).ok_or(NumError::Syntax)?;
        // Go stops at the first overflow, before looking at the rest.
        value = value
            .checked_mul(u64::from(base))
            .and_then(|v| v.checked_add(u64::from(digit)))
            .ok_or(NumError::Range)?;
    }
    if underscores && !underscores_ok(text) {
        return Err(NumError::Syntax);
    }
    Ok(value)
}

/// `strconv.ParseFloat(text, bits)`: decimal or hexadecimal (`0x1.8p3`) floats with `_`
/// between digits, `inf`/`infinity` (signed) and `nan`; a finite value too large for the size
/// is out of range.
pub fn parse_float(text: &str, bits: u32) -> Result<f64, String> {
    let error = |e| num_error("ParseFloat", text, e);
    let (sign, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (-1.0, &text[1..]),
        Some(b'+') => (1.0, &text[1..]),
        _ => (1.0, text),
    };
    let lower = unsigned.to_ascii_lowercase();
    if lower == "inf" || lower == "infinity" {
        return Ok(sign * f64::INFINITY);
    }
    if text.eq_ignore_ascii_case("nan") {
        return Ok(f64::NAN);
    }
    if unsigned.contains('_') && !underscores_ok(text) {
        return Err(error(NumError::Syntax));
    }
    let cleaned = unsigned.replace('_', "");
    let magnitude = if lower.len() > 2 && lower.starts_with("0x") {
        parse_hex_float(&cleaned[2..], bits).map_err(error)?
    } else {
        let valid = {
            let (mantissa, exponent) = match cleaned.find(['e', 'E']) {
                Some(at) => (&cleaned[..at], Some(&cleaned[at + 1..])),
                None => (cleaned.as_str(), None),
            };
            let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
            let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
            digits(whole)
                && digits(fraction)
                && !(whole.is_empty() && fraction.is_empty())
                && exponent.is_none_or(|e| {
                    let e = e.strip_prefix(['+', '-']).unwrap_or(e);
                    !e.is_empty() && digits(e)
                })
        };
        if !valid {
            return Err(error(NumError::Syntax));
        }
        if bits == 32 {
            f64::from(
                cleaned
                    .parse::<f32>()
                    .map_err(|_| error(NumError::Syntax))?,
            )
        } else {
            cleaned
                .parse::<f64>()
                .map_err(|_| error(NumError::Syntax))?
        }
    };
    if magnitude.is_infinite() {
        return Err(error(NumError::Range));
    }
    Ok(sign * magnitude)
}

/// The magnitude of a hexadecimal float after `0x` (underscores removed), as Go's
/// `readFloat` and `atofHex` read it: at most 16 significant hex digits kept (later non-zero
/// ones only remembered), a required `p` exponent capped at 10000, then one rounding to the
/// size, subnormals included.
fn parse_hex_float(s: &str, bits: u32) -> Result<f64, NumError> {
    let bytes = s.as_bytes();
    let (mut mantissa, mut nd, mut nd_mant, mut dp) = (0u64, 0i64, 0i64, 0i64);
    let (mut trunc, mut saw_dot, mut saw_digits) = (false, false, false);
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'.' if !saw_dot => {
                saw_dot = true;
                dp = nd;
            }
            b'0' if nd == 0 => {
                saw_digits = true;
                dp -= 1;
            }
            _ if c.is_ascii_hexdigit() => {
                saw_digits = true;
                nd += 1;
                if nd_mant < 16 {
                    mantissa = mantissa * 16 + u64::from((c as char).to_digit(16).unwrap());
                    nd_mant += 1;
                } else if c != b'0' {
                    trunc = true;
                }
            }
            _ => break,
        }
        i += 1;
    }
    if !saw_digits {
        return Err(NumError::Syntax);
    }
    if !saw_dot {
        dp = nd;
    }
    dp *= 4;
    nd_mant *= 4;
    if !matches!(bytes.get(i), Some(b'p' | b'P')) {
        return Err(NumError::Syntax);
    }
    i += 1;
    let sign = match bytes.get(i) {
        Some(b'+') => {
            i += 1;
            1
        }
        Some(b'-') => {
            i += 1;
            -1
        }
        _ => 1,
    };
    if !bytes.get(i).is_some_and(u8::is_ascii_digit) {
        return Err(NumError::Syntax);
    }
    let mut e: i64 = 0;
    while let Some(c) = bytes.get(i).filter(|c| c.is_ascii_digit()) {
        if e < 10000 {
            e = e * 10 + i64::from(c - b'0');
        }
        i += 1;
    }
    if i != bytes.len() {
        return Err(NumError::Syntax);
    }
    dp += e * sign;
    let exp = if mantissa != 0 { dp - nd_mant } else { 0 };
    atof_hex(mantissa, exp, trunc, bits)
}

/// Go's `atofHex`: `mantissa × 2^exp` rounded to the float size (half to even), or out of range.
fn atof_hex(mut mantissa: u64, mut exp: i64, trunc: bool, bits: u32) -> Result<f64, NumError> {
    let (mant_bits, exp_bits, bias): (u32, u32, i64) = match bits {
        32 => (23, 8, -127),
        _ => (52, 11, -1023),
    };
    let max_exp = (1i64 << exp_bits) + bias - 2;
    let min_exp = bias + 1;
    exp += i64::from(mant_bits);
    while mantissa != 0 && mantissa >> (mant_bits + 2) == 0 {
        mantissa <<= 1;
        exp -= 1;
    }
    if trunc {
        mantissa |= 1;
    }
    while mantissa >> (1 + mant_bits + 2) != 0 {
        mantissa = mantissa >> 1 | mantissa & 1;
        exp += 1;
    }
    while mantissa > 1 && exp < min_exp - 2 {
        mantissa = mantissa >> 1 | mantissa & 1;
        exp += 1;
    }
    let mut round = mantissa & 3;
    mantissa >>= 2;
    round |= mantissa & 1;
    exp += 2;
    if round == 3 {
        mantissa += 1;
        if mantissa == 1 << (1 + mant_bits) {
            mantissa >>= 1;
            exp += 1;
        }
    }
    if mantissa >> mant_bits == 0 {
        exp = bias;
    }
    if exp > max_exp {
        return Err(NumError::Range);
    }
    let fraction = mantissa & ((1 << mant_bits) - 1);
    let biased = ((exp - bias) as u64) & ((1 << exp_bits) - 1);
    let bits_value = fraction | biased << mant_bits;
    Ok(match bits {
        32 => f64::from(f32::from_bits(bits_value as u32)),
        _ => f64::from_bits(bits_value),
    })
}

/// The `time` package's own `quote`: non-ASCII and control characters as `\x` bytes.
fn time_quote(s: &str) -> String {
    let mut out = String::from('"');
    for c in s.chars() {
        if !c.is_ascii() || c < ' ' {
            let mut bytes = [0; 4];
            for b in c.encode_utf8(&mut bytes).bytes() {
                let _ = write!(out, "\\x{b:02x}");
            }
        } else {
            if c == '"' || c == '\\' {
                out.push('\\');
            }
            out.push(c);
        }
    }
    out.push('"');
    out
}

/// `time.ParseDuration`, in nanoseconds, with Go's error texts.
pub fn parse_duration(text: &str) -> Result<i64, String> {
    let invalid = || format!("time: invalid duration {}", time_quote(text));
    const OVERFLOW: u64 = 1 << 63;
    let (negative, mut s) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    if s == "0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(invalid());
    }
    let mut total: u64 = 0;
    while !s.is_empty() {
        if !(s.starts_with('.') || s.starts_with(|c: char| c.is_ascii_digit())) {
            return Err(invalid());
        }
        let digits = s.bytes().take_while(u8::is_ascii_digit).count();
        let mut whole: u64 = 0;
        for b in s[..digits].bytes() {
            if whole > OVERFLOW / 10 {
                return Err(invalid());
            }
            whole = whole * 10 + u64::from(b - b'0');
            if whole > OVERFLOW {
                return Err(invalid());
            }
        }
        s = &s[digits..];
        let mut fraction: u64 = 0;
        let mut scale = 1.0;
        let mut fraction_digits = 0;
        if let Some(after) = s.strip_prefix('.') {
            fraction_digits = after.bytes().take_while(u8::is_ascii_digit).count();
            let mut overflow = false;
            for b in after[..fraction_digits].bytes() {
                if overflow {
                    continue;
                }
                if fraction > (OVERFLOW - 1) / 10 {
                    overflow = true;
                    continue;
                }
                let next = fraction * 10 + u64::from(b - b'0');
                if next > OVERFLOW {
                    overflow = true;
                    continue;
                }
                fraction = next;
                scale *= 10.0;
            }
            s = &after[fraction_digits..];
        }
        if digits == 0 && fraction_digits == 0 {
            return Err(invalid());
        }
        let unit_len = s
            .bytes()
            .take_while(|b| *b != b'.' && !b.is_ascii_digit())
            .count();
        if unit_len == 0 {
            return Err(format!(
                "time: missing unit in duration {}",
                time_quote(text)
            ));
        }
        let unit: u64 = match &s[..unit_len] {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            other => {
                return Err(format!(
                    "time: unknown unit {} in duration {}",
                    time_quote(other),
                    time_quote(text)
                ));
            }
        };
        s = &s[unit_len..];
        if whole > OVERFLOW / unit {
            return Err(invalid());
        }
        let mut value = whole * unit;
        if fraction > 0 {
            value += (fraction as f64 * (unit as f64 / scale)) as u64;
            if value > OVERFLOW {
                return Err(invalid());
            }
        }
        total += value;
        if total > OVERFLOW {
            return Err(invalid());
        }
    }
    if negative {
        return Ok((total as i64).wrapping_neg());
    }
    i64::try_from(total).map_err(|_| invalid())
}

/// `json.Marshal` of a string of any bytes (Go 1.27): `<`, `>`, `&`, U+2028, U+2029 and
/// control characters escaped, each invalid UTF-8 byte replaced with U+FFFD.
pub fn json_string(bytes: &[u8]) -> String {
    let mut out = String::from("\"");
    let mut rest = bytes;
    while !rest.is_empty() {
        let (valid, invalid) = match std::str::from_utf8(rest) {
            Ok(text) => (text, 0),
            Err(e) => {
                let invalid = e.error_len().unwrap_or(rest.len() - e.valid_up_to());
                (
                    std::str::from_utf8(&rest[..e.valid_up_to()]).unwrap(),
                    invalid,
                )
            }
        };
        for c in valid.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\u{8}' => out.push_str("\\b"),
                '\u{c}' => out.push_str("\\f"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if c < ' ' || matches!(c, '<' | '>' | '&' | '\u{2028}' | '\u{2029}') => {
                    out.push_str(&format!("\\u{:04x}", c as u32));
                }
                c => out.push(c),
            }
        }
        // Go replaces each byte of an invalid sequence.
        for _ in 0..invalid {
            out.push('\u{fffd}');
        }
        rest = &rest[valid.len() + invalid..];
    }
    out.push('"');
    out
}

/// A `syscall.Errno`'s text: glibc's with a lower-case first letter.
pub fn errno_text(error: &std::io::Error) -> String {
    let code = error.raw_os_error().unwrap_or(0);
    let text = std::io::Error::from_raw_os_error(code).to_string();
    let text = text
        .strip_suffix(&format!(" (os error {code})"))
        .unwrap_or(&text);
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().chain(chars).collect(),
        None => String::new(),
    }
}
