//! What Go's `net/http` server does around its handlers, for the service's HTTP servers: the
//! checks before routing, `OPTIONS *`, the helpers handlers answer with, and serving
//! connections.

use bytes::Bytes;
use http_body_util::Full;

pub type Request = hyper::Request<hyper::body::Incoming>;

/// An answer, before it becomes a hyper response.
pub struct Answer {
    pub status: u16,
    /// A reason phrase other than the status's own.
    pub reason: Option<String>,
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
}

impl Answer {
    pub fn new(status: u16, headers: Vec<(&'static str, String)>, body: &[u8]) -> Self {
        Self {
            status,
            reason: None,
            headers,
            body: body.to_vec(),
        }
    }

    /// A handler that sets no content type: Go sniffs one from the body, if any.
    pub fn sniffed(status: u16, mut headers: Vec<(&'static str, String)>, body: &[u8]) -> Self {
        if !body.is_empty() {
            let content_type = detect_content_type(body);
            headers.push(("content-type", content_type.into()));
        }
        Self::new(status, headers, body)
    }

    /// `http.Error`: the text, as plain text.
    pub fn error(status: u16, text: &str) -> Self {
        let headers = vec![
            ("content-type", "text/plain; charset=utf-8".into()),
            ("x-content-type-options", "nosniff".into()),
        ];
        Self::new(status, headers, format!("{text}\n").as_bytes())
    }

    /// `http.Redirect` with 307 (Go's `ServeMux`), for a location that is already an
    /// escaped absolute path: an HTML link for a GET. The header has its non-ASCII bytes
    /// escaped (`hexEscapeNonASCII`).
    pub fn redirect(location: &str, method: &hyper::Method) -> Self {
        let header: String = location
            .bytes()
            .map(|b| match b.is_ascii() {
                true => (b as char).to_string(),
                false => format!("%{b:x}"),
            })
            .collect();
        let mut headers = vec![("location", header)];
        let mut body = Vec::new();
        if matches!(*method, hyper::Method::GET | hyper::Method::HEAD) {
            headers.push(("content-type", "text/html; charset=utf-8".into()));
        }
        if *method == hyper::Method::GET {
            let escaped = location
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&#34;")
                .replace('\'', "&#39;");
            body = format!("<a href=\"{escaped}\">Temporary Redirect</a>.\n\n").into_bytes();
        }
        Self::new(307, headers, &body)
    }

    /// What `ServeMux` answers for a `*` target: an empty 400 that closes the connection.
    pub fn star(version: hyper::Version) -> Self {
        let mut headers = Vec::new();
        if version >= hyper::Version::HTTP_11 {
            headers.push(("connection", "close".into()));
        }
        Self::new(400, headers, b"")
    }

    /// Go's server refusing a request before any handler (`conn.serve`): the connection is
    /// closed, and with a reason, it follows the status in the status line and the body.
    fn bad_request(reason: Option<&str>) -> Self {
        let text = match reason {
            Some(reason) => format!("Bad Request: {reason}"),
            None => "Bad Request".into(),
        };
        let headers = vec![
            ("content-type", "text/plain; charset=utf-8".into()),
            ("connection", "close".into()),
        ];
        Self {
            reason: reason.map(|_| text.clone()),
            ..Self::new(400, headers, format!("400 {text}").as_bytes())
        }
    }

    fn into_response(self) -> hyper::Response<Full<Bytes>> {
        let mut response = hyper::Response::builder().status(self.status);
        if let Some(reason) = self.reason {
            let reason = hyper::ext::ReasonPhrase::try_from(reason.into_bytes());
            response = response.extension(reason.expect("a reason phrase from Go's"));
        }
        for (name, value) in self.headers {
            response = response.header(name, value);
        }
        response.body(Full::new(Bytes::from(self.body))).unwrap()
    }
}

/// What Go's server answers itself: requests it refuses before routing (`conn.readRequest`,
/// `url.ParseRequestURI`), and `OPTIONS *`.
fn answered_by_server(request: &Request) -> Option<Answer> {
    let uri = request.uri();
    if crate::go::unescape_bytes(uri.path(), false).is_none() {
        return Some(Answer::bad_request(None));
    }
    let hosts = request.headers().get_all(hyper::header::HOST);
    let mut hosts = hosts.iter();
    let (first_host, more_hosts) = (hosts.next(), hosts.next().is_some());
    if more_hosts {
        // "too many Host headers", a plain bad request.
        return Some(Answer::bad_request(None));
    }
    if request.version() >= hyper::Version::HTTP_11
        && first_host.is_none()
        && request.method() != hyper::Method::CONNECT
    {
        return Some(Answer::bad_request(Some("missing required Host header")));
    }
    if let Some(host) = first_host
        && !host
            .as_bytes()
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"!$%&'()*+,-.:;=[]_~".contains(b))
    {
        return Some(Answer::bad_request(Some("malformed Host header")));
    }
    if request.method() == hyper::Method::OPTIONS && uri == "*" {
        return Some(Answer::new(200, Vec::new(), b""));
    }
    None
}

/// Serves HTTP/1 requests from `listener` with `handler` until `stop` resolves, then closes
/// at once (`http.Server.Close`).
pub async fn serve<H, F>(
    listener: tokio::net::TcpListener,
    handler: H,
    stop: impl std::future::Future<Output = ()>,
) where
    H: Fn(Request) -> F + Clone + Send + Sync + 'static,
    F: std::future::Future<Output = Answer> + Send + 'static,
{
    tokio::pin!(stop);
    let mut connections = tokio::task::JoinSet::new();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            // Finished connections are reaped, or the set would keep each one.
            Some(_) = connections.join_next() => continue,
            () = &mut stop => break,
        };
        let Ok((stream, _)) = accepted else {
            continue;
        };
        crate::net::nodelay(&stream);
        let handler = handler.clone();
        connections.spawn(async move {
            let service = hyper::service::service_fn(move |request: Request| {
                let handler = handler.clone();
                async move {
                    let answer = match answered_by_server(&request) {
                        Some(answer) => answer,
                        None => handler(request).await,
                    };
                    Ok::<_, std::convert::Infallible>(answer.into_response())
                }
            });
            let io = hyper_util::rt::TokioIo::new(stream);
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await;
        });
    }
    connections.abort_all();
}

/// The request's `URL.EscapedPath`: `*` for that target, empty for an authority (CONNECT),
/// else [`escaped_path`] of its path (hyper gives `/` to an absolute target without one).
pub fn request_path(uri: &hyper::Uri) -> Option<String> {
    if uri == "*" {
        return Some("*".into());
    }
    escaped_path(uri.path_and_query().map_or("", |target| target.path()))
}

/// `url.URL.EscapedPath` of a request target's path: as sent when it is a valid escaping,
/// else the unescaped path escaped again (`None` for a bad `%` escape).
pub fn escaped_path(raw: &str) -> Option<String> {
    let decoded = crate::go::unescape_bytes(raw, false)?;
    let valid = raw
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"$&+,-./:;=@_~!'()*[]%".contains(&b));
    Some(match valid {
        true => raw.to_string(),
        false => crate::go::escape_path(&decoded),
    })
}

/// `http.DetectContentType`: the content type Go's server gives a response that sets none,
/// from its first 512 bytes (the WHATWG sniffing algorithm).
pub fn detect_content_type(data: &[u8]) -> &'static str {
    let data = &data[..data.len().min(512)];
    let is_ws = |b: u8| matches!(b, b'\t' | b'\n' | b'\x0c' | b'\r' | b' ');
    let first_non_ws = data.iter().position(|&b| !is_ws(b)).unwrap_or(data.len());
    let trimmed = &data[first_non_ws..];
    for tag in [
        &b"<!DOCTYPE HTML"[..],
        b"<HTML",
        b"<HEAD",
        b"<SCRIPT",
        b"<IFRAME",
        b"<H1",
        b"<DIV",
        b"<FONT",
        b"<TABLE",
        b"<A",
        b"<STYLE",
        b"<TITLE",
        b"<B",
        b"<BODY",
        b"<BR",
        b"<P",
        b"<!--",
    ] {
        // Letters match either case; a space or `>` must follow.
        let matches = trimmed.len() > tag.len()
            && tag.iter().zip(trimmed).all(|(&t, &d)| {
                let d = if t.is_ascii_uppercase() { d & 0xDF } else { d };
                t == d
            })
            && matches!(trimmed[tag.len()], b' ' | b'>');
        if matches {
            return "text/html; charset=utf-8";
        }
    }
    let masked = |data: &[u8], mask: &[u8], pattern: &[u8]| {
        data.len() >= pattern.len()
            && pattern
                .iter()
                .zip(mask)
                .zip(data)
                .all(|((&p, &m), &d)| d & m == p)
    };
    if masked(trimmed, b"\xFF\xFF\xFF\xFF\xFF", b"<?xml") {
        return "text/xml; charset=utf-8";
    }
    let signatures: &[(&[u8], &[u8], &str)] = &[
        (b"\xFF\xFF\xFF\xFF\xFF", b"%PDF-", "application/pdf"),
        (
            b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
            b"%!PS-Adobe-",
            "application/postscript",
        ),
        (
            b"\xFF\xFF\x00\x00",
            b"\xFE\xFF\x00\x00",
            "text/plain; charset=utf-16be",
        ),
        (
            b"\xFF\xFF\x00\x00",
            b"\xFF\xFE\x00\x00",
            "text/plain; charset=utf-16le",
        ),
        (
            b"\xFF\xFF\xFF\x00",
            b"\xEF\xBB\xBF\x00",
            "text/plain; charset=utf-8",
        ),
        (b"\xFF\xFF\xFF\xFF", b"\x00\x00\x01\x00", "image/x-icon"),
        (b"\xFF\xFF\xFF\xFF", b"\x00\x00\x02\x00", "image/x-icon"),
        (b"\xFF\xFF", b"BM", "image/bmp"),
        (b"\xFF\xFF\xFF\xFF\xFF\xFF", b"GIF87a", "image/gif"),
        (b"\xFF\xFF\xFF\xFF\xFF\xFF", b"GIF89a", "image/gif"),
        (
            b"\xFF\xFF\xFF\xFF\x00\x00\x00\x00\xFF\xFF\xFF\xFF\xFF\xFF",
            b"RIFF\x00\x00\x00\x00WEBPVP",
            "image/webp",
        ),
        (
            b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
            b"\x89PNG\x0D\x0A\x1A\x0A",
            "image/png",
        ),
        (b"\xFF\xFF\xFF", b"\xFF\xD8\xFF", "image/jpeg"),
        (
            b"\xFF\xFF\xFF\xFF\x00\x00\x00\x00\xFF\xFF\xFF\xFF",
            b"FORM\x00\x00\x00\x00AIFF",
            "audio/aiff",
        ),
        (b"\xFF\xFF\xFF", b"ID3", "audio/mpeg"),
        (b"\xFF\xFF\xFF\xFF\xFF", b"OggS\x00", "application/ogg"),
        (
            b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
            b"MThd\x00\x00\x00\x06",
            "audio/midi",
        ),
        (
            b"\xFF\xFF\xFF\xFF\x00\x00\x00\x00\xFF\xFF\xFF\xFF",
            b"RIFF\x00\x00\x00\x00AVI ",
            "video/avi",
        ),
        (
            b"\xFF\xFF\xFF\xFF\x00\x00\x00\x00\xFF\xFF\xFF\xFF",
            b"RIFF\x00\x00\x00\x00WAVE",
            "audio/wave",
        ),
    ];
    for (mask, pattern, content_type) in signatures {
        if masked(data, mask, pattern) {
            return content_type;
        }
    }
    if data.len() >= 12 {
        // An MP4 `ftyp` box naming an `mp4` brand.
        let size = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        if data.len() >= size
            && size.is_multiple_of(4)
            && &data[4..8] == b"ftyp"
            && (8..size)
                .step_by(4)
                .any(|at| at != 12 && &data[at..at + 3] == b"mp4")
        {
            return "video/mp4";
        }
    }
    if data.starts_with(b"\x1A\x45\xDF\xA3") {
        return "video/webm";
    }
    let mut font = [0u8; 36];
    font[34..].copy_from_slice(b"\xFF\xFF");
    let mut font_pattern = [0u8; 36];
    font_pattern[34..].copy_from_slice(b"LP");
    if masked(data, &font, &font_pattern) {
        return "application/vnd.ms-fontobject";
    }
    for (prefix, content_type) in [
        (&b"\x00\x01\x00\x00"[..], "font/ttf"),
        (b"OTTO", "font/otf"),
        (b"ttcf", "font/collection"),
        (b"wOFF", "font/woff"),
        (b"wOF2", "font/woff2"),
        (b"\x1F\x8B\x08", "application/x-gzip"),
        (b"PK\x03\x04", "application/zip"),
        (b"Rar!\x1A\x07\x00", "application/x-rar-compressed"),
        (b"Rar!\x1A\x07\x01\x00", "application/x-rar-compressed"),
        (b"\x00\x61\x73\x6D", "application/wasm"),
    ] {
        if data.starts_with(prefix) {
            return content_type;
        }
    }
    let binary = |b: u8| matches!(b, 0x00..=0x08 | 0x0B | 0x0E..=0x1A | 0x1C..=0x1F);
    match trimmed.iter().any(|&b| binary(b)) {
        true => "application/octet-stream",
        false => "text/plain; charset=utf-8",
    }
}
