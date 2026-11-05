//! Go's `http.ServeMux` (the routing of Go 1.22 and later) for literal patterns: exact paths,
//! and subtrees ending with `/`. Patterns with a method, a host or wildcards are not
//! supported; CONNECT requests are routed like the others.

/// A pattern: its unescaped segments, and whether it is a subtree (`/a/` is `/a/{...}`).
struct Pattern {
    segments: Vec<String>,
    subtree: bool,
}

/// What a request goes to.
pub enum Route<'a, H> {
    Handler(&'a H),
    /// `*` as the target (but `OPTIONS *`, which the server answers): an empty 400.
    BadRequest,
    /// A 307 redirect to this location.
    Redirect(String),
    NotFound,
}

pub struct ServeMux<H> {
    patterns: Vec<(Pattern, H)>,
}

impl<H> Default for ServeMux<H> {
    fn default() -> Self {
        Self {
            patterns: Vec::new(),
        }
    }
}

impl<H> ServeMux<H> {
    /// `Handle` for a literal pattern: a clean absolute path.
    pub fn handle(&mut self, pattern: &str, handler: H) -> Result<(), String> {
        let unsupported = !pattern.starts_with('/')
            || pattern.contains([' ', '\t', '{', '}'])
            || clean_path(pattern) != pattern;
        if unsupported {
            return Err(format!("unsupported pattern {pattern:?}"));
        }
        let subtree = pattern.ends_with('/');
        let mut segments = segments(pattern);
        if subtree {
            segments.pop();
        }
        self.patterns.push((Pattern { segments, subtree }, handler));
        Ok(())
    }

    /// The tree's match for a clean path: the exact pattern for all its segments, else the
    /// subtree with the longest prefix that leaves some of the path.
    fn find(&self, path: &str) -> Option<&(Pattern, H)> {
        let segments = segments(path);
        self.patterns
            .iter()
            .find(|(p, _)| !p.subtree && p.segments == segments)
            .or_else(|| {
                self.patterns
                    .iter()
                    .filter(|(p, _)| {
                        p.subtree
                            && p.segments.len() < segments.len()
                            && segments.starts_with(&p.segments)
                    })
                    .max_by_key(|(p, _)| p.segments.len())
            })
    }

    /// `exactMatch`: a subtree matches exactly only its own path with the trailing slash.
    fn exact(found: Option<&(Pattern, H)>, path: &str) -> bool {
        match found {
            None => false,
            Some((p, _)) if !p.subtree => true,
            Some((p, _)) => {
                path.ends_with('/') && p.segments.len() + 1 == path.matches('/').count()
            }
        }
    }

    /// `ServeHTTP` and `findHandler`, for `escaped_path` (`URL.EscapedPath`) and the raw
    /// query: a path missing its subtree's trailing slash, then an unclean path (but for
    /// CONNECT, which is not cleaned), is redirected to the clean path, still escaped
    /// (`urlFromEscaped`).
    pub fn route(&self, escaped_path: &str, query: Option<&str>, connect: bool) -> Route<'_, H> {
        if escaped_path == "*" {
            return Route::BadRequest;
        }
        let with_query = |location: String| match query {
            Some(query) if !query.is_empty() => format!("{location}?{query}"),
            _ => location,
        };
        let path = match connect {
            true => escaped_path.to_string(),
            false => clean_path(escaped_path),
        };
        let found = self.find(&path);
        if !Self::exact(found, &path) && !path.ends_with('/') && !path.is_empty() {
            let slashed = format!("{path}/");
            if Self::exact(self.find(&slashed), &slashed) {
                return Route::Redirect(with_query(slashed));
            }
        }
        if path != escaped_path {
            return Route::Redirect(with_query(path));
        }
        match found {
            Some((_, handler)) => Route::Handler(handler),
            None => Route::NotFound,
        }
    }
}

/// A path's segments, each unescaped when it can be (`firstSegment`), with an empty last one
/// for a trailing slash.
fn segments(path: &str) -> Vec<String> {
    if path == "/" {
        return vec![String::new()];
    }
    path.split('/')
        .skip(1)
        .map(|segment| crate::go::unescape(segment, false).unwrap_or_else(|| segment.to_string()))
        .collect()
}

/// Go's `cleanPath`: `path.Clean` of a rooted path, keeping a trailing slash.
pub fn clean_path(path: &str) -> String {
    if path.is_empty() {
        return "/".into();
    }
    let rooted = match path.starts_with('/') {
        true => path.to_string(),
        false => format!("/{path}"),
    };
    let mut clean = crate::go::clean(&rooted);
    if rooted.ends_with('/') && clean != "/" {
        clean.push('/');
    }
    clean
}
