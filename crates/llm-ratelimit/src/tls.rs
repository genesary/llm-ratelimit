//! `TlsConfigFromFiles` (`src/utils/tls.go`), shared by the Redis pools, the gRPC server and
//! the xDS client:
//! a key pair and a CA added to the system's roots, read as Go reads them; and x509's name
//! errors.

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject};
use std::sync::Arc;

/// The system's roots plus the certificates of `ca` (none if empty): `ca` must hold one.
pub fn roots(ca: &str) -> Result<rustls::RootCertStore, String> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }
    add_ca(&mut roots, ca)?;
    Ok(roots)
}

/// The certificates of `ca` alone: it must hold one.
pub fn ca_only(ca: &str) -> Result<rustls::RootCertStore, String> {
    let mut roots = rustls::RootCertStore::empty();
    add_ca(&mut roots, ca)?;
    Ok(roots)
}

/// Adds the certificates of `ca` (none if empty) to `roots`, as `AppendCertsFromPEM`.
fn add_ca(roots: &mut rustls::RootCertStore, ca: &str) -> Result<(), String> {
    if !ca.is_empty() {
        let pem = read(ca).map_err(|e| format!("failed to read file: {ca}: {e}"))?;
        let added = CertificateDer::pem_slice_iter(&pem)
            .filter_map(Result::ok)
            .filter(|c| roots.add(c.clone()).is_ok())
            .count();
        if added == 0 {
            return Err(format!(
                "failed to load the provided TLS CA certificate: {ca}"
            ));
        }
    }
    Ok(())
}

/// `os.ReadFile`'s error as Go writes it.
pub fn read(path: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| {
        let op = match e.kind() {
            std::io::ErrorKind::IsADirectory => "read",
            _ => "open",
        };
        format!("{op} {path}: {}", crate::go::errno_text(&e))
    })
}

/// `tls.LoadX509KeyPair`: the certificate file, then the key file, then the pair.
pub fn key_pair(
    cert: &str,
    key: &str,
    provider: &rustls::crypto::CryptoProvider,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
    let cert_pem = read(cert)?;
    let key_pem = read(key)?;
    let certs: Vec<_> = CertificateDer::pem_slice_iter(&cert_pem)
        .filter_map(Result::ok)
        .collect();
    if certs.is_empty() {
        return Err("tls: failed to find any PEM data in certificate input".into());
    }
    let key = PrivateKeyDer::from_pem_slice(&key_pem)
        .map_err(|_| "tls: failed to find any PEM data in key input".to_string())?;
    let signing = provider
        .key_provider
        .load_private_key(key.clone_key())
        .map_err(|e| format!("tls: {e}"))?;
    rustls::sign::CertifiedKey::new(certs.clone(), signing)
        .keys_match()
        .map_err(|_| "tls: private key does not match public key".to_string())?;
    Ok((certs, key))
}

/// `TlsConfigFromFiles` with a server CA: the server verified against the system's roots plus
/// `ca`, by name unless `skip_hostname_verification` (the chain still verified), with the
/// key pair of `cert` and `key` if both are set; an error is what upstream panics with.
pub fn server_ca_client(
    cert: &str,
    key: &str,
    ca: &str,
    skip_hostname_verification: bool,
) -> Result<rustls::ClientConfig, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    // A key pair needs both files.
    let identity = match (cert, key) {
        ("", _) | (_, "") => None,
        (cert, key) => Some(
            key_pair(cert, key, &provider)
                .map_err(|e| format!("failed lo load TLS key pair ({cert},{key}): {e}"))?,
        ),
    };
    let roots = Arc::new(roots(ca)?);
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?;
    // With no root at all, every handshake fails, as Go's verification would.
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider)
        .build()
        .ok();
    let builder = match skip_hostname_verification {
        true => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NameNotVerified(verifier))),
        false => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(GoNames(verifier))),
    };
    match identity {
        Some((certs, key)) => builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| format!("failed lo load TLS key pair: {e}")),
        None => Ok(builder.with_no_client_auth()),
    }
}

/// What x509's name checks read of a certificate: its SAN extension's DNS names and IP
/// addresses, and its subject's common name.
pub struct Names {
    dns: Vec<String>,
    ips: Vec<std::net::IpAddr>,
    has_san_extension: bool,
    common_name: String,
}

impl Names {
    /// The names of the DER certificate `der`; `None` if it does not parse.
    pub fn of(der: &[u8]) -> Option<Self> {
        use x509_parser::extensions::GeneralName;
        let (_, cert) = x509_parser::parse_x509_certificate(der).ok()?;
        let (mut dns, mut ips) = (Vec::new(), Vec::new());
        if let Ok(Some(san)) = cert.subject_alternative_name() {
            for name in &san.value.general_names {
                match name {
                    GeneralName::DNSName(name) => dns.push(name.to_string()),
                    GeneralName::IPAddress(bytes) => {
                        if let Ok(v4) = <[u8; 4]>::try_from(*bytes) {
                            ips.push(v4.into());
                        } else if let Ok(v6) = <[u8; 16]>::try_from(*bytes) {
                            ips.push(v6.into());
                        }
                    }
                    _ => {}
                }
            }
        }
        let has_san_extension = cert
            .extensions()
            .iter()
            .any(|e| e.oid == x509_parser::oid_registry::OID_X509_EXT_SUBJECT_ALT_NAME);
        // pkix keeps the last one.
        let common_name = cert
            .subject()
            .iter_common_name()
            .filter_map(|cn| cn.as_str().ok())
            .last()
            .unwrap_or_default()
            .to_string();
        Some(Self {
            dns,
            ips,
            has_san_extension,
            common_name,
        })
    }

    /// `Certificate.VerifyHostname`: an IP (maybe in brackets) against the IP addresses, a
    /// name against the DNS names (wildcards and a trailing dot for valid host names, exact
    /// matches otherwise); the error is `HostnameError`'s.
    pub fn verify_hostname(&self, host: &str) -> Result<(), String> {
        let candidate_ip = match host.len() >= 3 && host.starts_with('[') && host.ends_with(']') {
            true => &host[1..host.len() - 1],
            false => host,
        };
        if let Ok(ip) = candidate_ip.parse::<std::net::IpAddr>() {
            return match self.ips.iter().any(|candidate| same_ip(candidate, &ip)) {
                true => Ok(()),
                false => Err(self.hostname_error(candidate_ip)),
            };
        }
        let candidate = host.to_ascii_lowercase();
        let valid_candidate = valid_hostname(&candidate, false);
        let matched =
            self.dns
                .iter()
                .any(|name| match valid_candidate && valid_hostname(name, true) {
                    true => match_hostnames(name, &candidate),
                    false => match_exactly(name, &candidate),
                });
        match matched {
            true => Ok(()),
            false => Err(self.hostname_error(host)),
        }
    }

    /// `HostnameError.Error`.
    fn hostname_error(&self, host: &str) -> String {
        const MAX_NAMES_INCLUDED: usize = 100;
        if !self.has_san_extension && match_hostnames(&self.common_name, host) {
            return "x509: certificate relies on legacy Common Name field, use SANs instead".into();
        }
        let valid = match host.parse::<std::net::IpAddr>() {
            Ok(_) if self.ips.is_empty() => {
                return format!(
                    "x509: cannot validate certificate for {host} because it doesn't contain any IP SANs"
                );
            }
            Ok(_) if self.ips.len() >= MAX_NAMES_INCLUDED => {
                return format!(
                    "x509: certificate is valid for {} IP SANs, but none matched {host}",
                    self.ips.len()
                );
            }
            Ok(_) => self.ips.iter().map(go_ip).collect::<Vec<_>>().join(", "),
            Err(_) if self.dns.len() >= MAX_NAMES_INCLUDED => {
                return format!(
                    "x509: certificate is valid for {} names, but none matched {host}",
                    self.dns.len()
                );
            }
            Err(_) => self.dns.join(", "),
        };
        match valid.is_empty() {
            true => {
                format!("x509: certificate is not valid for any names, but wanted to match {host}")
            }
            false => format!("x509: certificate is valid for {valid}, not {host}"),
        }
    }
}

/// `net.IP.Equal`: an IPv4 address equals its IPv4-mapped IPv6 form.
fn same_ip(a: &std::net::IpAddr, b: &std::net::IpAddr) -> bool {
    a.to_canonical() == b.to_canonical()
}

/// `net.IP.String`: an IPv4-mapped address as IPv4.
fn go_ip(ip: &std::net::IpAddr) -> String {
    ip.to_canonical().to_string()
}

/// `validHostname`: labels of letters, digits, `_` and inner `-`; a pattern may start with a
/// `*` label, an input may end with a dot.
fn valid_hostname(host: &str, pattern: bool) -> bool {
    let host = match pattern {
        true => host,
        false => host.strip_suffix('.').unwrap_or(host),
    };
    if host.is_empty() || host == "*" {
        return false;
    }
    host.split('.').enumerate().all(|(i, part)| {
        if part.is_empty() {
            return false;
        }
        if pattern && i == 0 && part == "*" {
            return true;
        }
        part.chars()
            .enumerate()
            .all(|(j, c)| c.is_ascii_alphanumeric() || (c == '-' && j != 0) || c == '_')
    })
}

/// `matchExactly`.
fn match_exactly(a: &str, b: &str) -> bool {
    if a.is_empty() || a == "." || b.is_empty() || b == "." {
        return false;
    }
    a.eq_ignore_ascii_case(b)
}

/// `matchHostnames`: label by label, a leading `*` label matching any.
fn match_hostnames(pattern: &str, host: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase();
    let host = host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase();
    if pattern.is_empty() || host.is_empty() {
        return false;
    }
    let (pattern, host): (Vec<&str>, Vec<&str>) =
        (pattern.split('.').collect(), host.split('.').collect());
    pattern.len() == host.len()
        && pattern
            .iter()
            .zip(&host)
            .enumerate()
            .all(|(i, (p, h))| (i == 0 && *p == "*") || p == h)
}

/// The chain verified against the roots, the name not (Go's `InsecureSkipVerify` with its
/// own `VerifyPeerCertificate`).
#[derive(Debug)]
struct NameNotVerified(Option<Arc<rustls::client::WebPkiServerVerifier>>);

impl NameNotVerified {
    fn inner(&self) -> Result<&rustls::client::WebPkiServerVerifier, rustls::Error> {
        self.0.as_deref().ok_or(rustls::Error::InvalidCertificate(
            rustls::CertificateError::UnknownIssuer,
        ))
    }
}

impl rustls::client::danger::ServerCertVerifier for NameNotVerified {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        use rustls::{CertificateError, Error};
        match self.inner()?.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(rustls::client::danger::ServerCertVerified::assertion()),
            verified => verified,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner()?.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner()?.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        match &self.0 {
            Some(verifier) => verifier.supported_verify_schemes(),
            None => rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes(),
        }
    }
}

/// The chain verified against the roots, then the name as `Certificate.VerifyHostname`
/// matches it, a mismatch carrying `HostnameError`'s text.
#[derive(Debug)]
struct GoNames(Option<Arc<rustls::client::WebPkiServerVerifier>>);

/// `HostnameError`'s text, carried in a rustls error.
#[derive(Debug)]
struct HostnameError(String);

impl std::fmt::Display for HostnameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for HostnameError {}

impl rustls::client::danger::ServerCertVerifier for GoNames {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        use rustls::{CertificateError, Error};
        let verifier = NameNotVerified(self.0.clone());
        let verified = verifier.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        let names = Names::of(end_entity)
            .ok_or(Error::InvalidCertificate(CertificateError::BadEncoding))?;
        match names.verify_hostname(&server_name.to_str()) {
            Ok(()) => Ok(verified),
            Err(e) => Err(Error::InvalidCertificate(CertificateError::Other(
                rustls::OtherError(Arc::new(HostnameError(e))),
            ))),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        NameNotVerified(self.0.clone()).verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        NameNotVerified(self.0.clone()).verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        NameNotVerified(self.0.clone()).supported_verify_schemes()
    }
}

/// A rustls failure in Go's words where they are common (an unknown authority, a name the
/// certificate is not for, an expired certificate); rustls' otherwise (docs/parity.md §7).
pub fn error_text(error: &std::io::Error) -> String {
    use rustls::{CertificateError, Error};
    let rustls = error.get_ref().and_then(|e| e.downcast_ref::<Error>());
    let x509 = match rustls {
        Some(Error::InvalidCertificate(CertificateError::UnknownIssuer)) => {
            "certificate signed by unknown authority".to_string()
        }
        // `HostnameError`, from the certificate's names.
        Some(Error::InvalidCertificate(CertificateError::Other(other))) => {
            return format!("tls: failed to verify certificate: {}", other.0);
        }
        Some(Error::InvalidCertificate(CertificateError::NotValidForName)) => {
            "certificate is not valid for the server's name".to_string()
        }
        Some(Error::InvalidCertificate(CertificateError::ExpiredContext { time, not_after })) => {
            format!(
                "certificate has expired or is not yet valid: current time {} is after {}",
                rfc3339(time.as_secs()),
                rfc3339(not_after.as_secs())
            )
        }
        Some(Error::InvalidCertificate(CertificateError::NotValidYetContext {
            time,
            not_before,
        })) => format!(
            "certificate has expired or is not yet valid: current time {} is before {}",
            rfc3339(time.as_secs()),
            rfc3339(not_before.as_secs())
        ),
        Some(Error::InvalidCertificate(
            CertificateError::Expired | CertificateError::NotValidYet,
        )) => "certificate has expired or is not yet valid".to_string(),
        Some(e) => return format!("tls: {e}"),
        None => return error.to_string(),
    };
    format!("tls: failed to verify certificate: x509: {x509}")
}

/// `time.RFC3339` in UTC, as `x509` writes a validity error's times.
fn rfc3339(seconds: u64) -> String {
    jiff::Timestamp::from_second(seconds as i64)
        .map(|t| t.strftime("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A self-signed certificate for `cn` with these SANs (DNS names, IPs, URIs).
    fn names(cn: &str, dns: &[&str], ips: &[&str], uris: &[&str]) -> Names {
        use rcgen::{CertificateParams, DnType, KeyPair, SanType};
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, cn);
        params.subject_alt_names = dns
            .iter()
            .map(|d| SanType::DnsName((*d).try_into().unwrap()))
            .chain(ips.iter().map(|i| SanType::IpAddress(i.parse().unwrap())))
            .chain(uris.iter().map(|u| SanType::URI((*u).try_into().unwrap())))
            .collect();
        let cert = params.self_signed(&KeyPair::generate().unwrap()).unwrap();
        Names::of(cert.der()).unwrap()
    }

    /// `Certificate.VerifyHostname`'s results and `HostnameError`'s texts, as Go 1.26 gives
    /// them for the same certificates (crypto/x509/verify.go).
    #[test]
    fn names_are_verified_like_go() {
        let many: Vec<String> = (0..100).map(|i| format!("n{i}.example")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        let cases: Vec<(Names, &str, Result<(), &str>)> = vec![
            (
                names("x", &["a.example", "b.example"], &["10.0.0.1"], &[]),
                "redis.example",
                Err("x509: certificate is valid for a.example, b.example, not redis.example"),
            ),
            (
                names("x", &[], &["10.0.0.1"], &[]),
                "redis.example",
                Err(
                    "x509: certificate is not valid for any names, but wanted to match redis.example",
                ),
            ),
            (
                names("x", &["a.example"], &["10.0.0.1"], &[]),
                "10.0.0.2",
                Err("x509: certificate is valid for 10.0.0.1, not 10.0.0.2"),
            ),
            (
                names("x", &["a.example"], &[], &[]),
                "10.0.0.2",
                Err(
                    "x509: cannot validate certificate for 10.0.0.2 because it doesn't contain any IP SANs",
                ),
            ),
            (
                names("x", &many, &[], &[]),
                "redis.example",
                Err("x509: certificate is valid for 100 names, but none matched redis.example"),
            ),
            (
                names("x", &[], &[], &["spiffe://cluster.local/ns/a"]),
                "svc",
                Err("x509: certificate is not valid for any names, but wanted to match svc"),
            ),
            (
                names("x", &[], &["2001:db8::1:0:0:1"], &[]),
                "::2",
                Err("x509: certificate is valid for 2001:db8::1:0:0:1, not ::2"),
            ),
            (
                names("svc", &[], &[], &[]),
                "svc",
                Err("x509: certificate relies on legacy Common Name field, use SANs instead"),
            ),
            (names("x", &["*.example"], &[], &[]), "a.EXAMPLE", Ok(())),
            (
                names("x", &["svc.example"], &[], &[]),
                "svc.example.",
                Ok(()),
            ),
            (
                names("x", &["svc.example."], &[], &[]),
                "svc.example",
                Err("x509: certificate is valid for svc.example., not svc.example"),
            ),
            (names("x", &[], &["::1"], &[]), "[::1]", Ok(())),
            (
                names("x", &[], &["10.0.0.1"], &[]),
                "::ffff:10.0.0.1",
                Ok(()),
            ),
            (
                names("x", &["my_svc.example"], &[], &[]),
                "my_svc.example",
                Ok(()),
            ),
            (
                names("x", &["client"], &[], &[]),
                "spiffe://x",
                Err("x509: certificate is valid for client, not spiffe://x"),
            ),
            (
                names("x", &["*.example"], &[], &[]),
                "a.b.example",
                Err("x509: certificate is valid for *.example, not a.b.example"),
            ),
        ];
        for (names, host, expected) in cases {
            assert_eq!(
                names.verify_hostname(host),
                expected.map_err(str::to_string),
                "{host}"
            );
        }
    }
}
