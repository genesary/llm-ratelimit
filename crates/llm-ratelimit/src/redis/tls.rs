//! `RedisTlsConfig` (`TlsConfigFromFiles` with a server CA): loaded with the settings when
//! either pool uses TLS (`REDIS_TLS`, `REDIS_PERSECOND_TLS`). The server is verified against the system's roots plus `REDIS_TLS_CACERT`; `REDIS_TLS_CLIENT_CERT` and
//! `REDIS_TLS_CLIENT_KEY` are the client's certificate; with
//! `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION`, the chain is verified but not the name.

use crate::settings::Settings;
use std::sync::Arc;

/// The client configuration both pools share, if either uses TLS; an error is what upstream
/// panics with.
pub fn from_settings(s: &Settings) -> Result<Option<Arc<rustls::ClientConfig>>, String> {
    if !(s.redis_tls || s.redis_persecond_tls) {
        return Ok(None);
    }
    crate::tls::server_ca_client(
        &s.redis_tls_client_cert,
        &s.redis_tls_client_key,
        &s.redis_tls_cacert,
        s.redis_tls_skip_hostname_verification,
    )
    .map(|config| Some(Arc::new(config)))
}
