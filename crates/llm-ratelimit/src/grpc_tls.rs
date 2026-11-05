//! The gRPC server over TLS (`GRPC_SERVER_USE_TLS`): `GrpcServerTlsConfig`
//! (`src/settings/settings.go`), loaded with the settings; the `CertProvider`
//! (`src/provider/cert_provider.go`), which serves the key pair of `GRPC_SERVER_TLS_CERT` and
//! `GRPC_SERVER_TLS_KEY` and reloads it when their directory changes; and `verifyClient`
//! (`src/server/tls.go`). With `GRPC_CLIENT_TLS_CACERT`, clients must present a certificate
//! that CA issued (upstream also takes the system's roots: docs/parity.md §7); with
//! `GRPC_CLIENT_TLS_SAN` too, one valid for that name. As grpc-go's credentials, connections
//! must negotiate `h2`, unless `GRPC_ENFORCE_ALPN_ENABLED` is `false`.

use crate::log::{self, Failure};
use crate::runtime::{FsOp, Loader, Refresher};
use crate::settings::Settings;
use crate::stats::Stats;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::sign::CertifiedKey;
use std::sync::{Arc, RwLock};

/// What `GrpcServerTlsConfig` reads with the settings: the clients' verifier, if any.
pub struct ServerTls {
    cert: String,
    key: String,
    clients: Option<Arc<dyn ClientCertVerifier>>,
}

/// `GrpcServerTlsConfig`, if `GRPC_SERVER_USE_TLS`; an error is what upstream panics with.
pub fn from_settings(s: &Settings) -> Result<Option<ServerTls>, String> {
    if !s.grpc_server_use_tls {
        return Ok(None);
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let (cert, key) = (
        s.grpc_server_tls_cert.as_str(),
        s.grpc_server_tls_key.as_str(),
    );
    // A key pair needs both files; the cert provider loads it again.
    if !cert.is_empty() && !key.is_empty() {
        crate::tls::key_pair(cert, key, &provider)
            .map_err(|e| format!("failed lo load TLS key pair ({cert},{key}): {e}"))?;
    }
    let ca = s.grpc_client_tls_cacert.as_str();
    let clients = match ca.is_empty() {
        true => None,
        false => {
            // Not the system's roots: any public CA would vouch for any client.
            let roots = Arc::new(crate::tls::ca_only(ca)?);
            let verifier =
                rustls::server::WebPkiClientVerifier::builder_with_provider(roots, provider)
                    .build()
                    .map_err(|e| e.to_string())?;
            Some(match s.grpc_client_tls_san.as_str() {
                "" => verifier,
                san => Arc::new(NamedClient {
                    inner: verifier,
                    san: san.to_string(),
                }) as Arc<dyn ClientCertVerifier>,
            })
        }
    };
    Ok(Some(ServerTls {
        cert: cert.to_string(),
        key: key.to_string(),
        clients,
    }))
}

impl ServerTls {
    /// `NewServer`: the cert provider, then the TLS configuration serving its key pair.
    /// Without `enforce_alpn` (grpc-go's `EnforceALPNEnabled`), clients that negotiate no
    /// protocol are served too.
    pub fn acceptor(self, stats: &Stats, enforce_alpn: bool) -> Result<Acceptor, Failure> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let certs = CertProvider::start(stats, &self.cert, &self.key, &provider)?;
        let builder = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?;
        let builder = match self.clients {
            Some(verifier) => builder.with_client_cert_verifier(verifier),
            None => builder.with_no_client_auth(),
        };
        let mut config = builder.with_cert_resolver(certs.current.clone());
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Acceptor {
            tls: tokio_rustls::TlsAcceptor::from(Arc::new(config)),
            enforce_alpn,
            _certs: certs,
        })
    }
}

/// The key pair served, swapped on each reload.
#[derive(Debug)]
struct Current(RwLock<Option<Arc<CertifiedKey>>>);

impl rustls::server::ResolvesServerCert for Current {
    fn resolve(&self, _: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.0.read().unwrap().clone()
    }
}

/// `CertProvider`: goruntime's loader on the key pair's directory (its `certs.*` stats), and
/// the key pair reloaded after each refresh. Like upstream's, it runs for the life of the
/// process.
struct CertProvider {
    current: Arc<Current>,
    _loader: Loader,
}

impl CertProvider {
    /// `NewCertProvider`: both files in one directory, its loader, then the first load.
    fn start(
        stats: &Stats,
        cert: &str,
        key: &str,
        provider: &Arc<rustls::crypto::CryptoProvider>,
    ) -> Result<Self, Failure> {
        let directory = crate::go::dir(cert);
        if directory != crate::go::dir(key) {
            return Err(Failure::Fatal(
                "certFile and keyFile must be in the same directory".into(),
            ));
        }
        let refresher = Refresher::Directory {
            current_dir: String::new(),
            ops: vec![FsOp::Write, FsOp::Create, FsOp::Chmod],
        };
        let loader = Loader::new(
            &crate::go::dir(&directory),
            &crate::go::base(&directory),
            stats,
            &stats.scope_with_tags("certs"),
            refresher,
            true,
        )
        .map_err(|e| Failure::Fatal(format!("Failed to set up goruntime loader: {e}")))?;
        let current = Arc::new(Current(RwLock::new(None)));
        let reload = {
            let (current, provider) = (current.clone(), provider.clone());
            let (cert, key) = (cert.to_string(), key.to_string());
            move || match certified_key(&cert, &key, &provider) {
                Ok(certified) => {
                    *current.0.write().unwrap() = Some(certified);
                    log::info(&format!("CertProvider reloaded cert from ({cert}, {key})"));
                    true
                }
                Err(e) => {
                    log::error(&format!(
                        "CertProvider failed to load TLS key pair ({cert}, {key}): {e}"
                    ));
                    // Upstream exits rather than serve without a certificate.
                    current.0.read().unwrap().is_some()
                }
            }
        };
        if !reload() {
            return Err(Failure::Fatal(
                "CertProvider failed to load any certificate, exiting.".into(),
            ));
        }
        let watched = loader.clone();
        std::thread::spawn(move || {
            let (callback, refreshed) = std::sync::mpsc::sync_channel::<()>(0);
            watched.add_update_callback(callback);
            loop {
                crate::debug!("CertProvider: waiting for runtime update");
                if refreshed.recv().is_err() {
                    break;
                }
                crate::debug!("CertProvider: got runtime update and reloading config");
                reload();
            }
        });
        Ok(Self {
            current,
            _loader: loader,
        })
    }
}

/// `tls.LoadX509KeyPair`'s pair, ready to serve.
fn certified_key(
    cert: &str,
    key: &str,
    provider: &rustls::crypto::CryptoProvider,
) -> Result<Arc<CertifiedKey>, String> {
    let (certs, key) = crate::tls::key_pair(cert, key, provider)?;
    let signing = provider
        .key_provider
        .load_private_key(key)
        .map_err(|e| format!("tls: {e}"))?;
    Ok(Arc::new(CertifiedKey::new(certs, signing)))
}

/// `verifyClient`: the client's chain verified, then its name, as `x509.Certificate.Verify`
/// with `DNSName`; a name that does not match is logged.
#[derive(Debug)]
struct NamedClient {
    inner: Arc<dyn ClientCertVerifier>,
    san: String,
}

impl ClientCertVerifier for NamedClient {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        let names = crate::tls::Names::of(end_entity).ok_or(rustls::Error::InvalidCertificate(
            rustls::CertificateError::BadEncoding,
        ))?;
        match names.verify_hostname(&self.san) {
            Ok(()) => Ok(verified),
            Err(e) => {
                log::warn(&format!("error validating client: {e}"));
                Err(rustls::Error::InvalidCertificate(
                    rustls::CertificateError::NotValidForName,
                ))
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// The TLS side of the gRPC listener.
pub struct Acceptor {
    tls: tokio_rustls::TlsAcceptor,
    enforce_alpn: bool,
    _certs: CertProvider,
}

/// grpc-go's handshake deadline (`connectionTimeout`).
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

impl Acceptor {
    /// The connections of `accepted` once through their handshake (each its own, so that a
    /// slow one holds back no other), without those that fail it or do not negotiate `h2`.
    pub fn incoming<S, A>(
        &self,
        mut accepted: A,
    ) -> tokio_stream::wrappers::ReceiverStream<std::io::Result<TlsIo<Replayed<S>>>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        A: tokio_stream::Stream<Item = std::io::Result<S>> + Unpin + Send + 'static,
    {
        use tokio_stream::StreamExt;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let (tls, enforce_alpn) = (self.tls.clone(), self.enforce_alpn);
        tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    () = tx.closed() => return,
                    next = accepted.next() => next,
                };
                let stream = match next {
                    Some(Ok(stream)) => stream,
                    Some(Err(e)) => {
                        let _ = tx.send(Err(e)).await;
                        continue;
                    }
                    None => return,
                };
                let (tls, tx) = (tls.clone(), tx.clone());
                tokio::spawn(async move {
                    let handshake = async {
                        let stream = first_record_is_a_handshake(stream).await?;
                        tls.accept(stream).await.ok()
                    };
                    let Ok(Some(stream)) = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake).await
                    else {
                        crate::debug!("gRPC connection closed: TLS handshake failed");
                        return;
                    };
                    // grpc-go closes the connection, so with a close_notify.
                    if enforce_alpn && stream.get_ref().1.alpn_protocol().is_none() {
                        let mut stream = stream;
                        let _ = tokio::io::AsyncWriteExt::shutdown(&mut stream).await;
                        return;
                    }
                    let _ = tx.send(Ok(TlsIo(stream))).await;
                });
            }
        });
        tokio_stream::wrappers::ReceiverStream::new(rx)
    }
}

/// `stream` with its first bytes read again, if they start a handshake record: crypto/tls
/// closes a connection whose first record is no handshake without an alert, having read what
/// was sent (its first read takes up to 512 bytes).
async fn first_record_is_a_handshake<S>(mut stream: S) -> Option<Replayed<S>>
where
    S: tokio::io::AsyncRead + Unpin,
{
    const HANDSHAKE: u8 = 0x16;
    let mut first = vec![0; 512];
    let read = tokio::io::AsyncReadExt::read(&mut stream, &mut first)
        .await
        .ok()?;
    first.truncate(read);
    (first.first() == Some(&HANDSHAKE)).then_some(Replayed { first, stream })
}

/// A connection whose first bytes, already read, are read again.
pub struct Replayed<S> {
    first: Vec<u8>,
    stream: S,
}

impl<S> tonic::transport::server::Connected for Replayed<S>
where
    S: tonic::transport::server::Connected,
{
    type ConnectInfo = S::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.stream.connect_info()
    }
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Replayed<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if !self.first.is_empty() {
            let n = self.first.len().min(buf.remaining());
            buf.put_slice(&self.first[..n]);
            self.first.drain(..n);
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Replayed<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}

/// A connection over TLS, known to the server by its transport's address.
pub struct TlsIo<S>(tokio_rustls::server::TlsStream<S>);

impl<S> tonic::transport::server::Connected for TlsIo<S>
where
    S: tonic::transport::server::Connected,
{
    type ConnectInfo = S::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.0.get_ref().0.connect_info()
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> tokio::io::AsyncRead for TlsIo<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for TlsIo<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }
}
