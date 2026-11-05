//! The memcached client upstream uses, `github.com/bradfitz/gomemcache/memcache`
//! (v0.0.0-20230905024940): a key's server picked by the CRC-32 of the key, idle connections
//! kept per server, each operation within 500 ms of taking its connection, and Go's errors.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::time::Instant;

/// `DefaultTimeout`: a connection's operation, from taking it (or dialling it).
const TIMEOUT: Duration = Duration::from_millis(500);

/// A failed operation, as gomemcache's errors read.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// `ErrCacheMiss`.
    CacheMiss,
    /// `ErrNotStored`.
    NotStored,
    /// `ErrMalformedKey`: over 250 bytes, or with a space or a control character.
    MalformedKey,
    /// `ErrNoServers`.
    NoServers,
    /// `ErrCASConflict` (`EXISTS`).
    CasConflict,
    /// Any other failure, in Go's words.
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Error::CacheMiss => "memcache: cache miss",
            Error::NotStored => "memcache: item not stored",
            Error::MalformedKey => "malformed: key is too long or contains invalid characters",
            Error::NoServers => "memcache: no servers configured or available",
            Error::CasConflict => "memcache: compare-and-swap conflict",
            Error::Other(message) => message,
        })
    }
}

impl Error {
    /// `resumableError`: the connection is kept after it.
    fn resumable(&self) -> bool {
        matches!(
            self,
            Error::CacheMiss | Error::NotStored | Error::CasConflict | Error::MalformedKey
        )
    }
}

/// An item `GetMulti` found: what `%v` of upstream's `*memcache.Item` shows.
#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub key: String,
    pub value: Vec<u8>,
    pub flags: u32,
    pub cas: u64,
}

impl Item {
    /// `%v` of the `*memcache.Item`: `&{key [49 50] 0 0 7}` (key, value bytes, flags,
    /// expiration, CAS ID).
    pub fn go_format(&self) -> String {
        let bytes: Vec<String> = self.value.iter().map(u8::to_string).collect();
        format!(
            "&{{{} [{}] {} 0 {}}}",
            self.key,
            bytes.join(" "),
            self.flags,
            self.cas
        )
    }
}

/// A server, resolved as `ServerList.SetServers` resolves it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Address {
    Tcp(SocketAddr),
    Unix(String),
}

impl Address {
    fn network(&self) -> &'static str {
        match self {
            Address::Tcp(_) => "tcp",
            Address::Unix(_) => "unix",
        }
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Address::Tcp(address) => write!(f, "{address}"),
            Address::Unix(path) => f.write_str(path),
        }
    }
}

/// `ServerList.SetServers`: each server resolved (one with a `/` is a Unix socket); `None` when
/// one does not resolve.
pub async fn resolve(servers: &[String]) -> Option<Vec<Address>> {
    let mut addresses = Vec::new();
    for server in servers {
        if server.contains('/') {
            addresses.push(Address::Unix(server.clone()));
            continue;
        }
        let (host, port) = crate::go::split_host_port(server).ok()?;
        let port: u16 = port.parse().ok()?;
        let resolved: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
            .await
            .ok()?
            .collect();
        // `ResolveTCPAddr("tcp", …)` takes the first IPv4 address, else the first.
        let address = resolved.iter().find(|a| a.is_ipv4()).or(resolved.first())?;
        addresses.push(Address::Tcp(*address));
    }
    Some(addresses)
}

/// A connection to a server: TCP or Unix, over TLS or not.
type Stream = Box<dyn Io>;
trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Io for T {}

/// A connection, and how Go names it in its errors.
struct Conn {
    stream: BufStream<Stream>,
    /// `tcp 127.0.0.1:50000->127.0.0.1:11211`.
    name: String,
}

/// The client: its servers, and its idle connections.
pub struct Client {
    servers: RwLock<Vec<Address>>,
    /// `MaxIdleConns` (2 for less than one).
    max_idle: usize,
    /// `MEMCACHE_TLS`: the dialer's TLS.
    tls: Option<Arc<rustls::ClientConfig>>,
    free: Mutex<HashMap<Address, Vec<Conn>>>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("servers", &self.servers.read().unwrap())
            .finish_non_exhaustive()
    }
}

/// An operation's failure on its connection, in Go's words.
struct Failed(Error);

impl Client {
    pub fn new(
        servers: Vec<Address>,
        max_idle: i64,
        tls: Option<Arc<rustls::ClientConfig>>,
    ) -> Self {
        Self {
            servers: RwLock::new(servers),
            max_idle: usize::try_from(max_idle)
                .ok()
                .filter(|n| *n > 0)
                .unwrap_or(2),
            tls,
            free: Mutex::new(HashMap::new()),
        }
    }

    /// The servers, in their order.
    pub fn servers(&self) -> Vec<Address> {
        self.servers.read().unwrap().clone()
    }

    /// `SetServers`, once resolved.
    pub fn set_servers(&self, servers: Vec<Address>) {
        *self.servers.write().unwrap() = servers;
    }

    /// `PickServer`: the CRC-32 (IEEE) of the key's first 256 bytes picks among the servers;
    /// the server, and its place in the list (a server listed twice is two).
    fn pick(&self, key: &str) -> Result<(usize, Address), Error> {
        let servers = self.servers.read().unwrap();
        let at = match servers.len() {
            0 => return Err(Error::NoServers),
            1 => 0,
            n => {
                let bytes = &key.as_bytes()[..key.len().min(256)];
                (crc32fast::hash(bytes) % n as u32) as usize
            }
        };
        Ok((at, servers[at].clone()))
    }

    /// `GetMulti`: each server's keys in one `gets`, the servers in parallel; the last error
    /// any of them returns, with what the others found.
    pub async fn get_multi(&self, keys: &[String]) -> (HashMap<String, Item>, Option<Error>) {
        // Grouped by the server's place in the list, as Go's map of its `net.Addr`s.
        let mut by_server: Vec<(usize, Address, Vec<String>)> = Vec::new();
        for key in keys {
            if !legal_key(key) {
                return (HashMap::new(), Some(Error::MalformedKey));
            }
            let (at, address) = match self.pick(key) {
                Ok(picked) => picked,
                Err(e) => return (HashMap::new(), Some(e)),
            };
            match by_server.iter_mut().find(|(place, _, _)| *place == at) {
                Some((_, _, keys)) => keys.push(key.clone()),
                None => by_server.push((at, address, vec![key.clone()])),
            }
        }
        let gets = by_server
            .iter()
            .map(|(_, address, keys)| self.get_from(address, keys));
        let mut items = HashMap::new();
        let mut error = None;
        for (found, failed) in futures_util::future::join_all(gets).await {
            items.extend(found);
            if let Some(e) = failed {
                error = Some(e);
            }
        }
        (items, error)
    }

    async fn get_from(
        &self,
        address: &Address,
        keys: &[String],
    ) -> (Vec<(String, Item)>, Option<Error>) {
        let mut found = Vec::new();
        let result = self
            .with_conn(address, async |conn, deadline| {
                let command = format!("gets {}\r\n", keys.join(" "));
                write(conn, deadline, command.as_bytes()).await?;
                loop {
                    let line = read_line(conn, deadline).await?;
                    if line == b"END\r\n" {
                        return Ok(());
                    }
                    let (key, flags, size, cas) = get_response_line(&line)?;
                    let mut value = vec![0; size + 2];
                    read_exact(conn, deadline, &mut value).await?;
                    if !value.ends_with(b"\r\n") {
                        return Err(Failed(Error::Other(
                            "memcache: corrupt get result read".into(),
                        )));
                    }
                    value.truncate(size);
                    found.push((
                        key.clone(),
                        Item {
                            key,
                            value,
                            flags,
                            cas,
                        },
                    ));
                }
            })
            .await;
        (found, result.err())
    }

    /// `Increment`: the new value, or `CacheMiss` for a key not there.
    pub async fn increment(&self, key: &str, delta: u64) -> Result<u64, Error> {
        self.incr_decr("incr", key, delta).await
    }

    /// `Decrement`: floored at 0 by memcached.
    pub async fn decrement(&self, key: &str, delta: u64) -> Result<u64, Error> {
        self.incr_decr("decr", key, delta).await
    }

    async fn incr_decr(&self, verb: &str, key: &str, delta: u64) -> Result<u64, Error> {
        if !legal_key(key) {
            return Err(Error::MalformedKey);
        }
        let (_, address) = self.pick(key)?;
        self.with_conn(&address, async |conn, deadline| {
            write(
                conn,
                deadline,
                format!("{verb} {key} {delta}\r\n").as_bytes(),
            )
            .await?;
            let line = read_line(conn, deadline).await?;
            if line == b"NOT_FOUND\r\n" {
                return Err(Failed(Error::CacheMiss));
            }
            let text = String::from_utf8_lossy(&line[..line.len().saturating_sub(2)]).into_owned();
            if let Some(message) = line.strip_prefix(b"CLIENT_ERROR ") {
                let message = String::from_utf8_lossy(&message[..message.len().saturating_sub(2)]);
                return Err(Failed(Error::Other(format!(
                    "memcache: client error: {message}"
                ))));
            }
            crate::go::parse_uint10(&text).map_err(|e| Failed(Error::Other(e)))
        })
        .await
    }

    /// `Add`: `NotStored` when the key is there. The key is checked once connected, as
    /// `populateOne` checks it.
    pub async fn add(&self, key: &str, value: &[u8], expiration: i32) -> Result<(), Error> {
        let (_, address) = self.pick(key)?;
        self.with_conn(&address, async |conn, deadline| {
            if !legal_key(key) {
                return Err(Failed(Error::MalformedKey));
            }
            let mut command = format!("add {key} 0 {expiration} {}\r\n", value.len()).into_bytes();
            command.extend_from_slice(value);
            command.extend_from_slice(b"\r\n");
            write(conn, deadline, &command).await?;
            let line = read_line(conn, deadline).await?;
            match line.as_slice() {
                b"STORED\r\n" => Ok(()),
                b"NOT_STORED\r\n" => Err(Failed(Error::NotStored)),
                b"EXISTS\r\n" => Err(Failed(Error::CasConflict)),
                b"NOT_FOUND\r\n" => Err(Failed(Error::CacheMiss)),
                _ => Err(Failed(Error::Other(format!(
                    "memcache: unexpected response line from \"add\": {}",
                    crate::go::quote(&String::from_utf8_lossy(&line))
                )))),
            }
        })
        .await
    }

    /// An operation on a connection to `address` (an idle one, or a new one): kept after it
    /// unless it failed otherwise than `resumableError` says (`condRelease`).
    async fn with_conn<T>(
        &self,
        address: &Address,
        operation: impl AsyncFnOnce(&mut Conn, Instant) -> Result<T, Failed>,
    ) -> Result<T, Error> {
        let mut conn = match self.idle(address) {
            Some(conn) => conn,
            None => self.dial(address).await?,
        };
        let deadline = Instant::now() + TIMEOUT;
        match operation(&mut conn, deadline).await {
            Ok(value) => {
                self.release(address, conn);
                Ok(value)
            }
            Err(Failed(e)) => {
                if e.resumable() {
                    self.release(address, conn);
                }
                Err(e)
            }
        }
    }

    fn idle(&self, address: &Address) -> Option<Conn> {
        self.free.lock().unwrap().get_mut(address)?.pop()
    }

    /// `putFreeConn`: kept unless `MaxIdleConns` are.
    fn release(&self, address: &Address, conn: Conn) {
        let mut free = self.free.lock().unwrap();
        let idle = free.entry(address.clone()).or_default();
        if idle.len() < self.max_idle {
            idle.push(conn);
        }
    }

    /// `dial`: within the timeout, the TLS handshake included; a timeout is
    /// `ConnectTimeoutError`.
    async fn dial(&self, address: &Address) -> Result<Conn, Error> {
        let timed_out = || Error::Other(format!("memcache: connect timeout to {address}"));
        let deadline = Instant::now() + TIMEOUT;
        let connect = async {
            let (stream, name): (Stream, String) = match address {
                Address::Tcp(peer) => {
                    let stream = tokio::net::TcpStream::connect(peer)
                        .await
                        .map_err(|e| dial_error(address, &e))?;
                    crate::net::nodelay(&stream);
                    let local = stream
                        .local_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_default();
                    (Box::new(stream), format!("tcp {local}->{peer}"))
                }
                Address::Unix(path) => {
                    let stream = tokio::net::UnixStream::connect(path)
                        .await
                        .map_err(|e| dial_error(address, &e))?;
                    (Box::new(stream), format!("unix ->{path}"))
                }
            };
            let stream = match (&self.tls, address) {
                (None, _) => stream,
                (Some(config), address) => {
                    // Go's `tls.Dialer` verifies the address' host: here an IP, or a socket's
                    // path, which rustls cannot name: with the hostname not verified, a name
                    // of its own does (docs/parity.md §7).
                    let name = match address {
                        Address::Tcp(peer) => rustls::pki_types::ServerName::from(peer.ip()),
                        Address::Unix(path) => rustls::pki_types::ServerName::try_from(
                            path.clone(),
                        )
                        .unwrap_or_else(|_| {
                            rustls::pki_types::ServerName::try_from("memcached.invalid").unwrap()
                        }),
                    };
                    let connector = tokio_rustls::TlsConnector::from(config.clone());
                    let tls = connector
                        .connect(name, stream)
                        .await
                        .map_err(|e| Error::Other(crate::tls::error_text(&e)))?;
                    Box::new(tls) as Stream
                }
            };
            Ok::<_, Error>(Conn {
                stream: BufStream::new(stream),
                name,
            })
        };
        tokio::time::timeout_at(deadline, connect)
            .await
            .map_err(|_| timed_out())?
    }
}

/// `dial <network> <address>: connect: <errno>`, as Go's dialer fails.
fn dial_error(address: &Address, error: &std::io::Error) -> Error {
    let network = address.network();
    Error::Other(match error.raw_os_error() {
        Some(_) => format!(
            "dial {network} {address}: connect: {}",
            crate::go::errno_text(error)
        ),
        None => format!("dial {network} {address}: {error}"),
    })
}

/// A read or write on `conn` past its deadline, or failing: Go's `net.OpError`, or `EOF`.
fn io_error(op: &str, conn: &Conn, error: Option<std::io::Error>) -> Failed {
    Failed(Error::Other(match error {
        None => format!("{op} {}: i/o timeout", conn.name),
        Some(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => "EOF".into(),
        Some(e) if e.raw_os_error().is_some() => {
            format!("{op} {}: {op}: {}", conn.name, crate::go::errno_text(&e))
        }
        Some(e) => format!("{op} {}: {e}", conn.name),
    }))
}

async fn write(conn: &mut Conn, deadline: Instant, bytes: &[u8]) -> Result<(), Failed> {
    let written = async {
        conn.stream.write_all(bytes).await?;
        conn.stream.flush().await
    };
    match tokio::time::timeout_at(deadline, written).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(io_error("write", conn, Some(e))),
        Err(_) => Err(io_error("write", conn, None)),
    }
}

/// `ReadSlice('\n')`: a line, its `\r\n` included; `EOF` if the server closed first.
async fn read_line(conn: &mut Conn, deadline: Instant) -> Result<Vec<u8>, Failed> {
    let mut line = Vec::new();
    match tokio::time::timeout_at(deadline, conn.stream.read_until(b'\n', &mut line)).await {
        Ok(Ok(_)) if line.ends_with(b"\n") => Ok(line),
        Ok(Ok(_)) => Err(Failed(Error::Other("EOF".into()))),
        Ok(Err(e)) => Err(io_error("read", conn, Some(e))),
        Err(_) => Err(io_error("read", conn, None)),
    }
}

/// `io.ReadFull`: `unexpected EOF` when the server closed partway, `EOF` before any byte.
async fn read_exact(conn: &mut Conn, deadline: Instant, buffer: &mut [u8]) -> Result<(), Failed> {
    let mut read = 0;
    while read < buffer.len() {
        match tokio::time::timeout_at(deadline, conn.stream.read(&mut buffer[read..])).await {
            Ok(Ok(0)) => {
                let eof = if read == 0 { "EOF" } else { "unexpected EOF" };
                return Err(Failed(Error::Other(eof.into())));
            }
            Ok(Ok(n)) => read += n,
            Ok(Err(e)) => return Err(io_error("read", conn, Some(e))),
            Err(_) => return Err(io_error("read", conn, None)),
        }
    }
    Ok(())
}

/// `scanGetResponseLine`: `VALUE <key> <flags> <bytes> [<cas>]`.
fn get_response_line(line: &[u8]) -> Result<(String, u32, usize, u64), Failed> {
    let unexpected = || {
        Failed(Error::Other(format!(
            "memcache: unexpected line in get response: {}",
            crate::go::quote(&String::from_utf8_lossy(line))
        )))
    };
    let text = std::str::from_utf8(line).map_err(|_| unexpected())?;
    let body = text.strip_suffix("\r\n").ok_or_else(unexpected)?;
    let fields: Vec<&str> = body.split(' ').collect();
    let (key, flags, size, cas) = match fields[..] {
        ["VALUE", key, flags, size] => (key, flags, size, "0"),
        ["VALUE", key, flags, size, cas] => (key, flags, size, cas),
        _ => return Err(unexpected()),
    };
    match (flags.parse(), size.parse(), cas.parse()) {
        (Ok(flags), Ok(size), Ok(cas)) => Ok((key.to_string(), flags, size, cas)),
        _ => Err(unexpected()),
    }
}

/// `legalKey`: at most 250 bytes, none a space, a control character or DEL.
fn legal_key(key: &str) -> bool {
    key.len() <= 250 && key.bytes().all(|b| b > b' ' && b != 0x7f)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// gomemcache's `PickServer` over three servers, for keys whose CRC-32 is known.
    #[test]
    fn keys_pick_servers_by_their_crc32() {
        let servers: Vec<Address> = (1..=3)
            .map(|i| Address::Tcp(format!("127.0.0.{i}:11211").parse().unwrap()))
            .collect();
        let client = Client::new(servers.clone(), 0, None);
        // crc32("a") = 0xe8b7be43, % 3 = 0; crc32("b") = 0x71beeff9, % 3 = 2.
        assert_eq!(client.pick("a").unwrap(), (0, servers[0].clone()));
        assert_eq!(client.pick("b").unwrap(), (2, servers[2].clone()));
        // Past 256 bytes, the key's end does not count.
        let long = "x".repeat(256);
        assert_eq!(
            client.pick(&format!("{long}1")).unwrap(),
            client.pick(&format!("{long}2")).unwrap()
        );
    }

    #[test]
    fn keys_are_legal_as_gomemcache_says() {
        assert!(legal_key(&"k".repeat(250)));
        assert!(!legal_key(&"k".repeat(251)));
        assert!(!legal_key("a b"));
        assert!(!legal_key("a\x7f"));
        assert!(legal_key("é"));
    }
}
