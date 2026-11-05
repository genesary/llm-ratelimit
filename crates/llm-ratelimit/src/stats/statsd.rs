//! gostats' statsd sink (`net_sink.go`): `name:value|c` lines, gathered into buffers of whole
//! lines (64 KiB over TCP, 1432 bytes over UDP, one datagram each), queued up to about 4 MiB
//! and written by a background task that reconnects every 3 seconds. A buffer that does not
//! fit in the queue is dropped.

use super::LoggingSink;
use super::sink::StatsdProtocol;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{mpsc, oneshot};

const RETRY_INTERVAL: Duration = Duration::from_secs(3);
const DIAL_TIMEOUT: Duration = Duration::from_millis(1500);
const WRITE_TIMEOUT: Duration = Duration::from_secs(1);
const LOG_ON_EVERY_N_DROPPED_BYTES: u64 = 1 << 15;
const BUFFER_SIZE_TCP: usize = 1 << 16;
const BUFFER_SIZE_UDP: usize = 1432;
const APPROX_MAX_MEM_BYTES: usize = 1 << 22;
const FLUSH_REQUESTS: usize = 8;

pub struct StatsdSink {
    pending: Arc<Pending>,
    buffer_size: usize,
    queue: mpsc::Sender<Vec<u8>>,
    flushes: mpsc::Sender<oneshot::Sender<()>>,
}

/// What the sink and its writer share: the lines not yet queued, and the bytes dropped so far.
struct Pending {
    lines: Mutex<Vec<u8>>,
    dropped: Mutex<u64>,
    log: Arc<LoggingSink>,
}

impl Pending {
    /// gostats' `handleFlushErrorSize`: counts `bytes` as dropped, logging when the total does
    /// not cross a 32 KiB boundary (sic), and discards the lines not yet queued.
    fn drop_bytes(&self, lines: &mut Vec<u8>, bytes: usize, reason: &str) {
        let mut dropped = self.dropped.lock().unwrap();
        let total = *dropped + bytes as u64;
        if total % LOG_ON_EVERY_N_DROPPED_BYTES > *dropped % LOG_ON_EVERY_N_DROPPED_BYTES {
            self.log.error(&format!("dropped {total} bytes: {reason}"));
        }
        *dropped = total;
        lines.clear();
    }
}

impl StatsdSink {
    /// Connects to `host:port` in the background; logs to stderr.
    pub fn new(host: &str, port: u16, protocol: StatsdProtocol) -> Self {
        Self::with_log(host, port, protocol, Arc::new(LoggingSink::stderr()))
    }

    pub fn with_log(
        host: &str,
        port: u16,
        protocol: StatsdProtocol,
        log: Arc<LoggingSink>,
    ) -> Self {
        let buffer_size = match protocol {
            StatsdProtocol::Tcp => BUFFER_SIZE_TCP,
            StatsdProtocol::Udp => BUFFER_SIZE_UDP,
        };
        let (queue, queued) = mpsc::channel(APPROX_MAX_MEM_BYTES / buffer_size);
        let (flushes, flush_requests) = mpsc::channel(FLUSH_REQUESTS);
        let pending = Arc::new(Pending {
            lines: Mutex::default(),
            dropped: Mutex::default(),
            log: log.clone(),
        });
        let writer = Writer {
            host: host.to_string(),
            port,
            protocol,
            connection: None,
            queued,
            flush_requests,
            pending: pending.clone(),
            log,
        };
        tokio::spawn(writer.run());
        Self {
            pending,
            buffer_size,
            queue,
            flushes,
        }
    }

    pub fn flush_counter(&self, name: &str, value: u64) {
        self.write_line(format!("{name}:{value}|c\n").as_bytes());
    }

    pub fn flush_gauge(&self, name: &str, value: u64) {
        self.write_line(format!("{name}:{value}|g\n").as_bytes());
    }

    /// A whole value as an integer, any other with six decimals.
    pub fn flush_timer(&self, name: &str, value: f64) {
        let line = match (0.0..u64::MAX as f64).contains(&value) && value.trunc() == value {
            true => format!("{name}:{}|ms\n", value as u64),
            false => format!("{name}:{}|ms\n", crate::go::format_fixed(value, 6)),
        };
        self.write_line(line.as_bytes());
    }

    /// Adds `line` to the buffer, queueing the buffer first when the line does not fit. A line
    /// longer than a buffer is queued on its own.
    fn write_line(&self, line: &[u8]) {
        let mut pending = self.pending.lines.lock().unwrap();
        if !pending.is_empty() && pending.len() + line.len() > self.buffer_size {
            let lines = std::mem::take(&mut *pending);
            let bytes = lines.len();
            self.enqueue(&mut pending, lines, bytes);
        }
        if pending.is_empty() && line.len() > self.buffer_size {
            // gostats hands such a line straight to the queue; when it is full, the line goes
            // without being counted as dropped.
            self.enqueue(&mut pending, line.to_vec(), 0);
        } else {
            pending.extend_from_slice(line);
        }
    }

    /// Queues `lines` for the writer, or, when the queue is full, drops them, counting
    /// `counted` bytes.
    fn enqueue(&self, pending: &mut Vec<u8>, lines: Vec<u8>, counted: usize) -> bool {
        let Err(error) = self.queue.try_send(lines) else {
            return true;
        };
        let (mpsc::error::TrySendError::Full(lines) | mpsc::error::TrySendError::Closed(lines)) =
            error;
        let reason = format!(
            "statsd channel full, dropping stats buffer with {} bytes",
            lines.len()
        );
        self.pending.drop_bytes(pending, counted, &reason);
        false
    }

    /// Queues the buffer and waits until the writer has sent everything queued, or has given
    /// up connecting. Returns at once when the buffer could not be queued.
    pub async fn flush(&self) {
        {
            let mut pending = self.pending.lines.lock().unwrap();
            if !pending.is_empty() {
                let lines = std::mem::take(&mut *pending);
                let bytes = lines.len();
                if !self.enqueue(&mut pending, lines, bytes) {
                    return;
                }
            }
        }
        let (done, sent) = oneshot::channel();
        if self.flushes.send(done).await.is_ok() {
            let _ = sent.await;
        }
    }
}

enum Connection {
    Tcp(TcpStream),
    Udp(UdpSocket),
}

/// The background task that owns the connection.
struct Writer {
    host: String,
    port: u16,
    protocol: StatsdProtocol,
    connection: Option<Connection>,
    queued: mpsc::Receiver<Vec<u8>>,
    flush_requests: mpsc::Receiver<oneshot::Sender<()>>,
    pending: Arc<Pending>,
    log: Arc<LoggingSink>,
}

impl Writer {
    async fn run(mut self) {
        let mut reconnect_failed = false;
        let mut retry: Option<Vec<u8>> = None;
        loop {
            if self.connection.is_none() {
                match self.connect().await {
                    Ok(connection) => {
                        self.connection = Some(connection);
                        reconnect_failed = false;
                    }
                    Err(error) => {
                        self.log.warn(&format!("connection error: {error}"));
                        // A second failure in a row releases the waiting flushes.
                        if reconnect_failed {
                            self.release_flushes();
                        }
                        reconnect_failed = true;
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                }
            }
            if let Some(lines) = retry.take() {
                if let Err((error, unsent)) = self.write(lines).await {
                    let mut pending = self.pending.lines.lock().unwrap();
                    self.pending.drop_bytes(&mut pending, unsent.len(), &error);
                }
                continue;
            }
            tokio::select! {
                request = self.flush_requests.recv() => {
                    let Some(done) = request else { return };
                    // Only what is queued now, so steady writes cannot hold the flush.
                    for _ in 0..self.queued.len() {
                        if self.connection.is_none() {
                            break;
                        }
                        let Some(lines) = self.queued.recv().await else { break };
                        if let Err((_, unsent)) = self.write(lines).await {
                            retry = Some(unsent);
                        }
                    }
                    let _ = done.send(());
                }
                lines = self.queued.recv() => {
                    let Some(lines) = lines else { return };
                    if let Err((_, unsent)) = self.write(lines).await {
                        retry = Some(unsent);
                    }
                }
            }
        }
    }

    async fn connect(&self) -> std::io::Result<Connection> {
        let address = (self.host.as_str(), self.port);
        let connect = async {
            Ok(match self.protocol {
                StatsdProtocol::Tcp => {
                    let stream = TcpStream::connect(address).await?;
                    crate::net::nodelay(&stream);
                    Connection::Tcp(stream)
                }
                StatsdProtocol::Udp => {
                    let target =
                        tokio::net::lookup_host(address)
                            .await?
                            .next()
                            .ok_or_else(|| {
                                std::io::Error::new(std::io::ErrorKind::NotFound, "no address")
                            })?;
                    let local = match target {
                        std::net::SocketAddr::V4(_) => "0.0.0.0:0",
                        std::net::SocketAddr::V6(_) => "[::]:0",
                    };
                    let socket = UdpSocket::bind(local).await?;
                    socket.connect(target).await?;
                    Connection::Udp(socket)
                }
            })
        };
        tokio::time::timeout(DIAL_TIMEOUT, connect)
            .await
            .unwrap_or_else(|_| Err(std::io::ErrorKind::TimedOut.into()))
    }

    /// Writes `lines`; on failure, closes the connection and returns the lines not fully
    /// written. Unlike gostats, which resends from the first unwritten byte, a partly written
    /// line is resent whole (docs/parity.md section 7).
    async fn write(&mut self, lines: Vec<u8>) -> Result<(), (String, Vec<u8>)> {
        let mut written = 0;
        let write = async {
            match self.connection.as_mut().expect("connected") {
                Connection::Tcp(stream) => {
                    while written < lines.len() {
                        match stream.write(&lines[written..]).await {
                            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
                            Ok(n) => written += n,
                            Err(error) => return Err(error),
                        }
                    }
                    Ok(())
                }
                Connection::Udp(socket) => socket.send(&lines).await.map(|_| ()),
            }
        };
        let result = tokio::time::timeout(WRITE_TIMEOUT, write)
            .await
            .unwrap_or_else(|_| Err(std::io::ErrorKind::TimedOut.into()));
        result.map_err(|error| {
            self.connection = None;
            (
                error.to_string(),
                lines[unsent_from(&lines, written)..].to_vec(),
            )
        })
    }

    /// Answers the waiting flushes without sending anything; at most eight times the queue's
    /// capacity, as gostats' `drainFlushQueue`, so steady flushing cannot hold it.
    fn release_flushes(&mut self) {
        for _ in 0..FLUSH_REQUESTS * 8 {
            match self.flush_requests.try_recv() {
                Ok(done) => {
                    let _ = done.send(());
                }
                Err(_) => return,
            }
        }
    }
}

/// Where to resend `lines` from once `written` bytes went out: the start of the first line not
/// fully written.
fn unsent_from(lines: &[u8], written: usize) -> usize {
    lines[..written]
        .iter()
        .rposition(|b| *b == b'\n')
        .map_or(0, |i| i + 1)
}

// The connection cannot be made to fail mid-write on demand, so this rule is tested here.
#[cfg(test)]
mod tests {
    use super::unsent_from;

    #[test]
    fn a_partly_written_line_is_resent_whole() {
        let lines = b"a:1|c\nbb:2|c\nccc:3|c\n";
        assert_eq!(unsent_from(lines, 0), 0);
        assert_eq!(unsent_from(lines, 3), 0);
        assert_eq!(unsent_from(lines, 6), 6);
        assert_eq!(unsent_from(lines, 9), 6);
        assert_eq!(unsent_from(lines, 13), 13);
        assert_eq!(unsent_from(lines, lines.len()), lines.len());
    }
}
