//! Stats sinks, against gostats v0.4.14 and envoyproxy/ratelimit@f4e7669's runner. The statsd
//! stream itself is compared with upstream in `tests/differential.rs`.

use llm_ratelimit::stats::{
    LoggingSink, Sink, SinkSettings, Stats, StatsdProtocol, StatsdSink, select_sink,
};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use tokio::net::{TcpListener, UdpSocket};

/// What a sink wrote.
#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<u8>>>);

impl Write for Output {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Output {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

fn logging_sink(output: &Output) -> LoggingSink {
    let now = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
    LoggingSink::new(Box::new(output.clone()), Box::new(move || now))
}

/// The expected lines were written by Go's `encoding/json` with gostats' `logLine`.
#[test]
fn the_logging_sink_writes_gostats_json_lines() {
    let output = Output::default();
    let sink = logging_sink(&output);
    sink.flush_counter(
        "ratelimit.service.rate_limit.d.k_<a&b>\u{2028}.total_hits",
        u64::MAX,
    );
    sink.flush();
    sink.warn("connection error: x\"y");
    assert_eq!(
        output.text(),
        concat!(
            r#"{"level":"debug","ts":1700000000.123457,"logger":"gostats.loggingsink","msg":"flushing counter","json":{"name":"ratelimit.service.rate_limit.d.k_\u003ca\u0026b\u003e\u2028.total_hits","type":"counter","value":"18446744073709551616.000000"}}"#,
            "\n",
            r#"{"level":"debug","ts":1700000000.123457,"logger":"gostats.loggingsink","msg":"flushing all stats","json":{"type":"all stats","value":"0.000000"}}"#,
            "\n",
            r#"{"level":"warn","ts":1700000000.123457,"logger":"gostats.loggingsink","msg":"connection error: x\"y","json":{}}"#,
            "\n",
        )
    );
}

const SEVERAL_SINKS: &str = "Error: unable to use more than one stats sink at the same time. Set one of USE_DOG_STATSD, USE_STATSD, USE_PROMETHEUS.";

#[tokio::test]
async fn the_sink_is_chosen_like_upstream() {
    let settings = SinkSettings::default();
    assert!(matches!(select_sink(&settings), Ok(Sink::Statsd(_))));

    let logging = SinkSettings {
        use_statsd: false,
        ..Default::default()
    };
    assert!(matches!(select_sink(&logging), Ok(Sink::Logging(_))));

    let disabled = SinkSettings {
        disable_stats: true,
        use_dog_statsd: true,
        use_prometheus: true,
        ..Default::default()
    };
    assert!(matches!(select_sink(&disabled), Ok(Sink::Null)));

    // USE_STATSD is on by default, so another sink alone is one too many.
    for (dog, prometheus) in [(true, false), (false, true)] {
        let several = SinkSettings {
            use_dog_statsd: dog,
            use_prometheus: prometheus,
            ..Default::default()
        };
        assert_eq!(select_sink(&several).err().as_deref(), Some(SEVERAL_SINKS));
    }
}

/// Over UDP each datagram holds whole lines, at most 1432 bytes of them; a longer line goes alone.
#[tokio::test]
async fn udp_datagrams_hold_whole_lines() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = server.local_addr().unwrap().port();
    let sink = StatsdSink::new("127.0.0.1", port, StatsdProtocol::Udp);
    let mut expected = String::new();
    for i in 0..100 {
        let name = format!("ratelimit.service.rate_limit.domain.key_{i:03}.total_hits");
        sink.flush_counter(&name, i);
        expected += &format!("{name}:{i}|c\n");
    }
    let long = "x".repeat(2000);
    sink.flush_counter(&long, 1);
    expected += &format!("{long}:1|c\n");
    sink.flush().await;

    let mut received = String::new();
    let mut buffer = vec![0; 65536];
    while received.len() < expected.len() {
        let n = tokio::time::timeout(Duration::from_secs(5), server.recv(&mut buffer))
            .await
            .expect("a datagram")
            .unwrap();
        let datagram = std::str::from_utf8(&buffer[..n]).unwrap();
        assert!(datagram.ends_with('\n'), "{datagram:?}");
        assert!(
            n <= 1432 || datagram.lines().count() == 1,
            "{n} bytes in {} lines",
            datagram.lines().count()
        );
        // Full datagrams: the next line would not have fitted.
        received += datagram;
    }
    assert_eq!(received, expected);
}

/// While the server is unreachable, buffers queue up to about 4 MiB; then they are dropped and
/// the loss is logged, and flushing does not wait.
#[tokio::test]
async fn buffers_are_dropped_when_the_server_is_unreachable() {
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    let output = Output::default();
    let sink = StatsdSink::with_log(
        "127.0.0.1",
        port,
        StatsdProtocol::Tcp,
        Arc::new(logging_sink(&output)),
    );
    let name = "n".repeat(1000);
    // 5 MiB of lines.
    for _ in 0..5 * 1024 {
        sink.flush_counter(&name, 1);
    }
    tokio::time::timeout(Duration::from_secs(1), sink.flush())
        .await
        .expect("a flush that cannot queue returns at once");
    let log = output.text();
    assert!(
        log.contains(r#""level":"error""#)
            && log.contains("bytes: statsd channel full, dropping stats buffer with 65"),
        "{log}"
    );
}

/// A flush waits for at most two failed connections, then the sink keeps retrying every 3
/// seconds and delivers once the server is up.
#[tokio::test]
async fn the_sink_reconnects() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);
    let output = Output::default();
    let sink = StatsdSink::with_log(
        "127.0.0.1",
        address.port(),
        StatsdProtocol::Tcp,
        Arc::new(logging_sink(&output)),
    );
    sink.flush_counter("before", 1);
    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(10), sink.flush())
        .await
        .expect("released after the second failure");
    assert!(
        started.elapsed() >= Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert!(output.text().contains(r#""level":"warn","ts":1700000000.123457,"logger":"gostats.loggingsink","msg":"connection error: "#));

    let server = TcpListener::bind(address).await.unwrap();
    sink.flush_counter("after", 2);
    let flushed = tokio::spawn(async move { sink.flush().await });
    let (stream, _) = tokio::time::timeout(Duration::from_secs(10), server.accept())
        .await
        .expect("reconnected")
        .unwrap();
    flushed.await.unwrap();
    let mut received = String::new();
    let mut lines = tokio::io::AsyncBufReadExt::lines(tokio::io::BufReader::new(stream));
    while let Ok(Ok(Some(line))) =
        tokio::time::timeout(Duration::from_millis(200), lines.next_line()).await
    {
        received += &line;
        received += "\n";
    }
    assert_eq!(received, "before:1|c\nafter:2|c\n");
}

/// Stats are flushed every `STATS_FLUSH_INTERVAL`, the first time one interval after starting.
#[tokio::test(start_paused = true)]
async fn stats_are_flushed_every_interval() {
    let output = Output::default();
    let stats = Arc::new(Stats::default());
    stats.config_load_success().inc();
    let _flushing = llm_ratelimit::stats::flush_every(
        stats.clone(),
        Sink::Logging(logging_sink(&output)),
        Duration::from_secs(10),
    );
    let flushes = |output: &Output| output.text().matches("flushing all stats").count();
    tokio::time::sleep(Duration::from_millis(9_900)).await;
    assert_eq!(flushes(&output), 0);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(flushes(&output), 1);
    assert!(output.text().contains("config_load_success"));
    stats.config_load_success().inc();
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(flushes(&output), 2);
    assert_eq!(output.text().matches("config_load_success").count(), 2);
}

/// Tag rules upstream's `EXTRA_TAGS` parsing cannot express, from gostats' `NewTagSet`: a value's
/// `:` becomes `_`, empty keys are left out, keys are written as they are.
#[tokio::test]
async fn tags_follow_gostats() {
    let tags = [("", "x"), ("k.e:y", "a:b"), ("b", "1")]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let stats = Stats::new(&tags);
    stats.config_load_success().inc();
    let output = Output::default();
    stats.flush(&Sink::Logging(logging_sink(&output))).await;
    assert!(
        output
            .text()
            .contains(r#""name":"ratelimit.service.config_load_success.__b=1.__k.e:y=a_b""#),
        "{}",
        output.text()
    );
}

/// Timers as gostats writes them: whole values as integers, others with six decimals
/// (`net_sink.go`'s `FlushTimer`, which takes integers only below `math.MaxUint64`); the
/// logging sink's `"timer"` line.
#[tokio::test]
async fn timers_are_written_like_gostats() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = server.local_addr().unwrap().port();
    let sink = StatsdSink::new("127.0.0.1", port, StatsdProtocol::Udp);
    for (name, value) in [
        ("a", 3.0),
        ("b", 1.5),
        ("c", -2.0),
        ("d", 0.0),
        ("e", 1e20),
        ("f", -0.0),
    ] {
        sink.flush_timer(name, value);
    }
    sink.flush().await;
    let mut buffer = vec![0; 65536];
    let n = tokio::time::timeout(Duration::from_secs(5), server.recv(&mut buffer))
        .await
        .expect("a datagram")
        .unwrap();
    assert_eq!(
        std::str::from_utf8(&buffer[..n]).unwrap(),
        "a:3|ms\nb:1.500000|ms\nc:-2.000000|ms\nd:0|ms\ne:100000000000000000000.000000|ms\nf:0|ms\n"
    );

    let output = Output::default();
    logging_sink(&output).flush_timer("t", 3.5);
    assert_eq!(
        output.text(),
        concat!(
            r#"{"level":"debug","ts":1700000000.123457,"logger":"gostats.loggingsink","msg":"flushing timer","json":{"name":"t","type":"timer","value":"3.500000"}}"#,
            "\n"
        )
    );
}

/// A call dropped before it ends (its client gone) still has its time recorded, as Go's
/// handler runs to its end.
#[tokio::test]
async fn a_dropped_call_is_timed() {
    let output = Output::default();
    let stats = Arc::new(Stats::default());
    let _flushing = llm_ratelimit::stats::flush_every(
        stats.clone(),
        Sink::Logging(logging_sink(&output)),
        Duration::from_secs(3600),
    );
    let server = llm_ratelimit::stats::ServerStats::new(stats);
    let call = server.record("M", std::future::pending::<()>());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), call)
            .await
            .is_err()
    );
    assert!(
        output
            .text()
            .contains(r#""name":"ratelimit_server.M.response_time","type":"timer""#),
        "{}",
        output.text()
    );
}

/// Gauges as gostats writes them: `name:value|g`; the logging sink's `"gauge"` line.
#[tokio::test]
async fn gauges_are_written_like_gostats() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = server.local_addr().unwrap().port();
    let sink = StatsdSink::new("127.0.0.1", port, StatsdProtocol::Udp);
    sink.flush_gauge("a", 0);
    sink.flush_gauge("b", u64::MAX);
    sink.flush().await;
    let mut buffer = vec![0; 65536];
    let n = tokio::time::timeout(Duration::from_secs(5), server.recv(&mut buffer))
        .await
        .expect("a datagram")
        .unwrap();
    assert_eq!(
        std::str::from_utf8(&buffer[..n]).unwrap(),
        "a:0|g\nb:18446744073709551615|g\n"
    );

    let output = Output::default();
    logging_sink(&output).flush_gauge("g", 7);
    assert_eq!(
        output.text(),
        concat!(
            r#"{"level":"debug","ts":1700000000.123457,"logger":"gostats.loggingsink","msg":"flushing gauge","json":{"name":"g","type":"gauge","value":"7.000000"}}"#,
            "\n"
        )
    );
}
