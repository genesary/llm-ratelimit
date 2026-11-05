//! `MEMCACHE_SRV`: the servers from a DNS SRV record (`src/srv/srv.go`), each `target:port`
//! (the target fully qualified, its final dot kept, as Go's `LookupSRV` gives it), sorted.

/// `ParseSrv`: `_<service>._<proto>.<name>`.
fn parse(srv: &str) -> Result<(String, String, String), String> {
    // `^_(.+?)\._(.+?)\.(.+)$`: the shortest service, then the shortest protocol.
    let invalid = || {
        let text = format!("could not parse {srv} to SRV parts");
        crate::log::error(&text);
        text
    };
    let rest = srv.strip_prefix('_').ok_or_else(invalid)?;
    let (service, rest) = shortest_before(rest, "._").ok_or_else(invalid)?;
    let (proto, name) = shortest_before(rest, ".").ok_or_else(invalid)?;
    if name.is_empty() {
        return Err(invalid());
    }
    Ok((service.into(), proto.into(), name.into()))
}

/// The text before the first `separator` that leaves at least one character before it.
fn shortest_before<'a>(text: &'a str, separator: &str) -> Option<(&'a str, &'a str)> {
    let start = text.char_indices().nth(1).map_or(text.len(), |(i, _)| i);
    let at = text.get(start..)?.find(separator)? + start;
    Some((&text[..at], &text[at + separator.len()..]))
}

/// The resolver's configuration: its servers and options.
type ResolverSetup = (
    hickory_resolver::config::ResolverConfig,
    hickory_resolver::config::ResolverOpts,
);

/// `ServerStringsFromSrv`: the record's servers, sorted (gomemcache shards by their order);
/// failures logged as upstream logs them.
async fn servers_with(
    srv: &str,
    setup: Result<ResolverSetup, String>,
) -> Result<Vec<String>, String> {
    let (service, proto, name) = parse(srv).inspect_err(|e| {
        crate::log::error(&format!("failed to parse SRV: {e}"));
    })?;
    let target = format!("_{service}._{proto}.{name}");
    let records = lookup(&target, setup).await.inspect_err(|e| {
        crate::log::error(&format!("failed to lookup SRV: {e}"));
    })?;
    crate::debug!("found {} servers(s) from SRV", records.len());
    let mut servers: Vec<String> = records
        .iter()
        .enumerate()
        .map(|(i, (target, port))| {
            let server = format!("{target}:{port}");
            crate::debug!("server from srv[{i}]: {server}");
            server
        })
        .collect();
    servers.sort();
    Ok(servers)
}

/// `refreshServers`: the record's servers, resolved; `None` if either fails.
pub async fn fetch(srv: &str) -> Option<Vec<super::client::Address>> {
    fetch_with(srv, system_setup()).await
}

async fn fetch_with(
    srv: &str,
    setup: Result<ResolverSetup, String>,
) -> Option<Vec<super::client::Address>> {
    let servers = servers_with(srv, setup).await.ok()?;
    super::client::resolve(&servers).await
}

/// `refreshServersPeriodically`: every `period`, the client's servers replaced by the
/// record's; a failure keeps them, with a warning.
pub fn refresh_every(
    client: std::sync::Arc<super::client::Client>,
    srv: String,
    period: std::time::Duration,
) {
    tokio::spawn(refresh_with(client, srv, period, system_setup));
}

async fn refresh_with(
    client: std::sync::Arc<super::client::Client>,
    srv: String,
    period: std::time::Duration,
    setup: impl Fn() -> Result<ResolverSetup, String>,
) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    // A `time.Ticker` drops the ticks a slow refresh misses.
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticks.tick().await;
        match fetch_with(&srv, setup()).await {
            Some(servers) => {
                client.set_servers(servers);
                crate::debug!("refreshed memcache hosts");
            }
            None => crate::log::warn("failed to refresh memcache hosts"),
        }
    }
}

/// The system's resolver configuration (`/etc/resolv.conf`), which Go's resolver reads.
fn system_setup() -> Result<ResolverSetup, String> {
    hickory_resolver::system_conf::read_system_conf().map_err(|e| e.to_string())
}

/// The SRV records of `name`.
async fn lookup(
    name: &str,
    setup: Result<ResolverSetup, String>,
) -> Result<Vec<(String, u16)>, String> {
    let (config, options) = setup.map_err(|e| format!("lookup {name}: {e}"))?;
    // Go's `DNSError` names the server it asked: the first one.
    let server = config.name_servers().first().map(|s| {
        let port = s.connections.first().map_or(53, |c| c.port);
        std::net::SocketAddr::new(s.ip, port).to_string()
    });
    let resolver = hickory_resolver::Resolver::builder_with_config(
        config,
        hickory_resolver::net::runtime::TokioRuntimeProvider::default(),
    )
    .with_options(options)
    .build()
    .map_err(|e| format!("lookup {name}: {e}"))?;
    let found = resolver
        .lookup(name, hickory_resolver::proto::rr::RecordType::SRV)
        .await
        .map_err(|e| match (e.is_no_records_found(), &server) {
            (true, Some(server)) => format!("lookup {name} on {server}: no such host"),
            _ => format!("lookup {name}: {e}"),
        })?;
    Ok(found
        .answers()
        .iter()
        .filter_map(|record| match &record.data {
            hickory_resolver::proto::rr::RData::SRV(srv) => {
                Some((srv.target.to_string(), srv.port))
            }
            _ => None,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_resolver::config::{ConnectionConfig, NameServerConfig, ResolverConfig};
    use hickory_resolver::proto::op::{Message, MessageType};
    use hickory_resolver::proto::rr::{Name, RData, Record, rdata::SRV};

    /// A DNS server on UDP answering every query with `targets` (`(name, port)`), or none.
    async fn dns_server(targets: Vec<(&'static str, u16)>) -> ResolverSetup {
        dns_server_of(std::sync::Arc::new(std::sync::Mutex::new(targets))).await
    }

    /// [`dns_server`], its targets changing with `targets`.
    async fn dns_server_of(
        targets: std::sync::Arc<std::sync::Mutex<Vec<(&'static str, u16)>>>,
    ) -> ResolverSetup {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buffer = [0u8; 512];
            while let Ok((n, from)) = socket.recv_from(&mut buffer).await {
                let query = Message::from_vec(&buffer[..n]).unwrap();
                let mut answer = Message::new(
                    query.metadata.id,
                    MessageType::Response,
                    query.metadata.op_code,
                );
                answer.metadata.recursion_desired = query.metadata.recursion_desired;
                answer.metadata.recursion_available = true;
                answer.queries = query.queries.clone();
                let targets = targets.lock().unwrap().clone();
                if targets.is_empty() {
                    answer.metadata.response_code =
                        hickory_resolver::proto::op::ResponseCode::NXDomain;
                }
                let name = query.queries[0].name().clone();
                for (target, port) in &targets {
                    let srv = SRV::new(0, 0, *port, Name::from_ascii(target).unwrap());
                    answer.add_answer(Record::from_rdata(name.clone(), 60, RData::SRV(srv)));
                }
                let _ = socket.send_to(&answer.to_vec().unwrap(), from).await;
            }
        });
        let mut connection = ConnectionConfig::udp();
        connection.port = address.port();
        let server = NameServerConfig::new(address.ip(), true, vec![connection]);
        (
            ResolverConfig::from_name_servers(vec![server]),
            Default::default(),
        )
    }

    /// The servers of a record: `target:port`, the target with its final dot, sorted.
    #[tokio::test]
    async fn records_give_their_targets_sorted() {
        let setup = dns_server(vec![("b.example.com.", 11212), ("a.example.com.", 11211)]).await;
        assert_eq!(
            servers_with("_memcache._tcp.example.com", Ok(setup)).await,
            Ok(vec![
                "a.example.com.:11211".to_string(),
                "b.example.com.:11212".to_string()
            ])
        );
    }

    /// `MEMCACHE_SRV_REFRESH`: the record read again at each period, the client's servers
    /// following it.
    #[tokio::test]
    async fn records_are_read_again() {
        let targets = std::sync::Arc::new(std::sync::Mutex::new(vec![("localhost.", 11211)]));
        let setup = dns_server_of(targets.clone()).await;
        let client = std::sync::Arc::new(super::super::client::Client::new(Vec::new(), 0, None));
        let period = std::time::Duration::from_millis(50);
        tokio::spawn(refresh_with(
            client.clone(),
            "_m._tcp.example.com".into(),
            period,
            move || Ok(setup.clone()),
        ));
        let port = |client: &super::super::client::Client| match client.servers().as_slice() {
            [super::super::client::Address::Tcp(address)] => Some(address.port()),
            _ => None,
        };
        let wait_for = async |expected| {
            for _ in 0..100 {
                if port(&client) == Some(expected) {
                    return;
                }
                tokio::time::sleep(period).await;
            }
            panic!(
                "the servers did not become port {expected}: {:?}",
                client.servers()
            );
        };
        wait_for(11211).await;
        *targets.lock().unwrap() = vec![("localhost.", 11212)];
        wait_for(11212).await;
    }

    /// A name without records: Go's `DNSError`, naming the server asked.
    #[tokio::test]
    async fn names_without_records_are_not_found() {
        let setup = dns_server(vec![]).await;
        let server = setup.0.name_servers()[0].ip;
        let port = setup.0.name_servers()[0].connections[0].port;
        assert_eq!(
            servers_with("_memcache._tcp.example.com", Ok(setup)).await,
            Err(format!(
                "lookup _memcache._tcp.example.com on {server}:{port}: no such host"
            ))
        );
    }

    /// `srvRegex`'s lazy groups: the shortest service and protocol.
    #[test]
    fn records_are_parsed_as_upstream_parses_them() {
        let parsed = |srv| parse(srv).ok();
        let parts = |a: &str, b: &str, c: &str| Some((a.into(), b.into(), c.into()));
        assert_eq!(
            parsed("_memcache._tcp.example.com"),
            parts("memcache", "tcp", "example.com")
        );
        // `.+?` takes one character at least, then the first separator after it.
        assert_eq!(parsed("_a._b.c"), parts("a", "b", "c"));
        assert_eq!(parsed("__._..x"), parts("_", ".", "x"));
        assert_eq!(parsed("_s._p.q._r.t"), parts("s", "p", "q._r.t"));
        assert_eq!(parsed("memcache._tcp.example.com"), None);
        assert_eq!(parsed("_memcache._tcp"), None);
        assert_eq!(parsed("_memcache._tcp."), None);
    }
}
