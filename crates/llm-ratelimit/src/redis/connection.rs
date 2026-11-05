//! How the service reaches Redis: upstream's main pool (`src/redis/driver_impl.go`
//! `newClientImpl`) over radix v4's dialer and pool, whose rules this follows (credentials and
//! a database from a `redis://` URL, `AUTH`, `SELECT`, a periodic `PING`), with upstream's
//! startup retry.

use crate::settings::Settings;
use fred::prelude::*;
use std::time::Duration;

/// How to reach Redis, from the `REDIS_*` settings of upstream's main pool.
#[derive(Clone, PartialEq)]
pub struct RedisConnection {
    /// `REDIS_SOCKET_TYPE`: Go's network (`unix`, `tcp`, `tcp4`, `tcp6`).
    pub network: String,
    /// The address radix dials: `REDIS_URL`, or the host of a `redis://` URL.
    pub address: String,
    /// radix's dialer credentials and database; empty when unset.
    pub username: String,
    pub password: String,
    pub database: String,
    /// The pool's connections (radix's default for 0: 4).
    pub pool_size: usize,
    /// The dial timeout; none for 0.
    pub timeout: Duration,
    /// `REDIS_URL` with its credentials masked, for messages.
    pub masked_url: String,
    /// `REDIS_AUTH`, `REDIS_POOL_SIZE`, `REDIS_POOL_ON_EMPTY_BEHAVIOR` and `REDIS_TYPE` as
    /// given, which `newClientImpl` logs and checks when it starts.
    pub auth: String,
    pub pool_size_setting: i64,
    /// `REDIS_PIPELINE_LIMIT`, which radix v4 ignores: upstream only warns.
    pub pipeline_limit: i64,
    pub pool_on_empty_behavior: String,
    pub redis_type: String,
    /// The pool's variables' prefix (`REDIS`, `REDIS_PERSECOND`), for errors naming them.
    pub prefix: &'static str,
    /// `REDIS_AUTH_FILE`: `user:password` or a password, read at each connection.
    pub auth_file: String,
    /// `REDIS_TLS` (or `REDIS_PERSECOND_TLS`): the pool's connections over TLS.
    pub tls: Option<Tls>,
    /// `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR`, for both pools.
    pub close_on_readonly: bool,
    /// `REDIS_PIPELINE_WINDOW` (nanoseconds) and `REDIS_CLUSTER_PIPELINE_PARALLELISM`, which
    /// upstream reads in `CLUSTER` mode.
    pub pipeline_window: i64,
    pub cluster_pipeline_parallelism: i64,
    /// `REDIS_URL` as given: in `CLUSTER` mode, the nodes to start from, comma-separated; in
    /// `SENTINEL` mode, the primary's name, then the sentinels.
    pub url: String,
    /// `REDIS_SENTINEL_AUTH` (or `REDIS_PERSECOND_SENTINEL_AUTH`): the sentinels' credentials.
    pub sentinel_auth: String,
    /// For a pool through sentinels: the sentinels and their credentials, which the auth
    /// file (upstream's pool dialer only) must not replace.
    sentinel_dial: Option<(Vec<Server>, (String, String))>,
    /// `REDIS_AWS_IAM_*` (or `REDIS_PERSECOND_AWS_IAM_*`) when enabled.
    pub aws_iam: Option<super::aws::IamSettings>,
    /// The ElastiCache IAM credentials, once loaded.
    iam: Option<Iam>,
}

/// The pool's ElastiCache IAM credentials (equal when they are the same).
#[derive(Debug, Clone)]
struct Iam(std::sync::Arc<super::aws::IamCredentials>);

impl PartialEq for Iam {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}

impl std::fmt::Debug for RedisConnection {
    /// Without the credentials: the URLs masked, the password and the auth left out.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConnection")
            .field("network", &self.network)
            .field("address", &mask_credentials_in_url(&self.address))
            .field("username", &self.username)
            .field("database", &self.database)
            .field("pool_size", &self.pool_size)
            .field("masked_url", &self.masked_url)
            .field("redis_type", &self.redis_type)
            .field("prefix", &self.prefix)
            .field("auth_file", &self.auth_file)
            .field("aws_iam", &self.aws_iam)
            .field("tls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

/// A pool's TLS configuration (the same for both pools: equal when it is the same one).
#[derive(Debug, Clone)]
pub struct Tls(pub std::sync::Arc<rustls::ClientConfig>);

impl PartialEq for Tls {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}

/// What `newClientImpl` takes for one pool, from its group of settings.
struct PoolSettings<'a> {
    /// The variables' prefix, for errors naming them.
    prefix: &'static str,
    socket_type: &'a str,
    url: &'a str,
    pool_size: i64,
    auth: &'a str,
    auth_file: &'a str,
    timeout: i64,
    pool_on_empty_behavior: &'a str,
    redis_type: &'a str,
    pipeline_limit: i64,
    close_on_readonly: bool,
    pipeline_window: i64,
    cluster_pipeline_parallelism: i64,
    sentinel_auth: &'a str,
    aws_iam: Option<super::aws::IamSettings>,
}

impl RedisConnection {
    /// The main pool's connection (`REDIS_*`), or why upstream refuses to start (or what is
    /// not supported yet).
    pub fn from_settings(s: &Settings) -> Result<Self, String> {
        Self::new(PoolSettings {
            prefix: "REDIS",
            socket_type: &s.redis_socket_type,
            url: &s.redis_url,
            pool_size: s.redis_pool_size,
            auth: &s.redis_auth,
            auth_file: &s.redis_auth_file,
            timeout: s.redis_timeout,
            pool_on_empty_behavior: &s.redis_pool_on_empty_behavior,
            redis_type: &s.redis_type,
            pipeline_limit: s.redis_pipeline_limit,
            close_on_readonly: s.redis_close_connection_on_readonly_error,
            pipeline_window: s.redis_pipeline_window,
            cluster_pipeline_parallelism: s.redis_cluster_pipeline_parallelism,
            sentinel_auth: &s.redis_sentinel_auth,
            aws_iam: s.redis_aws_iam_auth.then(|| super::aws::IamSettings {
                prefix: "REDIS",
                cache_name: s.redis_aws_iam_cache_name.clone(),
                user_id: s.redis_aws_iam_user_id.clone(),
                serverless: s.redis_aws_iam_serverless,
                region: s.redis_aws_iam_region.clone(),
            }),
        })
    }

    /// The per-second pool's connection (`REDIS_PERSECOND_*`), for limits in seconds.
    pub fn per_second_from_settings(s: &Settings) -> Result<Self, String> {
        Self::new(PoolSettings {
            prefix: "REDIS_PERSECOND",
            socket_type: &s.redis_persecond_socket_type,
            url: &s.redis_persecond_url,
            pool_size: s.redis_persecond_pool_size,
            auth: &s.redis_persecond_auth,
            auth_file: &s.redis_persecond_auth_file,
            timeout: s.redis_persecond_timeout,
            pool_on_empty_behavior: &s.redis_persecond_pool_on_empty_behavior,
            redis_type: &s.redis_persecond_type,
            pipeline_limit: s.redis_persecond_pipeline_limit,
            close_on_readonly: s.redis_close_connection_on_readonly_error,
            pipeline_window: s.redis_persecond_pipeline_window,
            cluster_pipeline_parallelism: s.redis_persecond_cluster_pipeline_parallelism,
            sentinel_auth: &s.redis_persecond_sentinel_auth,
            aws_iam: s
                .redis_persecond_aws_iam_auth
                .then(|| super::aws::IamSettings {
                    prefix: "REDIS_PERSECOND",
                    cache_name: s.redis_persecond_aws_iam_cache_name.clone(),
                    user_id: s.redis_persecond_aws_iam_user_id.clone(),
                    serverless: s.redis_persecond_aws_iam_serverless,
                    region: s.redis_persecond_aws_iam_region.clone(),
                }),
        })
    }

    fn new(p: PoolSettings<'_>) -> Result<Self, String> {
        let pool_size = match p.pool_size {
            0 => 4,
            size => usize::try_from(size)
                .map_err(|_| format!("invalid {}_POOL_SIZE {size}", p.prefix))?,
        };
        let (address, username, password, database) = dialled(p.auth, p.url);
        Ok(Self {
            network: p.socket_type.to_string(),
            address,
            username,
            password,
            database,
            pool_size,
            timeout: crate::settings::duration(p.timeout),
            masked_url: mask_credentials_in_url(p.url),
            auth: p.auth.to_string(),
            pool_size_setting: p.pool_size,
            pipeline_limit: p.pipeline_limit,
            pool_on_empty_behavior: p.pool_on_empty_behavior.to_string(),
            redis_type: p.redis_type.to_string(),
            prefix: p.prefix,
            auth_file: p.auth_file.to_string(),
            tls: None,
            close_on_readonly: p.close_on_readonly,
            pipeline_window: p.pipeline_window,
            cluster_pipeline_parallelism: p.cluster_pipeline_parallelism,
            url: p.url.to_string(),
            sentinel_auth: p.sentinel_auth.to_string(),
            sentinel_dial: None,
            aws_iam: p.aws_iam,
            iam: None,
        })
    }

    /// The sentinel at `url`, dialled over TCP with `SentinelDialer`: what is set of the
    /// sentinel auth, then of the first sentinel URL (`parseRedisURL(urls[0])`), then of its own
    /// URL (`Dial`), for the credentials and the database; no auth file.
    fn sentinel(&self, url: &str, first: &str) -> Self {
        let (_, mut username, mut password, mut database) = dialled(&self.sentinel_auth, first);
        let (address, own_username, own_password, own_database) = dialled("", url);
        for (field, own) in [
            (&mut username, own_username),
            (&mut password, own_password),
            (&mut database, own_database),
        ] {
            if field.is_empty() {
                *field = own;
            }
        }
        Self {
            network: "tcp".into(),
            address,
            username,
            password,
            database,
            auth_file: String::new(),
            ..self.clone()
        }
    }

    /// The node at `url` of a cluster, dialled over TCP as radix's cluster dials its nodes.
    fn node(&self, url: &str) -> Self {
        let (address, username, password, database) = dialled(&self.auth, url);
        Self {
            network: "tcp".into(),
            address,
            username,
            password,
            database,
            ..self.clone()
        }
    }

    /// Where to connect, as Go's dialer resolves the address for the network; failures are
    /// dial errors (retried like upstream's).
    async fn server(&self) -> Result<ServerConfig, String> {
        let network = self.network.as_str();
        let family: fn(&std::net::IpAddr) -> bool = match network {
            "unix" => {
                return Ok(ServerConfig::Unix {
                    path: self.address.clone().into(),
                });
            }
            "tcp" => |_| true,
            "tcp4" => std::net::IpAddr::is_ipv4,
            "tcp6" => std::net::IpAddr::is_ipv6,
            _ => return Err(format!("dial {network}: unknown network {network}")),
        };
        // An unparseable redis:// URL is dialled as it is: keep its credentials out of errors.
        let masked = mask_credentials_in_url(&self.address);
        let dial_error =
            |e: String| format!("dial {network}: {}", e.replace(&self.address, &masked));
        let (host, port) = crate::go::split_host_port(&self.address).map_err(dial_error)?;
        let port: u16 = port
            .parse()
            .map_err(|_| dial_error(format!("lookup {network}/{port}: unknown port")))?;
        let addresses: Vec<std::net::IpAddr> = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|e| dial_error(e.to_string()))?
            .map(|a| a.ip())
            .collect();
        let ip = addresses
            .iter()
            .find(|ip| family(ip))
            .ok_or_else(|| dial_error(format!("address {masked}: no suitable address found")))?;
        let mut server = Server::new(ip.to_string(), port);
        if self.tls.is_some() {
            server.tls_server_name = Some(self.tls_server_name().into());
        }
        Ok(ServerConfig::Centralized { server })
    }

    /// radix's AUTH arguments: with a user even when the password is empty.
    fn auth_arguments(credentials: &(String, String)) -> Option<Vec<String>> {
        match fred_credentials(credentials) {
            (Some(username), Some(password)) => Some(vec![username, password]),
            (_, Some(password)) => Some(vec![password]),
            _ => None,
        }
    }

    /// The user and password to authenticate with: from `REDIS_AUTH` (or the URL), or from
    /// the auth file, read now, what it leaves empty taken from the URL (radix's `Dial`).
    async fn credentials(&self) -> Result<(String, String), String> {
        self.connection_credentials().credentials().await
    }

    /// Whether upstream's dialer resolves the credentials at each connection (a
    /// `CredentialProvider`, which replaces radix's dialer with `CustomConn`).
    fn has_credential_provider(&self) -> bool {
        !self.auth_file.is_empty() || self.iam.is_some()
    }

    /// What reads the credentials at each connection: the ElastiCache IAM token, or the file
    /// and the URL's for what it leaves empty (with no file, they are `REDIS_AUTH`'s or the
    /// URL's).
    fn connection_credentials(&self) -> ConnectionCredentials {
        ConnectionCredentials {
            iam: self.iam.clone().map(|Iam(iam)| iam),
            path: self.auth_file.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            sentinels: self.sentinel_dial.clone(),
        }
    }

    /// A client builder; the probe leaves the database to the dial (`probe`).
    fn builder(&self, server: ServerConfig, probe: bool) -> Builder {
        // With an auth file, each connection reads it (the provider), as radix's dialer does;
        // fred reads it once connected, where radix reads it before dialling.
        let mut credential_provider = None;
        let (username, password) = if !self.has_credential_provider() {
            fred_credentials(&(self.username.clone(), self.password.clone()))
        } else {
            let provider: std::sync::Arc<dyn fred::types::config::CredentialProvider> =
                std::sync::Arc::new(self.connection_credentials());
            credential_provider = Some(provider);
            (None, None)
        };
        let mut builder = Builder::from_config(Config {
            server,
            username,
            password,
            database: self.database.parse().ok().filter(|_| !probe),
            fail_fast: true,
            credential_provider,
            tls: self
                .tls
                .clone()
                .map(|Tls(config)| fred::types::config::TlsConfig {
                    connector: fred::types::config::TlsConnector::Rustls(config.into()),
                    hostnames: fred::types::config::TlsHostMapping::None,
                }),
            ..Default::default()
        });
        let timeout = self.timeout;
        builder.with_connection_config(|c| {
            c.connection_timeout = timeout;
            // Go's TCP connections are `TCP_NODELAY`.
            c.tcp.nodelay = Some(true);
            // radix bounds only the dial, not the commands that set a connection up.
            c.internal_command_timeout = Duration::from_secs(10 * 365 * 24 * 3600);
        });
        builder
    }

    /// One attempt of upstream's loop: radix's dial (connect, AUTH, then the URL's database as
    /// given, which Redis may refuse), then `PONG` to `PING`, then the pool. A failed dial is
    /// traced like radix's; `failed` is upstream's `hadConnError`.
    async fn attempt(&self, failed: &mut bool) -> Result<Connected, String> {
        let probe = match self.probe(Dial::Single).await {
            Ok(probe) => probe,
            Err(message) => {
                traced_failure(failed, &message);
                return Err(message);
            }
        };
        traced_success(failed);
        Ok(Connected {
            pool: self.set_up(probe).await?,
            replicas: Vec::new(),
        })
    }

    /// One attempt of upstream's loop in `CLUSTER` mode (`ClusterConfig.New`): a pool on the
    /// first node that dials (each failure traced), the topology from it (`CLUSTER SLOTS`), a
    /// pool on each of its other nodes (`sync`, by their addresses, so with `REDIS_AUTH` but
    /// not the seed URL's credentials), then `PONG` to `PING` from the cluster.
    async fn attempt_cluster(&self, failed: &mut bool) -> Result<Connected, String> {
        let mut last_error = String::new();
        let mut probed = None;
        for seed in self.url.split(',') {
            match self.node(seed).probe(Dial::ClusterNode).await {
                Ok((_, probe)) => {
                    probed = Some((seed, probe));
                    break;
                }
                Err(message) => {
                    traced_failure(failed, &message);
                    last_error = message;
                }
            }
        }
        let Some((seed, probe)) = probed else {
            return Err(format!(
                "could not connect to any redis instances, last error was: {last_error}"
            ));
        };
        traced_success(failed);
        let slots: Result<fred::types::Value, _> =
            probe.custom(fred::cmd!("CLUSTER"), vec!["SLOTS"]).await;
        let _ = probe.quit().await;
        let slots = slots.map_err(|e| format!("response returned from Conn: {}", e.details()))?;
        let mut primaries = Vec::new();
        let mut secondaries = Vec::new();
        for (address, primary) in topology(&slots) {
            // The seed's pool is under its URL: a node spelled alike is not dialled again.
            if address != seed {
                match self.node(&address).probe(Dial::ClusterNode).await {
                    Ok((_, client)) => {
                        let _ = client.quit().await;
                        traced_success(failed);
                    }
                    Err(message) => {
                        traced_failure(failed, &message);
                        return Err(format!(
                            "error creating client for {}: {message}",
                            crate::go::quote(&address)
                        ));
                    }
                }
            }
            match (primary, crate::go::split_host_port(&address)) {
                (true, Ok((host, port))) => {
                    if let Ok(port) = port.parse() {
                        primaries.push(Server::new(host, port));
                    }
                }
                (false, _) => secondaries.push(address),
                _ => {}
            }
        }
        // fred's clients over the primaries, dialled as the nodes are.
        let pool = self
            .node("")
            .open_pool(ServerConfig::Clustered {
                hosts: primaries,
                policy: Default::default(),
            })
            .await?;
        self.with_replicas(pool, &secondaries).await
    }

    /// A pool of the pool size's clients on `server`, reconnecting as radix's, once it answers
    /// `PONG` to `PING`; a failure closes it (upstream's `client.Close()`).
    async fn open_pool(&self, server: ServerConfig) -> Result<Pool, String> {
        let pool = self
            .builder(server, false)
            .set_policy(radix_reconnects())
            .build_pool(self.pool_size)
            .map_err(|e| e.to_string())?;
        let opened = async {
            pool.init().await.map_err(|e| e.details().to_string())?;
            let pong: String = pool
                .next()
                .ping(None)
                .await
                .map_err(|e| e.details().to_string())?;
            match pong.as_str() {
                "PONG" => Ok(()),
                _ => Err(format!(
                    "unexpected PING response: {}",
                    crate::go::quote(&pong)
                )),
            }
        }
        .await;
        if let Err(e) = opened {
            let _ = pool.quit().await;
            return Err(e);
        }
        Ok(pool)
    }

    /// `pool` with the pools radix also keeps on `replicas`; a failure closes them all.
    async fn with_replicas(&self, pool: Pool, replicas: &[String]) -> Result<Connected, String> {
        match self.replica_pools(replicas).await {
            Ok(replicas) => Ok(Connected { pool, replicas }),
            Err(e) => {
                let _ = pool.quit().await;
                Err(e)
            }
        }
    }

    /// The pools radix keeps on `replicas` (which the service never uses): their connections
    /// count in the pool's stats. A failure closes those opened.
    async fn replica_pools(&self, replicas: &[String]) -> Result<Vec<Pool>, String> {
        let mut pools = Vec::new();
        for address in replicas {
            let node = self.node(address);
            let opened = match node.server().await {
                Ok(server) => node.open_pool(server).await,
                Err(e) => Err(e),
            };
            match opened {
                Ok(pool) => pools.push(pool),
                Err(e) => {
                    for pool in pools {
                        let _ = pool.quit().await;
                    }
                    return Err(e);
                }
            }
        }
        Ok(pools)
    }

    /// The sentinels of `REDIS_URL` (after the primary's name), as `SentinelDialer` dials them.
    fn sentinels(&self) -> Vec<Self> {
        let urls: Vec<&str> = self.url.split(',').skip(1).collect();
        urls.iter().map(|url| self.sentinel(url, urls[0])).collect()
    }

    /// The first sentinel that dials (`dialSentinel`), or the last dial's error.
    async fn dial_sentinel(&self) -> Result<Client, String> {
        let mut last_error = String::new();
        for sentinel in self.sentinels() {
            match sentinel.probe(Dial::Sentinel).await {
                Ok((_, client)) => return Ok(client),
                Err(message) => last_error = message,
            }
        }
        Err(last_error)
    }

    /// `SENTINEL MASTER` and `SENTINEL SLAVES` on `sentinel`: the primary's address, then the
    /// replicas' (those flagged `slave` alone).
    async fn sentinel_nodes(&self, sentinel: &Client) -> Result<(String, Vec<String>), String> {
        let name = self.url.split(',').next().unwrap_or_default();
        let ask = |what: &'static str| {
            sentinel.custom::<fred::types::Value, _>(fred::cmd!("SENTINEL"), vec![what, name])
        };
        let refused =
            |e: fred::error::Error| format!("response returned from Conn: {}", e.details());
        let primary = ask("MASTER").await.map_err(refused)?;
        let replicas = ask("SLAVES").await.map_err(refused)?;
        let fields = |value: &fred::types::Value| -> std::collections::HashMap<String, String> {
            let fred::types::Value::Array(items) = value else {
                return Default::default();
            };
            items
                .chunks(2)
                .filter_map(|pair| Some((pair.first()?.as_string()?, pair.get(1)?.as_string()?)))
                .collect()
        };
        let address = |m: &std::collections::HashMap<String, String>, command: &str| match (
            m.get("ip"),
            m.get("port"),
        ) {
            (Some(ip), Some(port)) if !ip.is_empty() && !port.is_empty() => {
                Ok(crate::net::join_host_port(ip, port.parse().unwrap_or(0)))
            }
            _ => Err(format!("malformed {}: {m:?}", crate::go::quote(command))),
        };
        let primary = address(&fields(&primary), "SENTINEL MASTER")?;
        let mut secondaries = Vec::new();
        if let fred::types::Value::Array(replicas) = &replicas {
            for replica in replicas {
                let replica = fields(replica);
                if replica.get("flags").map(String::as_str) == Some("slave") {
                    secondaries.push(address(&replica, "SENTINEL SLAVES")?);
                }
            }
        }
        Ok((primary, secondaries))
    }

    /// One attempt of upstream's loop in `SENTINEL` mode (`SentinelConfig.New`): the first
    /// sentinel that dials (untraced), the sentinels it knows (`SENTINEL SENTINELS`), the
    /// primary and its replicas, a pool on each (by address, with `REDIS_AUTH`), then `PONG`
    /// to `PING` from the primary.
    async fn attempt_sentinel(&self, failed: &mut bool) -> Result<Connected, String> {
        let name = self.url.split(',').next().unwrap_or_default();
        let sentinel = self.dial_sentinel().await?;
        let known: Result<fred::types::Value, _> = sentinel
            .custom(fred::cmd!("SENTINEL"), vec!["SENTINELS", name])
            .await;
        let nodes = match known {
            Ok(_) => self.sentinel_nodes(&sentinel).await,
            Err(e) => Err(format!("response returned from Conn: {}", e.details())),
        };
        let _ = sentinel.quit().await;
        let (primary, secondaries) = nodes?;
        for address in std::iter::once(&primary).chain(&secondaries) {
            match self.node(address).probe(Dial::Single).await {
                Ok((_, client)) => {
                    let _ = client.quit().await;
                    traced_success(failed);
                }
                Err(message) => {
                    traced_failure(failed, &message);
                    return Err(format!(
                        "error creating client for {}: {message}",
                        crate::go::quote(address)
                    ));
                }
            }
        }
        let sentinels = self.sentinels();
        let hosts = sentinels
            .iter()
            .filter_map(|s| {
                let (host, port) = crate::go::split_host_port(&s.address).ok()?;
                Some(Server::new(host, port.parse().ok()?))
            })
            .collect::<Vec<_>>();
        let first = sentinels.first();
        let sentinel_credentials = first
            .map(|s| (s.username.clone(), s.password.clone()))
            .unwrap_or_default();
        let (username, password) = fred_credentials(&sentinel_credentials);
        // The pool's settings: by the nodes' addresses (with `REDIS_AUTH`).
        let mut pooled = self.node("");
        pooled.sentinel_dial = Some((hosts.clone(), sentinel_credentials));
        let pool = pooled
            .open_pool(ServerConfig::Sentinel {
                hosts,
                service_name: name.to_string(),
                username,
                password,
            })
            .await?;
        self.with_replicas(pool, &secondaries).await
    }

    /// radix's sentinel `spin`: every 5 s, the primary the sentinels name; when it changed (a
    /// failover), the pool reconnects to it. (radix also subscribes to `switch-master`, which
    /// no sentinel publishes: they publish `+switch-master`.)
    fn follow_primary(&self, pool: Pool) {
        let connection = self.clone();
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_secs(5),
                Duration::from_secs(5),
            );
            loop {
                ticks.tick().await;
                let Ok(sentinel) = connection.dial_sentinel().await else {
                    continue;
                };
                let nodes = connection.sentinel_nodes(&sentinel).await;
                let _ = sentinel.quit().await;
                let Ok((primary, _)) = nodes else {
                    continue;
                };
                // Every client on the primary (those reconnecting aside): one left on the
                // demoted node would get READONLY errors.
                let on_primary = pool.clients().iter().all(|client| {
                    client.active_connections().iter().all(|server| {
                        crate::net::join_host_port(&server.host, server.port.into()) == primary
                    })
                });
                if !on_primary {
                    let _ = pool.force_reconnection().await;
                }
            }
        });
    }

    /// radix's `Dialer.Dial`: the connection, then AUTH and SELECT as it sends them, which
    /// Redis may refuse. Then the probe, which answers `PING`.
    async fn probe(&self, dial: Dial) -> Result<(ServerConfig, Client), String> {
        // The auth file is read before dialling (radix's `CustomConn`).
        let credentials = self.credentials().await?;
        let server = self.server().await?;
        let mut stream = self.dial(&server).await?;
        let mut commands = Vec::new();
        commands.extend(Self::auth_arguments(&credentials).map(|a| ("AUTH", a)));
        if !self.database.is_empty() {
            commands.push(("SELECT", vec![self.database.clone()]));
        }
        // A cluster's dialer sends READONLY, unless upstream replaced it (`CustomConn`).
        if dial == Dial::ClusterNode && !self.has_credential_provider() && !self.close_on_readonly {
            commands.push(("READONLY", Vec::new()));
        }
        for (name, arguments) in commands {
            // Sent by hand: fred reads Redis' refusal of an unneeded AUTH as `OK`.
            command(&mut stream, name, &arguments).await?;
        }
        drop(stream);
        let probe = self
            .builder(server.clone(), true)
            .build()
            .map_err(|e| e.to_string())?;
        probe.init().await.map_err(|e| e.details().to_string())?;
        Ok((server, probe))
    }

    /// `tls.Dial`'s server name: the address before its last `:`, without IPv6's brackets.
    fn tls_server_name(&self) -> String {
        let host = self
            .address
            .rsplit_once(':')
            .map_or(self.address.as_str(), |(host, _)| host);
        host.strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(host)
            .to_string()
    }

    /// The TLS handshake on `stream`, if the pool uses TLS; a failure is the TLS error alone,
    /// as Go's `tls.Dialer` returns it.
    async fn handshake(&self, stream: Stream) -> std::io::Result<Stream> {
        let Some(Tls(config)) = &self.tls else {
            return Ok(stream);
        };
        let name = rustls::pki_types::ServerName::try_from(self.tls_server_name())
            .map_err(|e| std::io::Error::other(TlsError(format!("tls: {e}"))))?;
        let connector = tokio_rustls::TlsConnector::from(config.clone());
        match connector.connect(name, stream).await {
            Ok(tls) => Ok(Box::new(tls)),
            Err(e) => Err(std::io::Error::other(TlsError(crate::tls::error_text(&e)))),
        }
    }

    /// Go's dial of the server: `dial <network> <address>: connect: <errno>`, or a timeout;
    /// then the TLS handshake, within the same timeout.
    async fn dial(&self, server: &ServerConfig) -> Result<Stream, String> {
        let network = &self.network;
        let (address, connected) = match server {
            ServerConfig::Unix { path } => {
                let address = path.display().to_string();
                let connect = tokio::net::UnixStream::connect(path.clone());
                let connected = async {
                    let stream = Box::new(connect.await?) as Stream;
                    self.handshake(stream).await
                };
                (address, self.within_timeout(connected).await)
            }
            ServerConfig::Centralized { server } => {
                let address = crate::net::join_host_port(&server.host, server.port.into());
                let connect =
                    tokio::net::TcpStream::connect((server.host.to_string(), server.port));
                let started = tokio::time::Instant::now();
                let connected = self
                    .within_timeout(async {
                        let stream = connect.await?;
                        crate::net::nodelay(&stream);
                        Ok(Box::new(stream) as Stream)
                    })
                    .await;
                // The handshake shares the dial's deadline; past it, Go says the context's.
                let connected = match connected {
                    Some(Ok(stream)) => {
                        let left = self.timeout.saturating_sub(started.elapsed());
                        let handshake = self.handshake(stream);
                        match self.timeout.is_zero() {
                            true => Some(handshake.await),
                            false => match tokio::time::timeout(left, handshake).await {
                                Ok(result) => Some(result),
                                Err(_) => return Err("context deadline exceeded".into()),
                            },
                        }
                    }
                    other => other,
                };
                (address, connected)
            }
            _ => return Err(format!("dial {network}: unsupported server")),
        };
        match connected {
            Some(Ok(stream)) => Ok(stream),
            Some(Err(e)) if e.get_ref().is_some_and(|inner| inner.is::<TlsError>()) => Err(e
                .into_inner()
                .map(|inner| inner.to_string())
                .unwrap_or_default()),
            Some(Err(e)) if e.raw_os_error().is_some() => Err(format!(
                "dial {network} {address}: connect: {}",
                crate::go::errno_text(&e)
            )),
            Some(Err(e)) => Err(format!("dial {network} {address}: {e}")),
            None => Err(format!("dial {network} {address}: i/o timeout")),
        }
    }

    /// `connect` within the dial timeout (none for 0); `None` once it has passed.
    async fn within_timeout<T>(
        &self,
        connect: impl std::future::Future<Output = std::io::Result<T>>,
    ) -> Option<std::io::Result<T>> {
        match self.timeout.is_zero() {
            true => Some(connect.await),
            false => tokio::time::timeout(self.timeout, connect).await.ok(),
        }
    }

    /// The probe's `PING`, then the pool.
    async fn set_up(&self, (server, probe): (ServerConfig, Client)) -> Result<Pool, String> {
        let checked = async {
            let pong: String = probe
                .ping(None)
                .await
                .map_err(|e| e.details().to_string())?;
            match pong.as_str() {
                "PONG" => Ok(()),
                _ => Err(format!(
                    "unexpected PING response: {}",
                    crate::go::quote(&pong)
                )),
            }
        }
        .await;
        let _ = probe.quit().await;
        checked?;
        if !self.database.is_empty() && self.database.parse::<u8>().is_err() {
            return Err(format!(
                "{}_URL database {} is not supported",
                self.prefix,
                crate::go::quote(&self.database)
            ));
        }
        let pool = self
            .builder(server, false)
            .set_policy(radix_reconnects())
            .build_pool(self.pool_size)
            .map_err(|e| e.to_string())?;
        pool.init().await.map_err(|e| e.details().to_string())?;
        Ok(pool)
    }

    /// `newClientImpl`'s startup, with its log lines: retries until Redis answers `PING`,
    /// waiting as `REDIS_STARTUP_*` say, and gives up after `REDIS_STARTUP_MAX_ELAPSED_TIME`
    /// (never for 0) or when `cancelled` resolves. Then the pool, which reconnects lost
    /// connections and pings every `5s / size` like radix's.
    pub async fn connect(
        &self,
        startup: &StartupBackoff,
        cancelled: impl std::future::Future<Output = ()>,
    ) -> Result<Connected, String> {
        // `newCredentialProviderFromSettings`, before `newClientImpl` starts.
        if let Some(iam) = &self.aws_iam {
            if !self.auth.is_empty() || !self.auth_file.is_empty() {
                return Err(format!(
                    "{p}_AWS_IAM_AUTH cannot be combined with {p}_AUTH or {p}_AUTH_FILE",
                    p = self.prefix
                ));
            }
            let credentials = super::aws::IamCredentials::load(iam).await?;
            let connection = Self {
                aws_iam: None,
                iam: Some(Iam(std::sync::Arc::new(credentials))),
                ..self.clone()
            };
            return Box::pin(connection.connect(startup, cancelled)).await;
        }
        if !self.auth_file.is_empty() && !self.auth.is_empty() {
            return Err(format!(
                "{p}_AUTH and {p}_AUTH_FILE are mutually exclusive",
                p = self.prefix
            ));
        }
        let masked = &self.masked_url;
        crate::log::warn(&format!(
            "connecting to redis on {masked} with pool size {}",
            self.pool_size_setting
        ));
        // createDialer.
        if self.tls.is_some() {
            crate::log::warn(&format!("enabling TLS to redis {masked}"));
        }
        if !self.auth.is_empty() {
            match self.auth.split_once(':') {
                Some((user, _)) => crate::log::warn(&format!(
                    "enabling authentication to redis {masked} with user {user}"
                )),
                None => crate::log::warn(&format!(
                    "enabling authentication to redis {masked} without user"
                )),
            }
        }
        if self.pipeline_limit > 0 {
            crate::log::warn(&format!(
                "REDIS_PIPELINE_LIMIT={} is deprecated and has no effect in radix v4. Write buffering is controlled solely by REDIS_PIPELINE_WINDOW.",
                self.pipeline_limit
            ));
        }
        let cluster = self.redis_type.eq_ignore_ascii_case("cluster");
        if cluster && self.pipeline_window > 0 {
            crate::debug!(
                "Cluster mode: setting WriteFlushInterval to {}",
                crate::go::format_duration(self.pipeline_window)
            );
        }
        if self.has_credential_provider() {
            crate::log::warn(&format!(
                "Redis pool {masked}: resolving credentials on every connection attempt"
            ));
        }
        if self.close_on_readonly {
            crate::log::warn(&format!(
                "Redis pool {masked}: closing connections on READONLY error replies"
            ));
        }
        if cluster {
            let configured = self.cluster_pipeline_parallelism;
            let effective = cluster_pipeline_parallelism(configured, self.pool_size_setting)?;
            crate::log::warn(&match configured {
                0 => format!(
                    "Redis cluster pipeline parallelism: auto bounded to Redis pool size ({effective} concurrent groups)"
                ),
                _ if configured != effective => format!(
                    "Redis cluster pipeline parallelism: configured value {configured} exceeds Redis pool size {}; bounded to {effective} concurrent groups",
                    self.pool_size_setting
                ),
                1 => "Redis cluster pipeline parallelism: disabled (serial legacy behavior)".into(),
                _ => format!(
                    "Redis cluster pipeline parallelism: bounded to {effective} concurrent groups"
                ),
            });
        }
        match self.pool_on_empty_behavior.to_uppercase().as_str() {
            "WAIT" => crate::log::warn(&format!(
                "Redis pool {masked}: WAIT is default in radix v4 (blocks until connection available)"
            )),
            "CREATE" => return Err("REDIS_POOL_ON_EMPTY_BEHAVIOR=CREATE is not supported in radix v4. Pool will block instead of creating overflow connections. Remove this setting or set to WAIT, and consider increasing REDIS_POOL_SIZE.".into()),
            "ERROR" => return Err("REDIS_POOL_ON_EMPTY_BEHAVIOR=ERROR is not supported in radix v4. Pool will block instead of failing fast. Remove this setting or set to WAIT, and use context timeouts for fail-fast behavior.".into()),
            _ => crate::log::warn(&format!(
                "Redis pool {masked}: using v4 default (fixed size={}, blocks when full)",
                self.pool_size_setting
            )),
        }
        let sentinel = self.redis_type.eq_ignore_ascii_case("sentinel");
        if sentinel && self.url.split(',').count() < 2 {
            return Err("Expected master name and a list of urls for the sentinels, in the format: <redis master name>,<sentinel1>,...,<sentineln>".into());
        }
        let start = std::time::Instant::now();
        let mut attempt = 0;
        let mut failed = false;
        tokio::pin!(cancelled);
        let connected = loop {
            match self.redis_type.to_lowercase().as_str() {
                // Upstream logs REDIS_URL as given, credentials included (docs/parity.md §7).
                "single" => crate::log::warn(&format!("Creating single with urls {masked}")),
                // `%v` of the URLs.
                "cluster" => crate::log::warn(&format!(
                    "Creating cluster with urls [{}]",
                    self.url
                        .split(',')
                        .map(mask_credentials_in_url)
                        .collect::<Vec<_>>()
                        .join(" ")
                )),
                // Its dialer, made at each attempt (`createDialer`), logs again.
                "sentinel" => {
                    let target = format!("sentinel({masked})");
                    if self.tls.is_some() {
                        crate::log::warn(&format!("enabling TLS to redis {target}"));
                    }
                    if !self.sentinel_auth.is_empty() {
                        match self.sentinel_auth.split_once(':') {
                            Some((user, _)) => crate::log::warn(&format!(
                                "enabling authentication to redis {target} with user {user}"
                            )),
                            None => crate::log::warn(&format!(
                                "enabling authentication to redis {target} without user"
                            )),
                        }
                    }
                }
                _ => return Err(format!("Unrecognized redis type {}", self.redis_type)),
            }
            let attempted = match (cluster, sentinel) {
                (true, _) => self.attempt_cluster(&mut failed).await,
                (_, true) => self.attempt_sentinel(&mut failed).await,
                _ => self.attempt(&mut failed).await,
            };
            let error = match attempted {
                Ok(connected) => break connected,
                Err(message) => message,
            };
            let elapsed = start.elapsed();
            let rounded = (elapsed.as_nanos() as i64 + 500_000) / 1_000_000 * 1_000_000;
            if !startup.max_elapsed.is_zero() && elapsed >= startup.max_elapsed {
                return Err(format!(
                    "timed out waiting for Redis connection to {masked} after {}: {error}",
                    crate::go::format_duration(rounded)
                ));
            }
            let wait = startup.duration(attempt);
            crate::log::warn(&format!(
                "Retrying Redis connection to {masked} in {} (elapsed: {}): {error}",
                crate::go::format_duration(wait.as_nanos() as i64),
                crate::go::format_duration(rounded)
            ));
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = &mut cancelled => {
                    return Err(format!(
                        "context cancelled while waiting for Redis connection to {}: context canceled",
                        self.masked_url
                    ));
                }
            }
            attempt += 1;
        };
        if sentinel {
            self.follow_primary(connected.pool.clone());
        }
        let interval = Duration::from_secs(5) / self.pool_size as u32;
        for pinger in std::iter::once(&connected.pool).chain(&connected.replicas) {
            let pinger = pinger.clone();
            tokio::spawn(async move {
                let mut ticks =
                    tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
                loop {
                    ticks.tick().await;
                    let _: Result<String, _> = pinger.next().ping(None).await;
                }
            });
        }
        Ok(connected)
    }
}

/// The pools a connection opened: the one the service uses, and those radix also keeps on
/// the replicas (a cluster's, or the sentinels' primary's).
pub struct Connected {
    pub pool: Pool,
    pub replicas: Vec<Pool>,
}

/// What radix dials: the server, a cluster's node (which also gets `READONLY`), or a
/// sentinel.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Dial {
    Single,
    ClusterNode,
    Sentinel,
}

/// `poolTrace` of a failed connection: logged, and remembered (`hadConnError`).
fn traced_failure(failed: &mut bool, message: &str) {
    *failed = true;
    crate::log::error(&format!("creating redis connection error : {message}"));
}

/// `poolTrace` of a connection made: logged if one failed before.
fn traced_success(failed: &mut bool) {
    if std::mem::take(failed) {
        crate::log::info("redis connection re-established after previous error");
    }
}

/// `ClusterTopo` of a `CLUSTER SLOTS` reply: each node's address (`host:port`) and whether
/// it is a primary, by its first slot, a primary before its secondaries.
fn topology(slots: &fred::types::Value) -> Vec<(String, bool)> {
    use fred::types::Value;
    let mut nodes: Vec<(String, i64, bool)> = Vec::new();
    let Value::Array(sets) = slots else {
        return Vec::new();
    };
    for set in sets {
        let Value::Array(set) = set else { continue };
        let Some(start) = set.first().and_then(Value::as_i64) else {
            continue;
        };
        for (index, node) in set.iter().skip(2).enumerate() {
            let Value::Array(node) = node else { continue };
            let (Some(host), Some(port)) = (
                node.first().and_then(Value::as_string),
                node.get(1).and_then(Value::as_i64),
            ) else {
                continue;
            };
            let address = crate::net::join_host_port(&host, port);
            match nodes.iter_mut().find(|(a, _, _)| *a == address) {
                Some(known) => known.1 = known.1.min(start),
                None => nodes.push((address, start, index == 0)),
            }
        }
    }
    nodes.sort_by_key(|(_, start, primary)| (*start, !*primary));
    nodes
        .into_iter()
        .map(|(address, _, primary)| (address, primary))
        .collect()
}

/// `effectiveClusterPipelineParallelism`: 1 stays serial; 0, or more than the pool's size (at
/// least 1), is the pool's size; a negative value is what upstream panics with.
fn cluster_pipeline_parallelism(configured: i64, pool_size: i64) -> Result<i64, String> {
    if configured < 0 {
        return Err("redis cluster pipeline parallelism must be >= 0".into());
    }
    if configured == 1 {
        return Ok(1);
    }
    let ceiling = pool_size.max(1);
    Ok(match configured == 0 || configured > ceiling {
        true => ceiling,
        false => configured,
    })
}

/// What radix dials for `url`: the address, then the user, password and database. The auth
/// (`createDialer`'s) is `user:password` or a password; radix fills in what is still empty from
/// a redis:// URL.
fn dialled(auth: &str, url: &str) -> (String, String, String, String) {
    let (mut username, mut password) = match auth.split_once(':') {
        Some((user, pass)) => (user.to_string(), pass.to_string()),
        None => (String::new(), auth.to_string()),
    };
    let (address, url) = parse_redis_url(url);
    if username.is_empty() {
        username = url.username;
    }
    if password.is_empty() {
        password = url.password;
    }
    (address, username, password, url.database)
}

/// fred's user and password for `credentials` (radix sends AUTH with a user even when the
/// password is empty).
fn fred_credentials((username, password): &(String, String)) -> (Option<String>, Option<String>) {
    match (username.is_empty(), password.is_empty()) {
        (false, _) => (Some(username.clone()), Some(password.clone())),
        (true, false) => (None, Some(password.clone())),
        (true, true) => (None, None),
    }
}

/// `fileCredentialProvider.Credentials`: the file trimmed, `user:password` or a password;
/// empty, it is an error.
fn read_auth_file(path: &str) -> Result<(String, String), String> {
    let contents = std::fs::read(path).map_err(|e| {
        let op = if e.kind() == std::io::ErrorKind::IsADirectory {
            "read"
        } else {
            "open"
        };
        format!("{op} {path}: {}", crate::go::errno_text(&e))
    })?;
    let contents = String::from_utf8_lossy(&contents);
    let auth = contents.trim();
    if auth.is_empty() {
        return Err(format!("redis credential file {path} is empty"));
    }
    Ok(match auth.split_once(':') {
        Some((user, password)) => (user.to_string(), password.to_string()),
        None => (String::new(), auth.to_string()),
    })
}

/// The pool's credentials at each connection: the ElastiCache IAM token, or those of the
/// auth file, read again, what it leaves empty being the URL's.
struct ConnectionCredentials {
    /// The ElastiCache IAM token's, in place of the file.
    iam: Option<std::sync::Arc<super::aws::IamCredentials>>,
    /// Empty: the URL's credentials (or `REDIS_AUTH`'s) as they are.
    path: String,
    username: String,
    password: String,
    /// Through sentinels: theirs, and their own credentials.
    sentinels: Option<(Vec<Server>, (String, String))>,
}

impl std::fmt::Debug for ConnectionCredentials {
    /// The path only: the password stays out.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionCredentials")
            .field("path", &self.path)
            .finish()
    }
}

impl ConnectionCredentials {
    async fn credentials(&self) -> Result<(String, String), String> {
        match &self.iam {
            Some(iam) => iam.credentials().await,
            None => self.file(),
        }
    }

    fn file(&self) -> Result<(String, String), String> {
        if self.path.is_empty() {
            return Ok((self.username.clone(), self.password.clone()));
        }
        let (mut username, mut password) = read_auth_file(&self.path)?;
        if username.is_empty() {
            username = self.username.clone();
        }
        if password.is_empty() {
            password = self.password.clone();
        }
        Ok((username, password))
    }
}

#[tonic::async_trait]
impl fred::types::config::CredentialProvider for ConnectionCredentials {
    async fn fetch(
        &self,
        server: Option<&Server>,
    ) -> Result<(Option<String>, Option<String>), fred::error::Error> {
        // fred asks for the sentinels' too: theirs are not the file's.
        if let (Some((sentinels, credentials)), Some(server)) = (&self.sentinels, server)
            && sentinels
                .iter()
                .any(|s| s.host == server.host && s.port == server.port)
        {
            return Ok(fred_credentials(credentials));
        }
        let credentials = self
            .credentials()
            .await
            .map_err(|e| fred::error::Error::new(fred::error::ErrorKind::Auth, e))?;
        Ok(fred_credentials(&credentials))
    }
}

/// A TLS handshake's failure, told apart from the dial's.
#[derive(Debug)]
struct TlsError(String);

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TlsError {}

/// A dialled connection to Redis: TCP or Unix.
type Stream = Box<dyn Io>;
trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Io for T {}

/// One command on `stream`, as radix's `conn.Do` during its dial: an error reply is
/// `response returned from Conn: <error>`.
async fn command(stream: &mut Stream, name: &str, arguments: &[String]) -> Result<(), String> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let mut request = format!("*{}\r\n", arguments.len() + 1).into_bytes();
    for argument in std::iter::once(name).chain(arguments.iter().map(String::as_str)) {
        request.extend(format!("${}\r\n", argument.len()).into_bytes());
        request.extend(argument.as_bytes());
        request.extend(b"\r\n");
    }
    stream
        .write_all(&request)
        .await
        .map_err(|e| e.to_string())?;
    // AUTH and SELECT answer one line: `+OK`, or `-<error>`.
    let mut reply = Vec::new();
    let read = tokio::io::BufReader::new(stream)
        .read_until(b'\n', &mut reply)
        .await
        .map_err(|e| e.to_string())?;
    if read == 0 {
        return Err("EOF".into());
    }
    let reply = String::from_utf8_lossy(&reply);
    let reply = reply.trim_end_matches(['\r', '\n']);
    match reply.strip_prefix('-') {
        Some(error) => Err(format!("response returned from Conn: {error}")),
        None => Ok(()),
    }
}

/// `jpillora/backoff` as upstream configures it for Redis startup: `min × 2^attempt`, a random
/// point between `min` and that, within `[min, max]`.
#[derive(Debug, Clone)]
pub struct StartupBackoff {
    pub min: Duration,
    pub max: Duration,
    /// Give up after this long; never for 0.
    pub max_elapsed: Duration,
}

impl StartupBackoff {
    pub fn from_settings(s: &Settings) -> Self {
        Self {
            min: crate::settings::duration(s.redis_startup_initial_interval),
            max: crate::settings::duration(s.redis_startup_max_interval),
            max_elapsed: crate::settings::duration(s.redis_startup_max_elapsed_time),
        }
    }

    /// `ForAttempt` with jitter.
    pub fn duration(&self, attempt: u32) -> Duration {
        let min = if self.min.is_zero() {
            Duration::from_millis(100)
        } else {
            self.min
        };
        let max = if self.max.is_zero() {
            Duration::from_secs(10)
        } else {
            self.max
        };
        if min >= max {
            return max;
        }
        let minf = min.as_nanos() as f64;
        let durf = minf * 2f64.powi(attempt.min(1000) as i32);
        let jittered = random_unit() * (durf - minf) + minf;
        if !jittered.is_finite() || jittered > i64::MAX as f64 {
            return max;
        }
        Duration::from_nanos(jittered as u64).clamp(min, max)
    }
}

/// A random number in `[0, 1)` (no cryptographic quality needed for a retry delay).
fn random_unit() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    hasher.write_u128(now.as_nanos());
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// What radix reads from a `redis://` URL (`parseRedisURL`, after Go's `url.Parse`): the host,
/// and credentials and a database from the user info, the path or the query (empty when
/// absent). Any other address, or one Go cannot parse, is used as it is.
struct RedisUrl {
    username: String,
    password: String,
    database: String,
}

fn parse_redis_url(url: &str) -> (String, RedisUrl) {
    let mut parsed = RedisUrl {
        username: String::new(),
        password: String::new(),
        database: String::new(),
    };
    let Some(rest) = url.strip_prefix("redis://") else {
        return (url.to_string(), parsed);
    };
    let rest = rest.split_once('#').map_or(rest, |(before, _)| before);
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (authority, path) = match rest.find('/') {
        Some(at) => (&rest[..at], &rest[at..]),
        None => (rest, ""),
    };
    let (userinfo, host) = match authority.rfind('@') {
        Some(at) => (Some(&authority[..at]), &authority[at + 1..]),
        None => (None, authority),
    };
    let unescaped = |s: &str| crate::go::unescape(s, false);
    let (user, pass) = match userinfo.map(|u| u.split_once(':').unwrap_or((u, ""))) {
        Some((user, pass)) => (unescaped(user), unescaped(pass)),
        None => (Some(String::new()), Some(String::new())),
    };
    let database = unescaped(path);
    // Go's url.Parse refuses a host with spaces, as it does bad escapes.
    let valid_host = !host.contains(char::is_whitespace);
    let (Some(user), Some(pass), Some(database), true) = (user, pass, database, valid_host) else {
        return (url.to_string(), parsed);
    };
    // url.Values.Get: the first value of a key; a pair with `;` is dropped.
    let query_value = |name: &str| {
        query
            .split('&')
            .filter(|pair| !pair.contains(';'))
            .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
            .find(|(k, _)| crate::go::unescape(k, true).as_deref() == Some(name))
            .and_then(|(_, v)| crate::go::unescape(v, true))
            .unwrap_or_default()
    };
    parsed.username = query_value("username");
    if !user.is_empty() {
        parsed.username = user;
    }
    parsed.password = query_value("password");
    if userinfo.is_some_and(|u| u.contains(':')) {
        parsed.password = pass;
    }
    parsed.database = query_value("db");
    if !database.is_empty() && database != "/" {
        parsed.database = database[1..].to_string();
    }
    (host.to_string(), parsed)
}

/// `MaskCredentialsInUrl` (`redis://user:pass@host` becomes `redis://*****@host`, for each
/// comma-separated URL), and the `password` and `username` query parameters masked too, which
/// upstream leaves in its messages (`docs/parity.md` §7).
pub fn mask_credentials_in_url(url: &str) -> String {
    url.split(',')
        .map(|u| {
            let parts: Vec<&str> = u.split('@').collect();
            let masked = match parts[..] {
                [first, .., last] if first.starts_with("redis://") => {
                    format!("redis://*****@{last}")
                }
                _ => u.to_string(),
            };
            let Some((base, query)) = masked.split_once('?') else {
                return masked;
            };
            let query: Vec<String> = query
                .split('&')
                .map(|pair| {
                    let (key, _) = pair.split_once('=').unwrap_or((pair, ""));
                    // Compared as the parser reads keys (decoded).
                    let decoded = crate::go::unescape(key, true).unwrap_or_default();
                    match matches!(decoded.as_str(), "password" | "username") {
                        true => format!("{key}=*****"),
                        false => pair.to_string(),
                    }
                })
                .collect();
            format!("{base}?{}", query.join("&"))
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// radix's reconnection after a lost connection: 125 ms, doubled on each failure up to 4 s.
fn radix_reconnects() -> ReconnectPolicy {
    let mut policy = ReconnectPolicy::new_exponential(0, 125, 4000, 2);
    policy.set_jitter(0);
    policy
}

#[cfg(test)]
mod cluster_tests {
    use super::*;

    /// A connection from these settings over the defaults.
    fn connection(variables: &[(&str, &str)]) -> RedisConnection {
        let lookup = |key: &str| {
            variables
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        };
        RedisConnection::from_settings(&Settings::from_lookup(&lookup).unwrap()).unwrap()
    }

    /// radix dials the topology's nodes by their addresses: with `REDIS_AUTH`, but not the
    /// seed URL's credentials nor its database.
    #[test]
    fn cluster_nodes_take_the_auth_but_not_the_seed_url() {
        let seeded = connection(&[("REDIS_URL", "redis://u:pw@10.0.0.1:7000/0")]);
        let node = seeded.node("10.0.0.2:7001");
        assert_eq!(
            (
                node.address.as_str(),
                node.username.as_str(),
                node.password.as_str()
            ),
            ("10.0.0.2:7001", "", "")
        );
        assert_eq!((node.database.as_str(), node.network.as_str()), ("", "tcp"));
        let seed = seeded.node("redis://u:pw@10.0.0.1:7000/0");
        assert_eq!(
            (seed.username.as_str(), seed.password.as_str()),
            ("u", "pw")
        );
        let authenticated = connection(&[("REDIS_AUTH", "user:secret")]).node("10.0.0.2:7001");
        assert_eq!(
            (
                authenticated.username.as_str(),
                authenticated.password.as_str()
            ),
            ("user", "secret")
        );
    }

    /// Its debug form leaves the credentials out.
    #[test]
    fn a_connection_debugs_without_credentials() {
        let printed = format!(
            "{:?}",
            connection(&[
                ("REDIS_URL", "redis://u:hunter2@h:1"),
                ("REDIS_AUTH", "x:s3cret")
            ])
        );
        assert!(
            !printed.contains("hunter2") && !printed.contains("s3cret"),
            "{printed}"
        );
    }

    /// `ClusterTopo`: nodes by their first slot, merged across ranges, primaries before their
    /// secondaries.
    #[test]
    fn a_topology_is_read_like_radix() {
        use fred::types::Value;
        let node = |host: &str, port: i64| {
            Value::Array(vec![
                Value::String(host.into()),
                Value::Integer(port),
                Value::String("id".into()),
            ])
        };
        let set = |start: i64, end: i64, nodes: Vec<Value>| {
            let mut set = vec![Value::Integer(start), Value::Integer(end)];
            set.extend(nodes);
            Value::Array(set)
        };
        let slots = Value::Array(vec![
            set(10923, 16383, vec![node("10.0.0.3", 7002)]),
            set(
                5461,
                10922,
                vec![node("10.0.0.2", 7001), node("10.0.0.5", 7004)],
            ),
            set(0, 100, vec![node("::1", 7000)]),
            set(101, 5460, vec![node("::1", 7000)]),
        ]);
        assert_eq!(
            topology(&slots),
            [
                ("[::1]:7000".to_string(), true),
                ("10.0.0.2:7001".into(), true),
                ("10.0.0.5:7004".into(), false),
                ("10.0.0.3:7002".into(), true),
            ]
        );
    }

    #[test]
    fn cluster_pipeline_parallelism_is_bounded_like_upstream() {
        let bounded = |configured, size| cluster_pipeline_parallelism(configured, size);
        assert_eq!(bounded(1, 10), Ok(1));
        assert_eq!(bounded(0, 10), Ok(10));
        assert_eq!(bounded(4, 10), Ok(4));
        assert_eq!(bounded(11, 10), Ok(10));
        assert_eq!(bounded(0, 0), Ok(1));
        assert_eq!(bounded(3, 0), Ok(1));
        assert_eq!(
            bounded(-1, 10),
            Err("redis cluster pipeline parallelism must be >= 0".into())
        );
    }
}

#[cfg(test)]
mod auth_file_tests {
    use super::*;

    /// radix fills the user and the password independently from the URL, each only if the
    /// file left it empty.
    #[test]
    fn a_file_is_completed_by_the_url() {
        let dir = std::env::temp_dir().join(format!("ratelimit-auth-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let credentials = |contents: &str| {
            let path = dir.join("auth");
            std::fs::write(&path, contents).unwrap();
            ConnectionCredentials {
                iam: None,
                path: path.to_string_lossy().into_owned(),
                username: "url-user".into(),
                password: "url-pass".into(),
                sentinels: None,
            }
            .file()
            .unwrap()
        };
        assert_eq!(credentials("me:"), ("me".into(), "url-pass".into()));
        assert_eq!(credentials("secret"), ("url-user".into(), "secret".into()));
        assert_eq!(credentials(":secret"), ("url-user".into(), "secret".into()));
        assert_eq!(credentials("me:a:b\n"), ("me".into(), "a:b".into()));
    }
}
