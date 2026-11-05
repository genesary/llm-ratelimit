//! The environment variables of envoyproxy/ratelimit (`src/settings/settings.go`), read as
//! envconfig v1.4.0 reads them: a variable set to an empty value counts as set; integers in
//! any Go base, Go booleans, durations and floats parsed by Go's rules; lists split on `,` and
//! maps on `,` then `:`. The first invalid value, in upstream's field order, is an error with
//! envconfig's text (upstream panics with it).

use crate::config::LoadOptions;
use crate::go;
use crate::limiter::CacheSettings;
use crate::service::{LimitHeaders, ServiceSettings};
use crate::stats::{
    DogStatsdSettings, Mogrifier, PrometheusSettings, SinkSettings, StatsdProtocol,
};
use std::collections::BTreeMap;

/// A `time.Duration` setting as a `Duration`; 0 for a negative one.
pub fn duration(nanos: i64) -> std::time::Duration {
    std::time::Duration::from_nanos(u64::try_from(nanos).unwrap_or(0))
}

/// Reads one environment variable; `None` when unset.
pub type Lookup<'a> = &'a dyn Fn(&str) -> Option<String>;

/// A variable that cannot be read; the text is envconfig's.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SettingsError(pub String);

/// A setting's value, for comparing with upstream's.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    Bool(bool),
    Float(f64),
    List(Vec<String>),
    Map(BTreeMap<String, String>),
}

/// How a kind of setting is read and shown.
trait Kind {
    type Value: Default;
    /// The Go type, in envconfig's errors.
    const GO_TYPE: &'static str;
    /// Whether upstream's settings show it (`values`).
    const SHOWN: bool = true;
    fn parse(text: &str) -> Result<Self::Value, String>;
    fn value(value: &Self::Value) -> Value;

    /// `envconfig.Process` for the field.
    fn read(
        lookup: Lookup,
        key: &str,
        default: &str,
        field: &str,
    ) -> Result<Self::Value, SettingsError> {
        read::<Self>(lookup, key, default, field)
    }
}

struct Str;
struct Int;
struct Int64;
struct Bool;
struct Duration;
struct Float32;
struct Float64;
struct List;
struct Map;
struct TlsConfig;

impl Kind for Str {
    type Value = String;
    const GO_TYPE: &'static str = "string";
    fn parse(text: &str) -> Result<String, String> {
        Ok(text.to_string())
    }
    fn value(value: &String) -> Value {
        Value::Str(value.clone())
    }
}

impl Kind for Int {
    type Value = i64;
    const GO_TYPE: &'static str = "int";
    fn parse(text: &str) -> Result<i64, String> {
        go::parse_int(text, 64)
    }
    fn value(value: &i64) -> Value {
        Value::Int(*value)
    }
}

impl Kind for Int64 {
    type Value = i64;
    const GO_TYPE: &'static str = "int64";
    fn parse(text: &str) -> Result<i64, String> {
        go::parse_int(text, 64)
    }
    fn value(value: &i64) -> Value {
        Value::Int(*value)
    }
}

impl Kind for Bool {
    type Value = bool;
    const GO_TYPE: &'static str = "bool";
    fn parse(text: &str) -> Result<bool, String> {
        go::parse_bool(text)
    }
    fn value(value: &bool) -> Value {
        Value::Bool(*value)
    }
}

/// A `time.Duration`, in nanoseconds (it may be negative).
impl Kind for Duration {
    type Value = i64;
    const GO_TYPE: &'static str = "time.Duration";
    fn parse(text: &str) -> Result<i64, String> {
        go::parse_duration(text)
    }
    fn value(value: &i64) -> Value {
        Value::Int(*value)
    }
}

impl Kind for Float32 {
    type Value = f32;
    const GO_TYPE: &'static str = "float32";
    fn parse(text: &str) -> Result<f32, String> {
        go::parse_float(text, 32).map(|v| v as f32)
    }
    fn value(value: &f32) -> Value {
        Value::Float(f64::from(*value))
    }
}

impl Kind for Float64 {
    type Value = f64;
    const GO_TYPE: &'static str = "float64";
    fn parse(text: &str) -> Result<f64, String> {
        go::parse_float(text, 64)
    }
    fn value(value: &f64) -> Value {
        Value::Float(*value)
    }
}

impl Kind for List {
    type Value = Vec<String>;
    const GO_TYPE: &'static str = "[]string";
    fn parse(text: &str) -> Result<Vec<String>, String> {
        if text.trim().is_empty() {
            return Ok(Vec::new());
        }
        Ok(text.split(',').map(String::from).collect())
    }
    fn value(value: &Vec<String>) -> Value {
        Value::List(value.clone())
    }
}

impl Kind for Map {
    type Value = BTreeMap<String, String>;
    const GO_TYPE: &'static str = "map[string]string";
    fn parse(text: &str) -> Result<BTreeMap<String, String>, String> {
        let mut map = BTreeMap::new();
        if text.trim().is_empty() {
            return Ok(map);
        }
        for pair in text.split(',') {
            let parts: Vec<&str> = pair.split(':').collect();
            let [key, value] = parts[..] else {
                return Err(format!("invalid map item: {}", go::quote(pair)));
            };
            map.insert(key.to_string(), value.to_string());
        }
        Ok(map)
    }
    fn value(value: &BTreeMap<String, String>) -> Value {
        Value::Map(value.clone())
    }
}

/// An untagged `*tls.Config` setting: envconfig still reads `<FIELD>_<TLS FIELD>` for each
/// field of Go's `tls.Config` it can decode. Upstream overwrites or ignores the values; only an
/// invalid one matters, as it panics. Checked in `tls.Config`'s field order.
impl Kind for TlsConfig {
    type Value = ();
    const GO_TYPE: &'static str = "*tls.Config";
    const SHOWN: bool = false;
    fn parse(_: &str) -> Result<(), String> {
        Ok(())
    }
    fn value(_: &()) -> Value {
        Value::List(Vec::new())
    }
    fn read(lookup: Lookup, prefix: &str, _: &str, _: &str) -> Result<(), SettingsError> {
        let uint16 = |v: &str| go::parse_uint_bits(v, 16).map(|_| ());
        let int = |v: &str| go::parse_int(v, 64).map(|_| ());
        let boolean = |v: &str| go::parse_bool(v).map(|_| ());
        let uint16s = |v: &str| -> Result<(), String> {
            if v.trim().is_empty() {
                return Ok(());
            }
            v.split(',').try_for_each(uint16)
        };
        let map = |v: &str| Map::parse(v).map(|_| ());
        type Check<'a> = &'a dyn Fn(&str) -> Result<(), String>;
        let fields: [(&str, &str, Check); 11] = [
            ("NameToCertificate", "map[string]*tls.Certificate", &map),
            ("ClientAuth", "tls.ClientAuthType", &int),
            ("InsecureSkipVerify", "bool", &boolean),
            ("CipherSuites", "[]uint16", &uint16s),
            ("PreferServerCipherSuites", "bool", &boolean),
            ("SessionTicketsDisabled", "bool", &boolean),
            ("MinVersion", "uint16", &uint16),
            ("MaxVersion", "uint16", &uint16),
            ("CurvePreferences", "[]tls.CurveID", &uint16s),
            ("DynamicRecordSizingDisabled", "bool", &boolean),
            ("Renegotiation", "tls.RenegotiationSupport", &int),
        ];
        for (field, go_type, check) in fields {
            let key = format!("{prefix}_{}", field.to_uppercase());
            if let Some(value) = lookup(&key) {
                check(&value)
                    .map_err(|e| SettingsError(parse_error(&key, field, &value, go_type, &e)))?;
            }
        }
        Ok(())
    }
}

/// `envconfig.Process` for one field: the variable if set (even empty), else the default if
/// any, else the zero value.
fn read<K: Kind + ?Sized>(
    lookup: Lookup,
    key: &str,
    default: &str,
    field: &str,
) -> Result<K::Value, SettingsError> {
    let value = match lookup(key) {
        Some(value) => value,
        None if !default.is_empty() => default.to_string(),
        None => return Ok(K::Value::default()),
    };
    K::parse(&value).map_err(|e| SettingsError(parse_error(key, field, &value, K::GO_TYPE, &e)))
}

/// envconfig's `ParseError` text.
fn parse_error(key: &str, field: &str, value: &str, go_type: &str, error: &str) -> String {
    format!(
        "envconfig.Process: assigning {key} to {field}: converting '{value}' to type {go_type}. \
         details: {error}"
    )
}

/// The Rust type of a kind of setting.
macro_rules! rust_type {
    (Str) => { String };
    (Int) => { i64 };
    (Int64) => { i64 };
    (Bool) => { bool };
    (Duration) => { i64 };
    (Float32) => { f32 };
    (Float64) => { f64 };
    (List) => { Vec<String> };
    (Map) => { BTreeMap<String, String> };
    (TlsConfig) => { () };
}

macro_rules! settings {
    ($($field:ident: $kind:ident = $key:literal, $default:literal, $go:literal;)*) => {
        /// Every setting of upstream's `Settings`, by its variable's name in lower case.
        #[derive(Debug, Clone, PartialEq)]
        pub struct Settings {
            $(#[doc = concat!("`", $key, "`")] pub $field: rust_type!($kind),)*
        }

        impl Settings {
            /// The settings `lookup` gives, as `settings.NewSettings` reads them.
            pub fn from_lookup(lookup: Lookup) -> Result<Self, SettingsError> {
                Ok(Self {
                    $($field: <$kind as Kind>::read(lookup, $key, $default, $go)?,)*
                })
            }

            /// Each setting by variable, in upstream's order.
            pub fn values(&self) -> Vec<(&'static str, Value)> {
                let mut values = Vec::new();
                $(if <$kind as Kind>::SHOWN {
                    values.push(($key, <$kind as Kind>::value(&self.$field)));
                })*
                values
            }

            /// Each variable, with its default as written, in upstream's order: what
            /// `docs/environment.md` documents.
            pub fn variables() -> Vec<(&'static str, &'static str)> {
                let mut variables = Vec::new();
                $(if <$kind as Kind>::SHOWN {
                    variables.push(($key, $default));
                })*
                variables
            }
        }
    };
}

// Upstream's fields, in order: field, kind, variable, default, Go field.
settings! {
    host: Str = "HOST", "0.0.0.0", "Host";
    port: Int = "PORT", "8080", "Port";
    debug_host: Str = "DEBUG_HOST", "0.0.0.0", "DebugHost";
    debug_port: Int = "DEBUG_PORT", "6070", "DebugPort";
    grpc_uds: Str = "GRPC_UDS", "", "GrpcUds";
    grpc_host: Str = "GRPC_HOST", "0.0.0.0", "GrpcHost";
    grpc_port: Int = "GRPC_PORT", "8081", "GrpcPort";
    grpc_server_tls_config: TlsConfig = "GRPCSERVERTLSCONFIG", "", "GrpcServerTlsConfig";
    grpc_max_connection_age: Duration = "GRPC_MAX_CONNECTION_AGE", "24h", "GrpcMaxConnectionAge";
    grpc_max_connection_age_grace: Duration = "GRPC_MAX_CONNECTION_AGE_GRACE", "1h", "GrpcMaxConnectionAgeGrace";
    grpc_server_use_tls: Bool = "GRPC_SERVER_USE_TLS", "false", "GrpcServerUseTLS";
    grpc_server_tls_cert: Str = "GRPC_SERVER_TLS_CERT", "", "GrpcServerTlsCert";
    grpc_server_tls_key: Str = "GRPC_SERVER_TLS_KEY", "", "GrpcServerTlsKey";
    grpc_client_tls_cacert: Str = "GRPC_CLIENT_TLS_CACERT", "", "GrpcClientTlsCACert";
    grpc_client_tls_san: Str = "GRPC_CLIENT_TLS_SAN", "", "GrpcClientTlsSAN";
    log_level: Str = "LOG_LEVEL", "WARN", "LogLevel";
    log_format: Str = "LOG_FORMAT", "text", "LogFormat";
    config_type: Str = "CONFIG_TYPE", "FILE", "ConfigType";
    force_start_without_initial_config: Bool = "FORCE_START_WITHOUT_INITIAL_CONFIG", "false", "ForceStartWithoutInitialConfig";
    config_grpc_xds_node_id: Str = "CONFIG_GRPC_XDS_NODE_ID", "default", "ConfigGrpcXdsNodeId";
    config_grpc_xds_node_metadata: Str = "CONFIG_GRPC_XDS_NODE_METADATA", "", "ConfigGrpcXdsNodeMetadata";
    config_grpc_xds_server_url: Str = "CONFIG_GRPC_XDS_SERVER_URL", "localhost:18000", "ConfigGrpcXdsServerUrl";
    config_grpc_xds_server_connect_retry_interval: Duration = "CONFIG_GRPC_XDS_SERVER_CONNECT_RETRY_INTERVAL", "3s", "ConfigGrpcXdsServerConnectRetryInterval";
    config_grpc_xds_client_additional_headers: Map = "CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS", "", "ConfigGrpcXdsClientAdditionalHeaders";
    config_grpc_xds_tls_config: TlsConfig = "CONFIGGRPCXDSTLSCONFIG", "", "ConfigGrpcXdsTlsConfig";
    config_grpc_xds_server_use_tls: Bool = "CONFIG_GRPC_XDS_SERVER_USE_TLS", "false", "ConfigGrpcXdsServerUseTls";
    config_grpc_xds_client_tls_cert: Str = "CONFIG_GRPC_XDS_CLIENT_TLS_CERT", "", "ConfigGrpcXdsClientTlsCert";
    config_grpc_xds_client_tls_key: Str = "CONFIG_GRPC_XDS_CLIENT_TLS_KEY", "", "ConfigGrpcXdsClientTlsKey";
    config_grpc_xds_server_tls_cacert: Str = "CONFIG_GRPC_XDS_SERVER_TLS_CACERT", "", "ConfigGrpcXdsServerTlsCACert";
    config_grpc_xds_server_tls_san: Str = "CONFIG_GRPC_XDS_SERVER_TLS_SAN", "", "ConfigGrpcXdsServerTlsSAN";
    xds_client_backoff_initial_interval: Duration = "XDS_CLIENT_BACKOFF_INITIAL_INTERVAL", "10s", "XdsClientBackoffInitialInterval";
    xds_client_backoff_max_interval: Duration = "XDS_CLIENT_BACKOFF_MAX_INTERVAL", "60s", "XdsClientBackoffMaxInterval";
    xds_client_backoff_random_factor: Float64 = "XDS_CLIENT_BACKOFF_RANDOM_FACTOR", "0.5", "XdsClientBackoffRandomFactor";
    xds_client_backoff_jitter: Bool = "XDS_CLIENT_BACKOFF_JITTER", "true", "XdsClientBackoffJitter";
    xds_client_max_msg_size_in_bytes: Int = "XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES", "", "XdsClientGrpcOptionsMaxMsgSizeInBytes";
    use_dog_statsd: Bool = "USE_DOG_STATSD", "false", "UseDogStatsd";
    use_dog_statsd_mogrifiers: List = "USE_DOG_STATSD_MOGRIFIERS", "", "UseDogStatsdMogrifiers";
    use_statsd: Bool = "USE_STATSD", "true", "UseStatsd";
    statsd_host: Str = "STATSD_HOST", "localhost", "StatsdHost";
    statsd_port: Int = "STATSD_PORT", "8125", "StatsdPort";
    extra_tags: Map = "EXTRA_TAGS", "", "ExtraTags";
    stats_flush_interval: Duration = "STATS_FLUSH_INTERVAL", "10s", "StatsFlushInterval";
    disable_stats: Bool = "DISABLE_STATS", "false", "DisableStats";
    sanitize_descriptor_metric_dots: Bool = "SANITIZE_DESCRIPTOR_METRIC_DOTS", "false", "SanitizeDescriptorMetricDots";
    use_prometheus: Bool = "USE_PROMETHEUS", "false", "UsePrometheus";
    prometheus_addr: Str = "PROMETHEUS_ADDR", ":9090", "PrometheusAddr";
    prometheus_path: Str = "PROMETHEUS_PATH", "/metrics", "PrometheusPath";
    prometheus_mapper_yaml: Str = "PROMETHEUS_MAPPER_YAML", "", "PrometheusMapperYaml";
    prometheus_response_time_as_milliseconds: Bool = "PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS", "false", "PrometheusResponseTimeAsMilliseconds";
    runtime_root: Str = "RUNTIME_ROOT", "/srv/runtime_data/current", "RuntimePath";
    runtime_subdirectory: Str = "RUNTIME_SUBDIRECTORY", "", "RuntimeSubdirectory";
    runtime_appdirectory: Str = "RUNTIME_APPDIRECTORY", "config", "RuntimeAppDirectory";
    runtime_ignoredotfiles: Bool = "RUNTIME_IGNOREDOTFILES", "false", "RuntimeIgnoreDotFiles";
    runtime_watch_root: Bool = "RUNTIME_WATCH_ROOT", "true", "RuntimeWatchRoot";
    expiration_jitter_max_seconds: Int64 = "EXPIRATION_JITTER_MAX_SECONDS", "300", "ExpirationJitterMaxSeconds";
    local_cache_size_in_bytes: Int = "LOCAL_CACHE_SIZE_IN_BYTES", "0", "LocalCacheSizeInBytes";
    near_limit_ratio: Float32 = "NEAR_LIMIT_RATIO", "0.8", "NearLimitRatio";
    cache_key_prefix: Str = "CACHE_KEY_PREFIX", "", "CacheKeyPrefix";
    backend_type: Str = "BACKEND_TYPE", "redis", "BackendType";
    stop_cache_key_increment_when_overlimit: Bool = "STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT", "false", "StopCacheKeyIncrementWhenOverlimit";
    use_calendar_month_rate_limit: Bool = "USE_CALENDAR_MONTH_RATE_LIMIT", "false", "UseCalendarMonthRateLimit";
    limit_response_headers_enabled: Bool = "LIMIT_RESPONSE_HEADERS_ENABLED", "false", "RateLimitResponseHeadersEnabled";
    limit_limit_header: Str = "LIMIT_LIMIT_HEADER", "RateLimit-Limit", "HeaderRatelimitLimit";
    limit_remaining_header: Str = "LIMIT_REMAINING_HEADER", "RateLimit-Remaining", "HeaderRatelimitRemaining";
    limit_reset_header: Str = "LIMIT_RESET_HEADER", "RateLimit-Reset", "HeaderRatelimitReset";
    limit_per_unit_headers_enabled: Bool = "LIMIT_PER_UNIT_HEADERS_ENABLED", "false", "RateLimitPerUnitHeadersEnabled";
    limit_request_headers_enabled: Bool = "LIMIT_REQUEST_HEADERS_ENABLED", "false", "RateLimitRequestHeadersEnabled";
    limit_request_limit_header: Str = "LIMIT_REQUEST_LIMIT_HEADER", "RateLimit-Limit", "HeaderRequestRatelimitLimit";
    limit_request_remaining_header: Str = "LIMIT_REQUEST_REMAINING_HEADER", "RateLimit-Remaining", "HeaderRequestRatelimitRemaining";
    limit_request_reset_header: Str = "LIMIT_REQUEST_RESET_HEADER", "RateLimit-Reset", "HeaderRequestRatelimitReset";
    healthy_with_at_least_one_config_loaded: Bool = "HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED", "false", "HealthyWithAtLeastOneConfigLoaded";
    redis_socket_type: Str = "REDIS_SOCKET_TYPE", "unix", "RedisSocketType";
    redis_type: Str = "REDIS_TYPE", "SINGLE", "RedisType";
    redis_url: Str = "REDIS_URL", "/var/run/nutcracker/ratelimit.sock", "RedisUrl";
    redis_pool_size: Int = "REDIS_POOL_SIZE", "10", "RedisPoolSize";
    redis_auth: Str = "REDIS_AUTH", "", "RedisAuth";
    redis_auth_file: Str = "REDIS_AUTH_FILE", "", "RedisAuthFile";
    redis_aws_iam_auth: Bool = "REDIS_AWS_IAM_AUTH", "false", "RedisAwsIamAuth";
    redis_aws_iam_cache_name: Str = "REDIS_AWS_IAM_CACHE_NAME", "", "RedisAwsIamCacheName";
    redis_aws_iam_user_id: Str = "REDIS_AWS_IAM_USER_ID", "", "RedisAwsIamUserId";
    redis_aws_iam_serverless: Bool = "REDIS_AWS_IAM_SERVERLESS", "false", "RedisAwsIamServerless";
    redis_aws_iam_region: Str = "REDIS_AWS_IAM_REGION", "", "RedisAwsIamRegion";
    redis_tls: Bool = "REDIS_TLS", "false", "RedisTls";
    redis_tls_config: TlsConfig = "REDISTLSCONFIG", "", "RedisTlsConfig";
    redis_tls_client_cert: Str = "REDIS_TLS_CLIENT_CERT", "", "RedisTlsClientCert";
    redis_tls_client_key: Str = "REDIS_TLS_CLIENT_KEY", "", "RedisTlsClientKey";
    redis_tls_cacert: Str = "REDIS_TLS_CACERT", "", "RedisTlsCACert";
    redis_tls_skip_hostname_verification: Bool = "REDIS_TLS_SKIP_HOSTNAME_VERIFICATION", "false", "RedisTlsSkipHostnameVerification";
    redis_pipeline_window: Duration = "REDIS_PIPELINE_WINDOW", "0", "RedisPipelineWindow";
    redis_pipeline_limit: Int = "REDIS_PIPELINE_LIMIT", "0", "RedisPipelineLimit";
    redis_cluster_pipeline_parallelism: Int = "REDIS_CLUSTER_PIPELINE_PARALLELISM", "1", "RedisClusterPipelineParallelism";
    redis_persecond: Bool = "REDIS_PERSECOND", "false", "RedisPerSecond";
    redis_persecond_socket_type: Str = "REDIS_PERSECOND_SOCKET_TYPE", "unix", "RedisPerSecondSocketType";
    redis_persecond_type: Str = "REDIS_PERSECOND_TYPE", "SINGLE", "RedisPerSecondType";
    redis_persecond_url: Str = "REDIS_PERSECOND_URL", "/var/run/nutcracker/ratelimitpersecond.sock", "RedisPerSecondUrl";
    redis_persecond_pool_size: Int = "REDIS_PERSECOND_POOL_SIZE", "10", "RedisPerSecondPoolSize";
    redis_persecond_auth: Str = "REDIS_PERSECOND_AUTH", "", "RedisPerSecondAuth";
    redis_persecond_auth_file: Str = "REDIS_PERSECOND_AUTH_FILE", "", "RedisPerSecondAuthFile";
    redis_persecond_aws_iam_auth: Bool = "REDIS_PERSECOND_AWS_IAM_AUTH", "false", "RedisPerSecondAwsIamAuth";
    redis_persecond_aws_iam_cache_name: Str = "REDIS_PERSECOND_AWS_IAM_CACHE_NAME", "", "RedisPerSecondAwsIamCacheName";
    redis_persecond_aws_iam_user_id: Str = "REDIS_PERSECOND_AWS_IAM_USER_ID", "", "RedisPerSecondAwsIamUserId";
    redis_persecond_aws_iam_serverless: Bool = "REDIS_PERSECOND_AWS_IAM_SERVERLESS", "false", "RedisPerSecondAwsIamServerless";
    redis_persecond_aws_iam_region: Str = "REDIS_PERSECOND_AWS_IAM_REGION", "", "RedisPerSecondAwsIamRegion";
    redis_persecond_tls: Bool = "REDIS_PERSECOND_TLS", "false", "RedisPerSecondTls";
    redis_sentinel_auth: Str = "REDIS_SENTINEL_AUTH", "", "RedisSentinelAuth";
    redis_persecond_sentinel_auth: Str = "REDIS_PERSECOND_SENTINEL_AUTH", "", "RedisPerSecondSentinelAuth";
    redis_persecond_pipeline_window: Duration = "REDIS_PERSECOND_PIPELINE_WINDOW", "0", "RedisPerSecondPipelineWindow";
    redis_persecond_pipeline_limit: Int = "REDIS_PERSECOND_PIPELINE_LIMIT", "0", "RedisPerSecondPipelineLimit";
    redis_persecond_cluster_pipeline_parallelism: Int = "REDIS_PERSECOND_CLUSTER_PIPELINE_PARALLELISM", "1", "RedisPerSecondClusterPipelineParallelism";
    redis_health_check_active_connection: Bool = "REDIS_HEALTH_CHECK_ACTIVE_CONNECTION", "false", "RedisHealthCheckActiveConnection";
    redis_close_connection_on_readonly_error: Bool = "REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR", "false", "RedisCloseConnectionOnReadOnlyError";
    redis_timeout: Duration = "REDIS_TIMEOUT", "10s", "RedisTimeout";
    redis_persecond_timeout: Duration = "REDIS_PERSECOND_TIMEOUT", "10s", "RedisPerSecondTimeout";
    redis_pool_on_empty_behavior: Str = "REDIS_POOL_ON_EMPTY_BEHAVIOR", "WAIT", "RedisPoolOnEmptyBehavior";
    redis_persecond_pool_on_empty_behavior: Str = "REDIS_PERSECOND_POOL_ON_EMPTY_BEHAVIOR", "WAIT", "RedisPerSecondPoolOnEmptyBehavior";
    redis_startup_initial_interval: Duration = "REDIS_STARTUP_INITIAL_INTERVAL", "1s", "RedisStartupInitialInterval";
    redis_startup_max_interval: Duration = "REDIS_STARTUP_MAX_INTERVAL", "30s", "RedisStartupMaxInterval";
    redis_startup_max_elapsed_time: Duration = "REDIS_STARTUP_MAX_ELAPSED_TIME", "0", "RedisStartupMaxElapsedTime";
    memcache_host_port: List = "MEMCACHE_HOST_PORT", "", "MemcacheHostPort";
    memcache_max_idle_conns: Int = "MEMCACHE_MAX_IDLE_CONNS", "2", "MemcacheMaxIdleConns";
    memcache_srv: Str = "MEMCACHE_SRV", "", "MemcacheSrv";
    memcache_srv_refresh: Duration = "MEMCACHE_SRV_REFRESH", "0", "MemcacheSrvRefresh";
    memcache_tls: Bool = "MEMCACHE_TLS", "false", "MemcacheTls";
    memcache_tls_config: TlsConfig = "MEMCACHETLSCONFIG", "", "MemcacheTlsConfig";
    memcache_tls_client_cert: Str = "MEMCACHE_TLS_CLIENT_CERT", "", "MemcacheTlsClientCert";
    memcache_tls_client_key: Str = "MEMCACHE_TLS_CLIENT_KEY", "", "MemcacheTlsClientKey";
    memcache_tls_cacert: Str = "MEMCACHE_TLS_CACERT", "", "MemcacheTlsCACert";
    memcache_tls_skip_hostname_verification: Bool = "MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION", "false", "MemcacheTlsSkipHostnameVerification";
    shadow_mode: Bool = "SHADOW_MODE", "false", "GlobalShadowMode";
    quota_mode: Bool = "QUOTA_MODE", "false", "GlobalQuotaMode";
    response_dynamic_metadata: Bool = "RESPONSE_DYNAMIC_METADATA", "false", "ResponseDynamicMetadata";
    merge_domain_config: Bool = "MERGE_DOMAIN_CONFIG", "false", "MergeDomainConfigurations";
    tracing_enabled: Bool = "TRACING_ENABLED", "false", "TracingEnabled";
    tracing_service_name: Str = "TRACING_SERVICE_NAME", "RateLimit", "TracingServiceName";
    tracing_service_namespace: Str = "TRACING_SERVICE_NAMESPACE", "", "TracingServiceNamespace";
    tracing_service_instance_id: Str = "TRACING_SERVICE_INSTANCE_ID", "", "TracingServiceInstanceId";
    tracing_exporter_protocol: Str = "TRACING_EXPORTER_PROTOCOL", "http", "TracingExporterProtocol";
    tracing_sampling_rate: Float64 = "TRACING_SAMPLING_RATE", "1", "TracingSamplingRate";
}

impl Settings {
    /// The settings of this process's environment.
    pub fn from_env() -> Result<Self, SettingsError> {
        Self::from_lookup(&|key| std::env::var_os(key).map(|v| v.to_string_lossy().into_owned()))
    }
}

impl Default for Settings {
    /// Upstream's defaults: the settings of an empty environment.
    fn default() -> Self {
        Self::from_lookup(&|_| None).expect("the defaults are valid")
    }
}

impl Settings {
    /// What the Redis cache takes from the settings.
    pub fn cache(&self) -> CacheSettings {
        CacheSettings {
            cache_key_prefix: self.cache_key_prefix.clone(),
            // freecache raises any size below its minimum to that minimum, negative included.
            local_cache_size_in_bytes: match self.local_cache_size_in_bytes {
                0 => 0,
                size => usize::try_from(size).unwrap_or(1),
            },
            stop_cache_key_increment_when_overlimit: self.stop_cache_key_increment_when_overlimit,
            near_limit_ratio: self.near_limit_ratio,
            expiration_jitter_max_seconds: self.expiration_jitter_max_seconds,
            use_calendar_month: self.use_calendar_month_rate_limit,
            global_shadow_mode: self.shadow_mode,
        }
    }

    /// What the service takes from the settings.
    pub fn service(&self) -> ServiceSettings {
        let headers = |enabled: bool, limit: &str, remaining: &str, reset: &str| {
            enabled.then(|| LimitHeaders {
                limit: limit.to_string(),
                remaining: remaining.to_string(),
                reset: reset.to_string(),
            })
        };
        ServiceSettings {
            use_calendar_month: self.use_calendar_month_rate_limit,
            global_shadow_mode: self.shadow_mode,
            global_quota_mode: self.quota_mode,
            response_headers: headers(
                self.limit_response_headers_enabled,
                &self.limit_limit_header,
                &self.limit_remaining_header,
                &self.limit_reset_header,
            ),
            request_headers: headers(
                self.limit_request_headers_enabled,
                &self.limit_request_limit_header,
                &self.limit_request_remaining_header,
                &self.limit_request_reset_header,
            ),
            per_unit_headers: self.limit_per_unit_headers_enabled,
            response_dynamic_metadata: self.response_dynamic_metadata,
        }
    }

    /// What loading config files takes from the settings.
    pub fn load_options(&self) -> LoadOptions {
        LoadOptions {
            merge_domain_configs: self.merge_domain_config,
            sanitize_descriptor_metric_dots: self.sanitize_descriptor_metric_dots,
            memcached: self.backend_type == "memcache",
            cache_key_prefix: self.cache_key_prefix.clone(),
            global_quota_mode: self.quota_mode,
        }
    }

    /// `STATS_FLUSH_INTERVAL`, which Go's ticker refuses unless positive.
    pub fn stats_flush_interval(&self) -> Result<std::time::Duration, String> {
        u64::try_from(self.stats_flush_interval)
            .ok()
            .filter(|nanos| *nanos > 0)
            .map(std::time::Duration::from_nanos)
            .ok_or_else(|| "non-positive interval for NewTicker".to_string())
    }

    /// What selecting the stats sink takes from the settings, and from the variables the
    /// sinks' libraries read themselves: gostats (`STATSD_PROTOCOL`, and its own checks of
    /// `USE_STATSD`, `STATSD_PORT` and `GOSTATS_*` when the statsd sink is used), datadog-go
    /// (`DD_*`) and the DogStatsD mogrifiers (read only when that sink is used, as upstream).
    pub fn sinks(&self, lookup: Lookup) -> Result<SinkSettings, String> {
        let get = |key: &str| lookup(key).unwrap_or_default();
        let several = [self.use_dog_statsd, self.use_statsd, self.use_prometheus]
            .iter()
            .filter(|on| **on)
            .count()
            > 1;
        let selected = !self.disable_stats && !several;
        if selected && self.use_statsd {
            gostats_checks(lookup)?;
        }
        let mogrifiers = if selected && self.use_dog_statsd {
            mogrifiers(&self.use_dog_statsd_mogrifiers, lookup)?
        } else {
            Vec::new()
        };
        if selected && self.use_dog_statsd && u16::try_from(self.statsd_port).is_err() {
            return Err(format!(
                "Failed to create dogstatsd sink: address {}: invalid port",
                self.statsd_port
            ));
        }
        // Off only for a valid false value.
        let origin_detection = go::parse_bool(&get("DD_ORIGIN_DETECTION_ENABLED")) != Ok(false);
        // A port Go cannot dial never connects; 0 cannot be dialled either.
        let statsd_port = u16::try_from(self.statsd_port).unwrap_or(0);
        Ok(SinkSettings {
            disable_stats: self.disable_stats,
            use_statsd: self.use_statsd,
            use_dog_statsd: self.use_dog_statsd,
            use_prometheus: self.use_prometheus,
            statsd_host: self.statsd_host.clone(),
            statsd_port,
            statsd_protocol: match get("STATSD_PROTOCOL").as_str() {
                "udp" | "udp4" | "udp6" => StatsdProtocol::Udp,
                _ => StatsdProtocol::Tcp,
            },
            dogstatsd: DogStatsdSettings {
                entity_id: get("DD_ENTITY_ID"),
                env: get("DD_ENV"),
                service: get("DD_SERVICE"),
                version: get("DD_VERSION"),
                origin_detection,
                mogrifiers,
            },
            prometheus: PrometheusSettings {
                addr: self.prometheus_addr.clone(),
                path: self.prometheus_path.clone(),
                mapper_yaml: self.prometheus_mapper_yaml.clone(),
                response_time_as_milliseconds: self.prometheus_response_time_as_milliseconds,
            },
        })
    }
}

/// gostats' `GetSettings`, which the statsd sink calls: it re-reads four variables with its
/// own parsers (`STATSD_PORT` in decimal only) and panics on an invalid one.
fn gostats_checks(lookup: Lookup) -> Result<(), String> {
    let check = |key: &str, parse: fn(&str) -> Result<(), String>| match lookup(key)
        .filter(|value| !value.is_empty())
    {
        Some(value) => parse(&value).map_err(|e| {
            format!(
                "parsing environment variable: {} with value: {}: {e}",
                go::quote(key),
                go::quote(&value)
            )
        }),
        None => Ok(()),
    };
    let boolean = |v: &str| go::parse_bool(v).map(|_| ());
    let integer = |v: &str| go::atoi(v).map(|_| ());
    check("USE_STATSD", boolean)?;
    check("STATSD_PORT", integer)?;
    check("GOSTATS_FLUSH_INTERVAL_SECONDS", integer)?;
    check("GOSTATS_LOGGING_SINK_DISABLED", boolean)
}

/// `newMogrifierMapFromEnv`: each mogrifier's `DOG_STATSD_MOGRIFIER_<NAME>_PATTERN`, `_TAGS`
/// and `_NAME` read by envconfig, which falls back to the bare `PATTERN`, `TAGS` and `NAME`.
fn mogrifiers(names: &[String], lookup: Lookup) -> Result<Vec<Mogrifier>, String> {
    names
        .iter()
        .map(|name| {
            let prefix = go::to_upper(&format!("DOG_STATSD_MOGRIFIER_{name}"));
            let field = |suffix: &str| {
                let key = format!("{prefix}_{suffix}");
                (lookup(&key).or_else(|| lookup(suffix)), key)
            };
            let (pattern, _) = field("PATTERN");
            let (tags, tags_key) = field("TAGS");
            let (template, _) = field("NAME");
            let tags = match tags {
                Some(value) => Map::parse(&value).map_err(|e| {
                    let error = parse_error(&tags_key, "Tags", &value, Map::GO_TYPE, &e);
                    format!("failed to load mogrifier {name}: {error}")
                })?,
                None => BTreeMap::new(),
            };
            Mogrifier::new(
                name,
                &pattern.unwrap_or_default(),
                &template.unwrap_or_default(),
                tags,
            )
        })
        .collect()
}
