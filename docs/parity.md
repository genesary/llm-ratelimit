# Parity inventory with envoy ratelimit

Reference: `envoyproxy/ratelimit` at commit `f4e7669` (2026-10-06). Rule (D3): the first version ships only when **every line** is `✅` with a test proving it, or `≠` (deliberate, justified divergence). Deliberate divergences are listed at the end.

Status legend: `⬜` to do · `🚧` in progress · `✅` done and proven · `≠` deliberate divergence · `—` ignored, as upstream.

Section 1 is **generated** from `src/settings/settings.go` (134 variables); regenerate it whenever the reference is upgraded.

## 1. Environment variables

Reading them (values, defaults, set-but-empty values, the first error and its text, including the `<SETTING>_<FIELD>` variables envconfig reads for each untagged `*tls.Config` setting, such as `REDISTLSCONFIG_MINVERSION`) is proven for every variable by `settings_are_read_like_upstream` in `tests/differential.rs`, against upstream's `settings.NewSettings()`. The rows below track what each one does.

| Variable | Upstream default | Status | Test |
|---|---|---|---|
| `HOST` | `0.0.0.0` | ✅ | `tests/differential.rs` (`the_healthcheck_answers_like_upstream`), listening like `GRPC_HOST` |
| `PORT` | `8080` | ✅ | `tests/differential.rs` (`the_healthcheck_answers_like_upstream`, `the_binary_*`) |
| `DEBUG_HOST` | `0.0.0.0` | ✅ | `tests/differential.rs` (`the_debug_port_answers_like_upstream`) |
| `DEBUG_PORT` | `6070` | ✅ | `tests/differential.rs` (`the_debug_port_answers_like_upstream`, `a_busy_debug_port_is_ignored_like_upstream`) |
| `GRPC_UDS` | — | ✅ | `the_binary_serves_on_a_unix_socket_like_upstream` in `tests/differential.rs` |
| `GRPC_HOST` | `0.0.0.0` | ✅ | `tests/differential.rs` (`the_binary_*`), listening like Go (dual-stack wildcard, `SO_REUSEPORT`) |
| `GRPC_PORT` | `8081` | ✅ | `tests/differential.rs` (`the_binary_*`); a busy port fails like upstream |
| `GRPC_MAX_CONNECTION_AGE` | `24h` | ✅ | applied to the gRPC server (tonic's keepalive settings) |
| `GRPC_MAX_CONNECTION_AGE_GRACE` | `1h` | ✅ | applied to the gRPC server (tonic's keepalive settings) |
| `GRPC_SERVER_USE_TLS` | `false` | ✅ | gRPC over TLS, only for clients negotiating `h2`, plaintext clients closed without an alert, on TCP or `GRPC_UDS`: `grpc_over_tls_is_served_like_upstream`, `the_binary_serves_on_a_unix_socket_like_upstream` |
| `GRPC_SERVER_TLS_CERT` | — | ✅ | the cert provider's key pair, with `GRPC_SERVER_TLS_KEY` (same directory), loaded with the settings and then by the provider, reloaded on changes: `grpc_over_tls_is_served_like_upstream`, `grpc_certificates_are_reloaded_like_upstream`; failures: `the_binary_fails_to_start_like_upstream` |
| `GRPC_SERVER_TLS_KEY` | — | ✅ | as `GRPC_SERVER_TLS_CERT` |
| `GRPC_CLIENT_TLS_CACERT` | — | ✅ | a client certificate required, issued by this CA (not the system's roots, §7): `grpc_over_tls_is_served_like_upstream`; an unreadable file: `the_binary_fails_to_start_like_upstream` |
| `GRPC_CLIENT_TLS_SAN` | — | ✅ | with a CA, the client's certificate valid for this name (DNS name or IP), a mismatch logged: `grpc_over_tls_is_served_like_upstream` |
| `LOG_LEVEL` | `WARN` | ✅ | `tests/differential.rs` (`the_binary_fails_to_start_like_upstream`, `logs_are_written_like_upstream`), `log.rs` unit tests |
| `LOG_FORMAT` | `text` | ✅ | `logs_are_written_like_upstream` in `tests/differential.rs` |
| `CONFIG_TYPE` | `FILE` | ✅ | `FILE`, and an invalid value: `the_binary_fails_to_start_like_upstream`; `GRPC_XDS_SOTW`: the `xds_*_like_upstream` tests |
| `FORCE_START_WITHOUT_INITIAL_CONFIG` | `false` | ✅ | the service waits for the provider's first config unless set (the end state is the same, as upstream); with xDS, no service while the server is unreachable: `xds_configs_are_acked_like_upstream`, and served meanwhile when set: `xds_streams_fail_like_upstream` |
| `CONFIG_GRPC_XDS_NODE_ID` | `default` | ✅ | the node's ID in every request: `xds_configs_are_acked_like_upstream` |
| `CONFIG_GRPC_XDS_NODE_METADATA` | — | ✅ | the node's metadata: its `KEY=VALUE` entries (split at the first `=`), others skipped: `xds_configs_are_acked_like_upstream` |
| `CONFIG_GRPC_XDS_SERVER_URL` | `localhost:18000` | ✅ | dialled as grpc-go dials it (blocking, its connection backoff), connected once the server speaks: the `xds_*_like_upstream` tests; `dns:///`, `passthrough:///` and `unix:` targets and the `:authority`: `xds.rs` unit tests |
| `CONFIG_GRPC_XDS_SERVER_CONNECT_RETRY_INTERVAL` | `3s` | ✅ | read, no effect, as upstream (its retry interceptor retries nothing by default) |
| `CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS` | — | ✅ | sent with each stream, keys lowercased (`-bin` values in base64): `xds_configs_are_acked_like_upstream`; a key or value grpc-go refuses fails the stream: `xds_streams_fail_like_upstream` |
| `CONFIG_GRPC_XDS_SERVER_USE_TLS` | `false` | ✅ | the server verified, `h2` negotiated unless `GRPC_ENFORCE_ALPN_ENABLED=false`, a failed handshake dialled again silently: `xds_over_tls_like_upstream`; its files read with the settings whatever `CONFIG_TYPE` (`the_binary_fails_to_start_like_upstream`) and again at each response, as every TLS file of the settings (`xds_responses_reread_the_tls_files_like_upstream`) |
| `CONFIG_GRPC_XDS_CLIENT_TLS_CERT` | — | ✅ | with `CONFIG_GRPC_XDS_CLIENT_TLS_KEY`, the client's key pair: `xds_over_tls_like_upstream`, `the_binary_fails_to_start_like_upstream` |
| `CONFIG_GRPC_XDS_CLIENT_TLS_KEY` | — | ✅ | as `CONFIG_GRPC_XDS_CLIENT_TLS_CERT` |
| `CONFIG_GRPC_XDS_SERVER_TLS_CACERT` | — | ✅ | added to the system's roots: `xds_over_tls_like_upstream`, `the_binary_fails_to_start_like_upstream` |
| `CONFIG_GRPC_XDS_SERVER_TLS_SAN` | — | ✅ | the name the server's certificate is verified for (else the URL's host), logged at each dial: `xds_over_tls_like_upstream` |
| `XDS_CLIENT_BACKOFF_INITIAL_INTERVAL` | `10s` | ✅ | jpillora/backoff's, never reset, between streams: `xds_configs_are_acked_like_upstream`; `xds.rs` unit tests |
| `XDS_CLIENT_BACKOFF_MAX_INTERVAL` | `60s` | ✅ | as `XDS_CLIENT_BACKOFF_INITIAL_INTERVAL` |
| `XDS_CLIENT_BACKOFF_RANDOM_FACTOR` | `0.5` | ✅ | as `XDS_CLIENT_BACKOFF_INITIAL_INTERVAL` (the factor: the default `0.5` never grows) |
| `XDS_CLIENT_BACKOFF_JITTER` | `true` | ✅ | as `XDS_CLIENT_BACKOFF_INITIAL_INTERVAL` |
| `XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES` | — | ✅ | grpc-go's limit (4 MiB when unset): a larger response fails the stream: `xds_streams_fail_like_upstream` |
| `USE_DOG_STATSD` | `false` | ✅ | behaviour: `tests/differential.rs`, `tests/dogstatsd.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `USE_DOG_STATSD_MOGRIFIERS` | — | ✅ | behaviour: `tests/differential.rs`, `tests/dogstatsd.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `DOG_STATSD_MOGRIFIER_<NAME>_PATTERN`, `_NAME`, `_TAGS` | — | ✅ | behaviour: `tests/differential.rs`, `tests/dogstatsd.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `DD_ENTITY_ID`, `DD_ENV`, `DD_SERVICE`, `DD_VERSION`, `DD_ORIGIN_DETECTION_ENABLED` | — (read by datadog-go, not by `settings.go`) | ✅ | behaviour: `tests/differential.rs`, `tests/dogstatsd.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `USE_STATSD` | `true` | ✅ | behaviour: `tests/stats.rs` (selection); read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `STATSD_HOST` | `localhost` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `STATSD_PORT` | `8125` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `EXTRA_TAGS` | — | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) (a value containing `:` makes upstream panic at startup) |
| `STATS_FLUSH_INTERVAL` | `10s` | ✅ | behaviour: `tests/stats.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `DISABLE_STATS` | `false` | ✅ | behaviour: `tests/stats.rs` (selection); read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `STATSD_PROTOCOL` | `tcp` (read by gostats, not by `settings.go`) | ✅ | behaviour (`tcp`, `udp`): `tests/differential.rs`, `tests/stats.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) (gostats also accepts `udp4`, `udp6` and any other network name) |
| `GOSTATS_FLUSH_INTERVAL_SECONDS`, `GOSTATS_LOGGING_SINK_DISABLED` | `5`, `false` (read by gostats, not by `settings.go`) | ✅ | only checked (an invalid value fails when the statsd sink is created), as upstream: `tests/differential.rs` |
| `SANITIZE_DESCRIPTOR_METRIC_DOTS` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `USE_PROMETHEUS` | `false` | ✅ | behaviour: `tests/stats.rs`, `tests/prometheus.rs` (selection); read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `PROMETHEUS_ADDR` | `:9090` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `PROMETHEUS_PATH` | `/metrics` | ✅ | behaviour: `tests/differential.rs`, `tests/prometheus.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `PROMETHEUS_MAPPER_YAML` | — | ✅ | behaviour: `tests/differential.rs`, `tests/prometheus.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `RUNTIME_ROOT` | `/srv/runtime_data/current` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `RUNTIME_SUBDIRECTORY` | — | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `RUNTIME_APPDIRECTORY` | `config` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `RUNTIME_IGNOREDOTFILES` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `RUNTIME_WATCH_ROOT` | `true` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `EXPIRATION_JITTER_MAX_SECONDS` | `300` | ✅ | `expirations_are_jittered_like_upstream` (the extras' range; 0 is none) |
| `LOCAL_CACHE_SIZE_IN_BYTES` | `0` | ✅ | behaviour: section 4; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `NEAR_LIMIT_RATIO` | `0.8` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `CACHE_KEY_PREFIX` | — | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `BACKEND_TYPE` | `redis` | ✅ | `redis`, and an invalid value: `the_binary_fails_to_start_like_upstream`; `memcache`: `memcached_serves_like_upstream` |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` | `false` | ✅ | behaviour: section 4; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `USE_CALENDAR_MONTH_RATE_LIMIT` | `false` | ✅ | `calendar_months_are_counted_like_upstream`: keys, TTLs, resets and the reset header across short, leap and year-end months |
| `LIMIT_RESPONSE_HEADERS_ENABLED` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_LIMIT_HEADER` | `RateLimit-Limit` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_REMAINING_HEADER` | `RateLimit-Remaining` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_RESET_HEADER` | `RateLimit-Reset` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_PER_UNIT_HEADERS_ENABLED` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_REQUEST_HEADERS_ENABLED` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_REQUEST_LIMIT_HEADER` | `RateLimit-Limit` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_REQUEST_REMAINING_HEADER` | `RateLimit-Remaining` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_REQUEST_RESET_HEADER` | `RateLimit-Reset` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED` | `false` | ✅ | `health_is_reported_like_upstream` in `tests/differential.rs` |
| `REDIS_SOCKET_TYPE` | `unix` | ✅ | `tcp`: `tests/differential.rs`; `unix`: `tests/runner.rs` |
| `REDIS_TYPE` | `SINGLE` | ✅ | `SINGLE`, and an invalid value: `tests/runner.rs`, `the_binary_fails_to_start_like_upstream`; `CLUSTER` (any case): `a_redis_cluster_serves_like_upstream`, `a_redis_cluster_over_tls_serves_like_upstream`, its startup lines and failures: `redis_startup_is_logged_like_upstream`, `the_binary_fails_to_start_like_upstream`; `SENTINEL` (any case): `a_redis_sentinel_serves_like_upstream`, `sentinel_startup_is_logged_like_upstream` |
| `REDIS_URL` | `/var/run/nutcracker/ratelimit.sock` | ✅ | `tests/differential.rs`; `redis://` URLs (user, password, database): `tests/runner.rs` |
| `REDIS_POOL_SIZE` | `10` | ✅ | `tests/runner.rs` (0 is radix's 4) |
| `REDIS_AUTH` | — | ✅ | `user:password` or a password: `tests/runner.rs` |
| `REDIS_AUTH_FILE` | — | ✅ | read at each connection (trimmed; missing or empty, the dial fails; with `REDIS_AUTH`, refused): `a_password_protected_redis_is_logged_like_upstream`, `auth_files_are_reread_like_upstream`, `the_binary_fails_to_start_like_upstream` |
| `REDIS_AWS_IAM_AUTH` | `false` | ✅ | each connection authenticates as the user with a presigned token, reused 14 min (`elasticache_iam_tokens_authenticate_like_upstream`, `expiring_aws_credentials_are_refreshed_like_upstream`); the AWS credentials found as Go's SDK finds them (`aws_credentials_are_found_like_upstream`); with `REDIS_AUTH` or `REDIS_AUTH_FILE`, and the AWS configuration's failures, refused at startup (`the_binary_fails_to_start_like_upstream`); the signature: `redis/aws.rs` unit test |
| `REDIS_AWS_IAM_CACHE_NAME` | — | ✅ | the token's host, lowercased; required (`the_binary_fails_to_start_like_upstream`) |
| `REDIS_AWS_IAM_USER_ID` | — | ✅ | the `AUTH` user and the token's `User`; required |
| `REDIS_AWS_IAM_SERVERLESS` | `false` | ✅ | `ResourceType=ServerlessCache` in the token |
| `REDIS_AWS_IAM_REGION` | — | ✅ | the token's region; else the AWS configuration's (`AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's); none refused at startup |
| `REDIS_TLS` | `false` | ✅ | `redis_over_tls_is_used_like_upstream` (its warning, the handshake within the dial, failures retried) |
| `REDIS_TLS_CLIENT_CERT` | — | ✅ | `redis_over_tls_is_used_like_upstream`; a file that does not load: `the_binary_fails_to_start_like_upstream` |
| `REDIS_TLS_CLIENT_KEY` | — | ✅ | `redis_over_tls_is_used_like_upstream`; a file that does not load: `the_binary_fails_to_start_like_upstream` |
| `REDIS_TLS_CACERT` | — | ✅ | added to the system's roots: `redis_over_tls_is_used_like_upstream`; a file that does not load or has no certificate: `the_binary_fails_to_start_like_upstream` |
| `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION` | `false` | ✅ | the chain verified, not the name: `redis_over_tls_is_used_like_upstream` |
| `REDIS_PIPELINE_WINDOW` | `0` | ✅ | no effect outside `CLUSTER` mode, as upstream (radix v4's write buffering is cluster-only); in `CLUSTER` mode, upstream's debug line; its buffering not reproduced (§7) |
| `REDIS_PIPELINE_LIMIT` | `0` | ✅ | no effect; upstream's deprecation warning: `redis_startup_is_logged_like_upstream` |
| `REDIS_CLUSTER_PIPELINE_PARALLELISM` | `1` | ✅ | in `CLUSTER` mode, bounded by `REDIS_POOL_SIZE` and logged as upstream (`a_redis_cluster_serves_like_upstream`, `redis/connection.rs` unit test); negative refused at startup (`the_binary_fails_to_start_like_upstream`); the pipelines' grouping is fred's (§7) |
| `REDIS_PERSECOND` | `false` | ✅ | `per_second_limits_use_their_own_redis_like_upstream`, `a_per_second_redis_starts_like_upstream` |
| `REDIS_PERSECOND_SOCKET_TYPE` | `unix` | ✅ | `per_second_limits_use_their_own_redis_like_upstream`, `a_per_second_redis_starts_like_upstream`; `the_per_second_pool_has_its_own_settings` in `tests/redis_connection.rs` |
| `REDIS_PERSECOND_TYPE` | `SINGLE` | ✅ | `SINGLE`: `the_per_second_pool_has_its_own_settings` in `tests/redis_connection.rs`; `CLUSTER`: `a_redis_cluster_serves_like_upstream`; `SENTINEL`: `a_redis_sentinel_serves_like_upstream` |
| `REDIS_PERSECOND_URL` | `/var/run/nutcracker/ratelimitpersecond.sock` | ✅ | `per_second_limits_use_their_own_redis_like_upstream`, `a_per_second_redis_starts_like_upstream`; `the_per_second_pool_has_its_own_settings` in `tests/redis_connection.rs` |
| `REDIS_PERSECOND_POOL_SIZE` | `10` | ✅ | `a_per_second_redis_starts_like_upstream`; `the_per_second_pool_has_its_own_settings` in `tests/redis_connection.rs` |
| `REDIS_PERSECOND_AUTH` | — | ✅ | `a_per_second_redis_starts_like_upstream`; `the_per_second_pool_has_its_own_settings` in `tests/redis_connection.rs` |
| `REDIS_PERSECOND_AUTH_FILE` | — | ✅ | as `REDIS_AUTH_FILE`, for the per-second pool: `the_per_second_pool_has_its_own_settings` in `tests/redis_connection.rs` |
| `REDIS_PERSECOND_AWS_IAM_AUTH` | `false` | ✅ | as `REDIS_AWS_IAM_AUTH`, for the per-second pool: `elasticache_iam_tokens_authenticate_like_upstream`, `the_binary_fails_to_start_like_upstream` |
| `REDIS_PERSECOND_AWS_IAM_CACHE_NAME` | — | ✅ | as `REDIS_AWS_IAM_CACHE_NAME` |
| `REDIS_PERSECOND_AWS_IAM_USER_ID` | — | ✅ | as `REDIS_AWS_IAM_USER_ID` |
| `REDIS_PERSECOND_AWS_IAM_SERVERLESS` | `false` | ✅ | as `REDIS_AWS_IAM_SERVERLESS` |
| `REDIS_PERSECOND_AWS_IAM_REGION` | — | ✅ | as `REDIS_AWS_IAM_REGION` |
| `REDIS_PERSECOND_TLS` | `false` | ✅ | the same configuration, for the per-second pool; its files loaded even with the pool off: `the_binary_fails_to_start_like_upstream` |
| `REDIS_SENTINEL_AUTH` | — | ✅ | the sentinels' credentials (`user:password` or a password), their dialer's line at each attempt, a refusal retried: `sentinel_startup_is_logged_like_upstream` |
| `REDIS_PERSECOND_SENTINEL_AUTH` | — | ✅ | as `REDIS_SENTINEL_AUTH`, for the per-second pool (the same code) |
| `REDIS_PERSECOND_PIPELINE_WINDOW` | `0` | ✅ | as `REDIS_PIPELINE_WINDOW`, for the per-second pool |
| `REDIS_PERSECOND_PIPELINE_LIMIT` | `0` | ✅ | no effect; the warning, as the main pool's: `the_per_second_pool_has_its_own_settings` in `tests/redis_connection.rs` |
| `REDIS_PERSECOND_CLUSTER_PIPELINE_PARALLELISM` | `1` | ✅ | as `REDIS_CLUSTER_PIPELINE_PARALLELISM`, bounded by `REDIS_PERSECOND_POOL_SIZE`: `a_redis_cluster_serves_like_upstream` |
| `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION` | `false` | ✅ | `redis_connections_drive_health_like_upstream` in `tests/differential.rs` |
| `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR` | `false` | ✅ | `SINGLE`: its warning only, as upstream, whose wrapper does not see the errors of the pipelined commands: `readonly_replies_are_handled_like_upstream`; `CLUSTER` with cluster mode |
| `REDIS_TIMEOUT` | `10s` | ✅ | the connection timeout, as radix's dialer: `tests/runner.rs` |
| `REDIS_PERSECOND_TIMEOUT` | `10s` | ✅ | `the_per_second_pool_has_its_own_settings` in `tests/redis_connection.rs` |
| `REDIS_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | ✅ | `CREATE`/`ERROR` refused at startup: `tests/runner.rs`, `the_binary_fails_to_start_like_upstream` |
| `REDIS_PERSECOND_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | ✅ | `a_per_second_redis_starts_like_upstream` (its warning); `the_per_second_pool_has_its_own_settings` in `tests/redis_connection.rs` |
| `REDIS_STARTUP_INITIAL_INTERVAL` | `1s` | ✅ | `tests/runner.rs` |
| `REDIS_STARTUP_MAX_INTERVAL` | `30s` | ✅ | `tests/runner.rs` |
| `REDIS_STARTUP_MAX_ELAPSED_TIME` | `0` | ✅ | `the_binary_fails_to_start_like_upstream` |
| `MEMCACHE_HOST_PORT` | — | ✅ | the servers, resolved at startup (one that does not resolve: none at all), each key on the one its CRC-32 picks: `memcached_keys_are_sharded_like_upstream`, `memcached_failures_are_logged_like_upstream`; with `MEMCACHE_SRV`, refused (`the_binary_fails_to_start_like_upstream`) |
| `MEMCACHE_MAX_IDLE_CONNS` | `2` | ✅ | the connections kept per server, as gomemcache keeps them: `memcached_keeps_idle_connections_like_gomemcache` |
| `MEMCACHE_SRV` | — | ✅ | the record's `target:port`, sorted (`memcached/srv.rs` unit tests, on a fake DNS); a record that does not parse or resolve logged and refused at startup (`the_binary_fails_to_start_like_upstream`, its lines compared by hand) |
| `MEMCACHE_SRV_REFRESH` | `0` | ✅ | the record read again at that interval, the servers replaced, ticks missed dropped: `memcached/srv.rs` unit test, on a fake DNS |
| `MEMCACHE_TLS` | `false` | ✅ | `memcached_over_tls_is_used_like_upstream`; its files read with the settings whatever the backend, after Redis's: `the_binary_fails_to_start_like_upstream` |
| `MEMCACHE_TLS_CLIENT_CERT` | — | ✅ | `memcached_over_tls_is_used_like_upstream` |
| `MEMCACHE_TLS_CLIENT_KEY` | — | ✅ | `memcached_over_tls_is_used_like_upstream` |
| `MEMCACHE_TLS_CACERT` | — | ✅ | `memcached_over_tls_is_used_like_upstream`, `the_binary_fails_to_start_like_upstream` |
| `MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION` | `false` | ✅ | a server on `[::1]` whose certificate is for 127.0.0.1: `memcached_over_tls_is_used_like_upstream` |
| `SHADOW_MODE` | `false` | ✅ | `tests/service.rs`, `tests/differential.rs`; read by the binary: `tests/settings.rs` |
| `QUOTA_MODE` | `false` | ✅ | `tests/service.rs`, `tests/differential.rs`; read by the binary: `tests/settings.rs` |
| `RESPONSE_DYNAMIC_METADATA` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `MERGE_DOMAIN_CONFIG` | `false` | ✅ | `tests/config.rs`; read by the binary: `tests/settings.rs` |
| `TRACING_ENABLED` | `false` | ✅ | `traces_are_exported_like_upstream`; `Tracing disabled` otherwise |
| `TRACING_SERVICE_NAME` | `RateLimit` | ✅ | `traces_are_exported_like_upstream` (over `OTEL_SERVICE_NAME`) |
| `TRACING_SERVICE_NAMESPACE` | — | ✅ | `traces_are_exported_like_upstream` |
| `TRACING_SERVICE_INSTANCE_ID` | — | ✅ | `traces_are_exported_like_upstream`; a random version 4 UUID by default: `trace/mod.rs` unit test |
| `TRACING_EXPORTER_PROTOCOL` | `http` | ✅ | `traces_are_exported_like_upstream`; an unknown one is fatal: `the_binary_fails_to_start_like_upstream` |
| `TRACING_SAMPLING_RATE` | `1` | ✅ | `traces_are_exported_like_upstream` (1, 0); the ratio's bound: `trace/mod.rs` unit test |
| `GRPC_ENFORCE_ALPN_ENABLED` | (read by grpc-go) | ✅ | `false` (any case) serves TLS clients that negotiate no protocol: `grpc_over_tls_is_served_like_upstream`; and lets the xDS client use such a server |
| `GRPC_SERVER_TLS_CA_CERT` | (injected by Envoy Gateway, read by no upstream code) | — ignored, as upstream | |
| `OTEL_EXPORTER_OTLP_*` | (read by the OpenTelemetry SDK) | ✅ | `ENDPOINT`, `HEADERS`, `COMPRESSION`, `CERTIFICATE`, `CLIENT_CERTIFICATE`, `CLIENT_KEY`: `traces_are_exported_like_upstream`; their `TRACES_` forms, `INSECURE`, `TIMEOUT`, URL and header parsing: `trace/export.rs` unit tests |

## 2. YAML configuration

| Item | Status | Test |
|---|---|---|
| Recognised keys: `domain`, `key`, `value`, `descriptors`, `rate_limit`, `unit`, `requests_per_unit`, `unlimited`, `shadow_mode`, `quota_mode`, `name`, `replaces`, `detailed_metric`, `value_to_metric`, `share_threshold`, `metadata` | ✅ | `tests/config.rs` |
| Unknown key ⇒ config error (recursive allow-list, `metadata` excluded) | ✅ | `tests/config.rs` |
| Units `second` … `year` (case-insensitive), month = 30 d, year = 365 d | ✅ | `tests/config.rs`, `tests/differential.rs` |
| `unlimited` + `unit` ⇒ error; `requests_per_unit: 0` accepted | ✅ | `tests/config.rs` |
| `replaces`: empty name or self-replacement ⇒ error; names need not be unique | ✅ | `tests/config.rs` |
| Matching: exact `key_value`, then trailing **and middle** wildcards, then key only | ✅ | `tests/config.rs` |
| `share_threshold` (pattern in the Redis key and the stats key) | ✅ | `tests/config.rs`, `tests/differential.rs` |
| `detailed_metric`, `value_to_metric` (enriched stats key) | ✅ | `tests/config.rs` |
| Duplicate domain ⇒ error, unless `MERGE_DOMAIN_CONFIG`; duplicate composite key ⇒ error | ✅ | `tests/config.rs` |
| Reload: error ⇒ `config_load_error` + previous config kept | ✅ | `tests/differential.rs` |
| Each config loaded reads the settings' TLS files again (`SetConfig`'s `NewSettings`): one gone stops the service with its error | ✅ | `config_reloads_reread_the_tls_files_like_upstream` (Linux), `xds_responses_reread_the_tls_files_like_upstream` |
| YAML 1.1 input as yaml.v2 reads it: control characters refused, byte order marks (UTF-8, UTF-16) read, NEL as a line break | ✅ | `tests/differential.rs` |
| Quoted scalars continued on less indented lines, as yaml.v2 reads them | ✅ | `quoted_scalars_continue_on_any_indentation_like_upstream` in `tests/differential.rs`; unit tests in `config/yaml.rs` |
| Flow collections (`[a,`, `{a: b,`) continued on less indented lines, as yaml.v2 reads them | ✅ | `flow_collections_continue_on_any_indentation_like_upstream` in `tests/differential.rs`; unit tests in `config/yaml.rs` against yaml.v2 v2.4.0's results |
| Implicit keys over several lines (quoted or plain, in a flow or a block) refused as yaml.v2 refuses them; anchor and alias names ending at their first character out of `[A-Za-z0-9_-]` (the rest read on, or refused), as libyaml ends them; the first document only, in its words | ✅ | `flow_collections_continue_on_any_indentation_like_upstream` in `tests/differential.rs`; unit tests in `config/yaml.rs` |
| Startup without a valid config ⇒ gRPC error on every call (no exit) | ✅ | the service answers errors until a config loads: `tests/differential.rs` (a bad file at start) |
| File provider: goruntime's loader (snapshot keys, dotfiles, symlinks, failures, nil loader), `SymlinkRefresher` / `DirectoryRefresher` on inotify events, reload coalescing, `runtime` scope | ✅ | snapshots and the first load: `tests/differential.rs`; reloads: `tests/differential.rs` on Linux (inotify, as upstream's fsnotify) |
| xDS SotW provider: go-control-plane's ADS client (`RateLimitConfig` resources, all of a response loaded as one config; ACK, or NACK with the loader's or protobuf's error, the last ACKed version kept; a failed stream opened again after the backoff on a new connection, a NACKed version then reported as acknowledged), the service waiting for the first config unless `FORCE_START_WITHOUT_INITIAL_CONFIG`, the stream stopped on SIGTERM | ✅ | `xds_configs_are_acked_like_upstream`, `xds_streams_fail_like_upstream`, `xds_over_tls_like_upstream`, `xds_responses_reread_the_tls_files_like_upstream` |

## 3. gRPC and HTTP API

| Item | Status | Test |
|---|---|---|
| `envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit` (no v2) | ✅ | `tests/grpc.rs`, `tests/differential.rs` |
| Every TCP connection, served (gRPC, HTTP, debug, Prometheus) or dialled (Redis, statsd, xDS, OTLP), is `TCP_NODELAY`, as Go's | ✅ | one helper (`net::nodelay`); gRPC and Redis: `tools/envoy-gateway/test.sh`, which fails without it (Nagle delays answers past Envoy Gateway's 20 ms timeout, and Envoy lets those requests through) |
| Requests that are not gRPC answered as grpc-go does before any handler: `415` (`INVALID_ARGUMENT`) for another content type, then `405` (`INTERNAL`) for another method than `POST` | ✅ | `non_grpc_requests_are_refused_like_upstream`; found by `the_image_serves_like_upstreams` |
| Request `hits_addend` (0 ⇒ 1 unless a per-descriptor addend) and per-descriptor (0 = check only) | ✅ | `tests/differential.rs` |
| `is_negative_hits`: floor 0, always `OK`, TTL reset to the full duration | ✅ | `tests/differential.rs` |
| Client-sent `limit`: replaces the config, without shadow / `replaces` / `unlimited` | ✅ | `tests/config.rs`, `tests/differential.rs` |
| Response fields: `overall_code`, `statuses[]`, `current_limit` (with `name`), `limit_remaining`, `duration_until_reset` | ✅ | `tests/differential.rs` |
| Empty domain or empty list ⇒ error; unknown domain ⇒ `OK` + `domain_not_found` | ✅ | `tests/service.rs`, `tests/differential.rs` |
| `LIMIT_*` headers: response, per unit, and request | ✅ | `tests/differential.rs` |
| Dynamic metadata (`RESPONSE_DYNAMIC_METADATA`): `domain`, `descriptors`, `hitsAddend`, merged `metadata` | ✅ | `tests/differential.rs`, `tests/service.rs` |
| `quota_mode`: `backend_name` / `model_name_override` groups, `OVER_LIMIT` when all exhausted, `passedBackends` per group | ✅ | `tests/service.rs`, `tests/differential.rs` |
| `replaces`: two passes, not transitive | ✅ | `tests/service.rs`, `tests/differential.rs` |
| Per-rule and global shadow mode (`global_shadow_mode`) | ✅ | `tests/service.rs`, `tests/differential.rs` |
| `/json` (any method): protojson, 200 / 429 / 400 / 500 | ✅ | `tests/differential.rs` (`json_requests_are_decoded_like_upstream` against `protojson.Unmarshal`, `json_is_served_like_upstream`) |
| SIGINT, SIGTERM, SIGHUP ⇒ gRPC stopped gracefully, exit status 0 (during the Redis startup wait: an error); the gRPC listener closed before the requests drain (a Unix socket's file removed) | ✅ | `the_binary_stops_on_sigterm_like_upstream`, `the_grpc_listener_closes_on_sigterm_like_upstream` in `tests/differential.rs` |
| `/healthcheck`: 500 with no body when unhealthy, unhealthy once shutting down; gorilla/mux routing (clean-path 301, 404); what Go's server answers itself (bad escapes, `Host` checks, `OPTIONS *`) | ✅ | `tests/differential.rs` (`the_healthcheck_answers_like_upstream`, `the_http_server_reads_requests_like_upstream`, `sigterm_turns_health_off_like_upstream`) |
| `grpc.health.v1`, service `ratelimit` | ✅ | `tests/differential.rs` (`health_is_reported_like_upstream`, `sigterm_turns_health_off_like_upstream`), `tests/health.rs` |
| Debug port: `/`, `/rlconfig` (`Dump()` format), `/stats`, `/debug/pprof/*`; Go's `ServeMux` (307 redirects, escaped paths), sniffed content types; a port it cannot listen on is not fatal | ✅ | `tests/differential.rs` (`the_debug_port_answers_like_upstream`, `bodies_are_sniffed_and_strings_quoted_like_go`, `a_busy_debug_port_is_ignored_like_upstream`) |
| `GRPC_UDS` | ✅ | `tests/differential.rs` |
| Max connection age | ✅ | the gRPC server's keepalive settings |
| Client mTLS + SAN, server certificate hot reload | ✅ | `grpc_over_tls_is_served_like_upstream` (and the `certs.*` stats), `grpc_certificates_are_reloaded_like_upstream` |

## 4. Algorithm and storage

| Item | Status | Test |
|---|---|---|
| Key `prefix + domain_key_value_…_bucketStart`, epoch-aligned; UTC calendar month | ✅ | `tests/differential.rs` (calendar month: `calendar_months_are_counted_like_upstream`) |
| Redis: `INCRBY` + `EXPIRE (unit + jitter)` pipeline | ✅ | `tests/differential.rs` (jitter: `expirations_are_jittered_like_upstream`) |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`: `GET` pipeline then `INCRBY`, siblings at 0, ghost keys | ✅ | `tests/differential.rs` |
| Local cache of over-limit keys (set before the shadow check, TTL = unit), freecache's size limits and eviction | ✅ | `tests/differential.rs`, `tests/freecache.rs` |
| `near_limit` threshold in `float32`; near / over split of `hitsAddend` | ✅ | `tests/differential.rs` |
| `REDIS_PERSECOND` (routing of `second` keys) | ✅ | `per_second_limits_use_their_own_redis_like_upstream` |
| Redis single (TCP or Unix socket, `redis://` URL, AUTH, pool, startup retry with backoff) | ✅ | `tests/runner.rs`, `tests/differential.rs` (`the_binary_*`) |
| Redis TLS, AUTH file | ✅ | `redis_over_tls_is_used_like_upstream`, `auth_files_are_reread_like_upstream` |
| Redis cluster: the topology from the first seed that dials (`READONLY` sent on its connections unless upstream's dialer is replaced), each of its nodes dialled by address (with `REDIS_AUTH`, without the seed URL's credentials), keys on their slots' nodes, a pool per node, replicas included (`cx_*` count every node's), over TLS too; a node down at startup refused as upstream | ✅ | `a_redis_cluster_serves_like_upstream`, `a_redis_cluster_over_tls_serves_like_upstream`, `a_cluster_node_down_at_startup_is_logged_like_upstream`; `redis/connection.rs` unit tests |
| Redis sentinel: the first sentinel that dials, the primary and its replicas it names (a pool on each: `cx_*` count theirs), by address with `REDIS_AUTH`; the failures as upstream's (an unknown primary, an unreachable sentinel, its auth refused, a URL without sentinels); after a failover, the new primary within radix's 5 s | ✅ | `a_redis_sentinel_serves_like_upstream`, `sentinel_startup_is_logged_like_upstream` |
| Redis AWS IAM (ElastiCache): a SigV4-presigned token as each connection's password, reused until 14 min or a minute before the AWS credentials expire; the credentials from the source Go's SDK picks (the environment's keys, a web identity, the profile's keys, role, web identity, SSO, login or process, the container's endpoint, IMDS), cached as its cache does (the container's 5 min early); `AWS_CA_BUNDLE` (or the profile's `ca_bundle`) as the only roots of its AWS calls but the container's | ✅ | `elasticache_iam_tokens_authenticate_like_upstream`, `aws_credentials_are_found_like_upstream`, `expiring_aws_credentials_are_refreshed_like_upstream`, `an_aws_ca_bundle_is_trusted_like_upstream`, `the_binary_fails_to_start_like_upstream`; `redis/aws.rs` unit test |
| Memcached: one `GetMulti` answers the request, then asynchronous increments (`incr`, `add` when absent with the window's expiration, a Unix time past 30 days, `incr` again on a lost race), a refund's `decr`; keys gomemcache refuses (> 250 B, spaces, control characters), values not 32-bit numbers and failures logged, the request answered from what was read; the local cache | ✅ | `memcached_serves_like_upstream`, `memcached_failures_are_logged_like_upstream`, `memcached_keys_are_sharded_like_upstream`, `memcached_over_tls_is_used_like_upstream`; `memcached/client.rs` unit tests |

## 5. Metrics, logs, traces

| Item | Status | Test |
|---|---|---|
| Per key: `total_hits`, `over_limit`, `near_limit`, `over_limit_with_local_cache`, `within_limit`, `shadow_mode`, `total_negative_hits` | ✅ | `tests/differential.rs` |
| `domain_not_found`, `global_shadow_mode`, `config_load_success`, `config_load_error` | ✅ | `tests/differential.rs` |
| `call.should_rate_limit.redis_error` / `service_error` | ✅ | `tests/differential.rs` |
| `ratelimit_server.<method>.total_requests` / `response_time` (health `Check` and `List` included) | ✅ | `server_stats_are_reported_like_upstream` in `tests/differential.rs` (statsd, DogStatsD, Prometheus with the default mapper's histogram); timer lines: `tests/stats.rs`, `tests/dogstatsd.rs` |
| `runtime.load_attempts`, `runtime.load_failures` (with `EXTRA_TAGS`, `reserved_tag` counted again) | ✅ | `tests/differential.rs` |
| Gauges (flushed every interval, after the stat generators): `redis_pool.cx_active`, `cx_total`, `cx_local_close`; `localcache.*` (freecache's counts); `runtime.num_values` | ✅ | `gauges_are_reported_like_upstream` in `tests/differential.rs` (at startup; after a Redis restart, ours only: see §7); freecache's counts against freecache itself: `tests/freecache.rs`; gauge lines: `tests/stats.rs`, `tests/dogstatsd.rs` |
| `memcache.*`: `multiget`, `increment`, `decrement`, `add` by `code` (tagged, with `EXTRA_TAGS`), `keys_requested`, `keys_found` | ✅ | `memcached_serves_like_upstream`, `memcached_failures_are_logged_like_upstream` |
| Exclusive sinks: selection and its fatal error, statsd over **TCP** (default) or UDP, log | ✅ | statsd over TCP: `tests/differential.rs`; selection, UDP datagrams, dropping, reconnecting, log lines: `tests/stats.rs` |
| DogStatsD (+ mogrifiers, global tags, origin detection) | ✅ | lines and tags: `tests/differential.rs`; datagrams, `int64` values, mogrifier rules and errors, origin detection: `tests/dogstatsd.rs` |
| Native Prometheus + statsd_exporter mapper: counters, gauges, histograms, TTLs, self-metrics, scrape answers | ✅ | mapper (loading, globs, regexes, templates) against statsd_exporter itself, series, text format, Gather errors, HTTP answers: `tests/differential.rs`; what panics upstream: `tests/prometheus.rs` |
| Prometheus summaries (observers not mapped to a histogram): statsd_exporter's quantiles and options, client_golang's age buckets and buffers, perks' CKMS estimates | ✅ | `summaries_reach_prometheus_like_upstream` and, with `EXTRA_TAGS`, `server_stats_are_reported_like_upstream` in `tests/differential.rs`; age buckets: unit tests in `stats/prometheus/summary.rs` |
| `SanitizeStatName` | ✅ | `tests/differential.rs` |
| `EXTRA_TAGS`, `STATS_FLUSH_INTERVAL` | ✅ | `EXTRA_TAGS`, `reserved_tag`: `tests/differential.rs`, `tests/stats.rs`; flush interval: `tests/stats.rs` |
| logrus' text (and terminal) and JSON (`@timestamp`, `@message`) formats, in Go's local time zone; the info, warning, error and fatal lines of the features ported (runner, sinks, runtime directory, Redis startup and pool, config, listeners, `/json`, local cache) | ✅ | `tests/differential.rs` (`logs_are_written_like_upstream`, `redis_startup_is_logged_like_upstream`, `a_late_redis_is_logged_like_upstream`, `the_binary_fails_to_start_like_upstream`), `log.rs` unit tests |
| Debug lines (`LOG_LEVEL=debug`), best effort (D3): config loading, each lookup, the cache and local cache, requests, statuses and responses as Go's `%+v` (prototext), errors, `/json`, the runtime and config update loops, health, Prometheus flushes. Today they match upstream's; they may change for the extensions | ✅ | `tests/differential.rs` (`debug_lines_are_written_like_upstream`, `prometheus_flushes_are_logged_like_upstream`, `messages_are_printed_like_upstream`) |
| OTLP traces, over gRPC or HTTP (gzip, TLS, client certificates): the resource (semconv 1.7 schema, `service.*`, `OTEL_RESOURCE_ATTRIBUTES`); otelgrpc 0.70's server span on every gRPC method (semconv 1.43 attributes, `OTEL_SEMCONV_STABILITY_OPT_IN` `rpc/old` and `rpc/dup`, status code and error status, flags); `ShouldRateLimit Execution` and `Redis Pipeline Execution` (with Memcached, `Memcached Fetch Execution`) beside it; `/json`'s three root spans with `NewJsonHandler Remaining Execution`; W3C TraceContext (with `tracestate`) and B3 (single and multiple, winning, deferred not sampled) extraction; `ParentBased(TraceIDRatioBased)` sampling; the span limits (`OTEL_*ATTRIBUTE_VALUE_LENGTH_LIMIT`, `OTEL_*ATTRIBUTE_COUNT_LIMIT`); only registered methods traced; the batch processor (`OTEL_BSP_*`) and the shutdown flush; an HTTP exporter given TLS settings for an insecure endpoint is fatal | ✅ | `traces_are_exported_like_upstream`, `memcached_calls_are_traced_like_upstream` in `tests/differential.rs`; parsing: unit tests in `trace/propagation.rs`, `trace/export.rs`, `trace/grpc.rs` |

## 6. Image and deployment

| Item | Status | Test |
|---|---|---|
| Binary `/bin/ratelimit`, distroless without a shell, amd64 + arm64 | ✅ | `Dockerfile` (static, mimalloc as allocator, on upstream's base image and tag: `distroless/static-debian12:nonroot`, a newer digest); `the_image_serves_like_upstreams` (`tests/image.rs`, in CI on amd64 and arm64, against upstream's image) |
| Any UID (65534), read-only FS, no capabilities | ✅ | `the_image_serves_like_upstreams` (`tests/image.rs`, in CI on amd64 and arm64, against upstream's image) |
| Ports 8080 / 8081 / 6070, `RUNTIME_ROOT=/srv/runtime_data/current` | ✅ | upstream's defaults: `the_image_serves_like_upstreams` (`tests/image.rs`, in CI on amd64 and arm64, against upstream's image) |
| Dual-stack listening (`HOST="::"`), `SO_REUSEPORT` | ✅ | the default wildcard serves IPv4 and IPv6, IPv4 alone in a container without IPv6: `the_image_serves_like_upstreams` (`tests/image.rs`, in CI on amd64 and arm64, against upstream's image); `SO_REUSEPORT`: `net.rs` |
| `/healthcheck` served after the initial Redis connection | ✅ | `a_late_redis_is_logged_like_upstream` (the HTTP port refused until Redis answers) |
| Image published for amd64 + arm64 with an SBOM and provenance, signed with cosign (keyless) | ✅ | `.github/workflows/release.yml`: on a pull request, the build with both platforms' attestations is checked without publishing; at `v0.1.0`, `ghcr.io/genesary/ratelimit:0.1.0` was published and signed, and `cosign verify` with the README's identity checks it |
| Deployment generated by Envoy Gateway (xDS TLS) works by changing only the image | ✅ | `tools/envoy-gateway/test.sh` (CI job `envoy-gateway`): Envoy Gateway v1.9.3 on kind (xDS and gRPC over TLS, the variables it injects without Redis TLS nor tracing, its security context: UID 65534, read-only, no capabilities); a global rate limit through Envoy answers alike with upstream's image and ours, `/rlconfig` alike, no call failing Envoy Gateway's 20 ms timeout |

## 7. Deliberate divergences

| Upstream behaviour | Our behaviour | Reason |
|---|---|---|
| `GRPC_CLIENT_TLS_CACERT` is added to the system's roots, so a client certificate any system-trusted CA issued (a public CA, in the image) is accepted unless `GRPC_CLIENT_TLS_SAN` is set | only certificates `GRPC_CLIENT_TLS_CACERT` issued | security: the system's roots vouch for servers, not for clients; `grpc_clients_are_verified_by_their_ca_alone` (Linux, where Go reads `SSL_CERT_FILE`) |
| The gRPC server's TLS handshake: Go's cipher suites, curves and alerts (e.g. for a refused client certificate, or a client offering only `http/1.1`) | rustls' | best effort (D3): which clients are served and refused is the same, but for the next row |
| Go accepts RSA keys from 1024 bits and ECDSA P-521 keys (the gRPC server's, its clients', Redis') | from 2048 bits, P-256 and P-384 (the ring provider) | keys below 2048 bits are deprecated; P-521 is rare |
| Client `limit` with `unit: UNKNOWN` ⇒ the process panics | gRPC error `INVALID_ARGUMENT` | D1: crashes are not reproduced |
| Invalid environment variable re-read on reload ⇒ panic | config error | D1 |
| An invalid environment variable (or `DOG_STATSD_MOGRIFIER_*`, or gostats' own checks) ⇒ panic at startup | an error with the same text | D1 |
| A variable whose value is not valid UTF-8 keeps its bytes (and Go's quoting shows them as `\xNN` in errors) | read with U+FFFD in place of invalid sequences | Rust strings are UTF-8 |
| `descriptors[].limit` in metadata as Go prototext, one or two spaces between fields depending on the build | the same text, always one space; `unit_multiplier:{value:N}` when set | Go randomises the spacing; `unit_multiplier` is an extension |
| Merging `metadata` writes into the configuration's own structs: a nested struct merged once keeps the other limit's fields in later responses, and concurrent requests race on it | merged into copies | D1: upstream bug; responses only differ once the configuration has been altered |
| gostats' Go runtime stats, `ratelimit.go.*` (gauges `alloc`, `sys`, `heapAlloc`, `heapSys`, `heapIdle`, `heapInuse`, `heapReleased`, `heapObjects`, `nextGC`, `lastGC`, `gcCPUPercent`, `numGoroutine`; counters `totalAlloc`, `lookups`, `mallocs`, `frees`, `pauseTotalNs`, `numGC`), which Prometheus gets unmapped (`ratelimit_go_*`) | not sent | No Rust equivalent: process figures under Go's names would mislead |
| The debug port's `/stats` prints expvar's `cmdline` and `memstats` (Go's `runtime.MemStats`) | `cmdline` only | No Rust equivalent; zeros or other figures under that name would mislead its readers |
| pprof serves the Go runtime's profiles (`goroutine`, `heap`, `allocs`, `block`, `mutex`, `threadcreate`, `goroutineleak`) and an HTML index at `/debug/pprof/`, CPU profiles and traces at `/debug/pprof/profile` and `/debug/pprof/trace` | `404 Unknown profile` for those names and the index (pprof's answer for any other name); `500 Could not enable CPU profiling: not available` and `500 Could not enable tracing: not available` | No Go runtime to profile; pprof's own error answers |
| `/rlconfig` lists domains and descriptors in Go's random map order | sorted by domain, then by key at each level | Deterministic |
| The Prometheus registry also holds Go's `go_*` and `process_*` collectors (a mapped name colliding with them makes scrapes fail) | not exposed | no Rust equivalent; see the row above |
| Go's default mux also serves `/debug/pprof/*` and `/debug/vars` on `PROMETHEUS_ADDR` (405 for other methods) | 404 | the same pprof and expvar question as above |
| A `PROMETHEUS_PATH` Go's mux panics on (not a clean absolute path), or reads as a method, a host or wildcards | error at startup; method, host and wildcard patterns are not supported | D1; not needed by the targeted deployments |
| A Prometheus mapper file using the top-level `buckets` or `quantiles` in a way statsd_exporter warns about, or setting `registerer`, `mappingscount` or `logger`, makes upstream panic at startup (its mapper has no logger) | error at startup | D1 |
| A histogram series with an `le` label or buckets not in increasing order, or a summary series with a `quantile` label or a negative `max_age`, makes upstream's exporter panic, which stops the process; a summary whose `max_age` is under a nanosecond per age bucket makes it loop forever | the event counts in `statsd_exporter_events_conflict_total` | D1 |
| A summary objective outside [0, 1] makes upstream's scrape panic (the connection closes with no answer) while the head stream holds fewer than 500 observations | the observation nearest that rank | D1 |
| A scrape asking for protobuf gets it | the text format (with the same `Content-Type` parameters) | Prometheus asks for text unless native histograms are enabled |
| When a Prometheus scrape fails, Go gathers concurrently: the order of several errors, and which of two help texts counts as expected, are random; the messages print metrics as Go prototext with random spacing and their creation time | registration order; single spaces, no creation time; Go's `%q` escaping of non-printable non-ASCII characters is not reproduced | deterministic |
| The text format sorts series with the same label values but different label names in a random order; gzip output is Go's | by label names; the same text, compressed by `flate2` | deterministic; the decompressed body is identical |
| The shipped Prometheus mapper, for a `ratelimit.service.rate_limit` stat whose last descriptor segment ends in `~` and letters, digits, `_` or `-` starting with a letter or digit (a descriptor value such as `a~b`, configured or from `detailed_metric`): upstream's keeps the whole segment in `key1` at one key, in `key2` at two | `key1` or `key2` before the last `~`, the rest in `limit`; at three keys and more, a `limit` label added | the `limit` label of list limits (D29); a user mapper file is not affected |
| The shipped Prometheus mapper, for a one-key `ratelimit.service.rate_limit` stat with a domain or descriptor segment that is exactly `*`: upstream's glob FSM takes it as its wildcard without capturing it, shifting the captures (`d.*` gives `key1=""`, `*.k` gives `domain="k"`) | each segment captured | its globs are regexes here (D29); deeper stats were already regexes upstream |
| A Prometheus mapper template with `%` directives using flags, widths or precisions (e.g. `%5s`) next to a `$n` is formatted by Go's `fmt` | printed as an unknown verb (`%!5(string=…)`) | not worth porting `fmt`; `%s`, `%v`, `%q`, `%x`, `%X`, `%T`, other verbs, `%%` and missing or extra arguments match |
| `PROMETHEUS_ADDR` with a service name for its port (`:http`) listens on that port | nothing listens | not needed by the targeted deployments |
| An `Accept` header with more than 12 clauses of equal weight is ordered by Go's pdqsort | kept in order | the order only matters for the `escaping` parameter |
| Off Linux, fsnotify watches the runtime directory with kqueue | polled every 100 ms (development only: the image is Linux, where both use inotify) | not a deployment target |
| A runtime directory that cannot be watched: the error names Go's errno (`no such file or directory (syscall.Errno 0x2)`) | the same form, with the C library's text for the errno | matches glibc's texts for common errors |
| U+2028 and U+2029 also break lines in YAML 1.1 (kept as characters in scalars) | characters only | the Rust parser is YAML 1.2; converting them would change scalars |
| yaml.v2 reads only the first YAML document of a file: a control character or invalid byte after a later `---` (and beyond its 512-byte read-ahead) is not seen; and it decodes as it parses, so a syntax error before a bad byte further on is reported first | the whole file is checked first | rare inputs; the file is refused either way in the second case |
| A snapshot's files are loaded in Go's random map order (which decides which error is reported, or the merge order) | in key order | one of upstream's orders, deterministic |
| protobuf-go starts its errors (`proto: mismatched message type`, logged and in NACKs) with `proto:` and a space or a non-breaking space, depending on the build | always a space | Go randomises it on purpose |
| On SIGTERM, upstream's process may exit before its xDS provider logs `Stopping xDS client watch for rate limit configurations` and the stream's `Canceled` error | always logged while a stream is up | one of upstream's outcomes, deterministic |
| A stream whose connection is lost reads as grpc-go's transport error for how it was lost (a reset, a `GOAWAY`, a missing trailer) | `rpc error: code = Unavailable desc = error reading from server: EOF` for all | best effort (D3); the stream is opened again either way |
| A `RateLimitConfig` (or `DiscoveryResponse`) protobuf-go cannot decode: its error for that input; a known field with the wrong wire type kept as an unknown field | `string field contains invalid UTF-8` or `proto: cannot parse invalid wire-format data`; a wrong wire type refused (NACKed, or the stream failed); messages nested over 100 levels refused (protobuf-go: 10000) | prost; malformed resources only |
| The xDS client's `user-agent` is grpc-go's | tonic's | only the management server sees it |
| A `unix://HOST/PATH` or `unix-abstract:` xDS URL: grpc-go's resolver refuses the first, dials the second | the path (or the host, without a path) dialled for the first; the second is not supported | not needed by the targeted deployments |
| `LOG_LEVEL=trace` writes each xDS response as Go prints the structure (with memory addresses) | not written | best effort (D3) |
| What upstream panics on at startup prints Go's panic and its stack trace, exit status 2 | a logrus `level=fatal` line with the panic's message, written whatever `LOG_LEVEL`, exit status 1 | D1: crashes are not reproduced; the stack trace has no equivalent |
| In `CLUSTER` mode, a pipeline's commands are grouped by key and the groups sent concurrently (`REDIS_CLUSTER_PIPELINE_PARALLELISM`), with `REDIS_PIPELINE_WINDOW`'s write buffering | fred's pipeline, each command sent to its slot's node | the same commands, keys and answers; only the scheduling differs |
| While Redis is down, radix fails at most one decision per pooled connection (`response returned from Conn: EOF`, or the socket's error; its pings drop the others), then each decision waits for a connection until the caller's deadline (its `WAIT` pool) | every decision fails at once, with `Unknown` and `response returned from Conn: EOF`, while every connection of the pool is down; a connection that is down is skipped while another is up. In `CLUSTER` mode, a decision still waits for a node that is down | agentgateway sets no deadline on its call, so waiting would hold every request for the length of the outage instead of applying its `failureMode`; behind Envoy (20 ms by default) only the latency changes. `decisions_fail_at_once_while_redis_is_down` |
| In `CLUSTER` mode, radix sends `READONLY` on every connection of its node pools, refreshes the topology every 5 s, and traces the dials it cancels when giving up on a node (`operation was canceled`, as many as timing allows) | `READONLY` on the startup probe of each node only; fred refreshes the topology on `MOVED` and `ASK`; no cancelled dials traced | `READONLY` only matters for reads from replicas, which the service never makes |
| In `CLUSTER` mode, radix also keeps a pool for a seed spelled unlike its node (`localhost:7000`, a `redis://` URL) until the topology replaces it, counted in `cx_*` | not opened | only at startup, for such seeds |
| In `SENTINEL` mode, radix tries the sentinels in Go's random map order (the error of the last one tried when none dials), follows a failover only when it next asks the sentinels (every 5 s: its `switch-master` subscription never fires, sentinels publish `+switch-master`), keeping meanwhile its pools by address (the demoted primary's connections, which Redis drops, redialled to it); after a failover, which of its connections were lost and redialled depends on timing | the sentinels in order; the same 5 s poll, and fred's own reconnection through the sentinels when Redis drops a connection, so requests can reach the new primary sooner | `cx_active` is the same; the counts of reconnections are timing-dependent upstream too |
| radix asks the sentinels (and, in `CLUSTER` mode, the topology) every 5 s, opening a pool on a replica that appears and closing that of one gone (`cx_*`) | the replicas' pools of the startup kept, each reconnecting to its address | the pools of replicas the service never uses: only `cx_*` differ, after the replicas change |
| radix opens one Redis connection at startup and the rest in the background | all the pool's connections at startup | the same once connected; a pool partly reachable at startup is not a target |
| Redis connection errors read as Go's `net` errors | the startup dial's errors as Go's and radix's (`dial tcp 127.0.0.1:1: connect: connection refused`, `i/o timeout`, `response returned from Conn: <Redis' refusal of AUTH or SELECT>`); later ones (a failed reconnection, a command) as fred's; a name that does not resolve reads as the system resolver's error. Each startup attempt dials, authenticates and selects the database on a connection of its own, as radix's dial, before fred connects | different libraries; the messages around them are upstream's |
| On SIGTERM, grpc-go closes each connection whose client does not answer its drain's ping 5 s later, once that connection's calls are done; a connection still in its TLS handshake or before its HTTP/2 preface holds the stop up to 120 s | the server stops 5 s later once no call is in flight on any connection | the process stops at the same time; `silent_grpc_clients_hold_the_stop_like_upstream` |
| `GRPC_MAX_CONNECTION_AGE` gets ±10% of jitter (grpc-go) | applied as it is (tonic) | spreads reconnections only |
| A `redis://` URL's `password` and `username` query parameters appear in startup messages (upstream masks only the user info), and `Creating single with urls %v` and `Creating cluster with urls %v` print `REDIS_URL` as given | masked too | keeps secrets out of logs |
| Failing to get AWS credentials at a connection, Go's SDK error text follows `retrieving AWS credentials for ElastiCache IAM authentication: failed to refresh cached credentials, `, after its retries (IMDS: 3 attempts) | the Rust SDK's (aws-config) text and retries | best effort (D3): the same prefix, the same retries of the connection |
| `config.LoadDefaultConfig` refuses at startup what it cannot parse in the environment or the profile for its other features (`AWS_MAX_ATTEMPTS=x`, `AWS_USE_FIPS_ENDPOINT=yes`, an incomplete SSO profile, …), and reads what it can of a malformed config or credentials file | such values ignored (an incomplete SSO profile fails at each connection instead); a file the Rust parser refuses is skipped whole | the credentials' and the region's own failures are reproduced; these are of settings the service does not use |
| Go's STS client for the credentials ignores `AWS_ENDPOINT_URL` (only `AWS_ENDPOINT_URL_STS` reaches it) | both honoured | a global endpoint override meant for every service |
| A `credential_process` without `PATH` in the environment fails (`sh` not found) | `sh` found in `/bin:/usr/bin` | Rust's default search path |
| Under a role, the container's or IMDS' credentials have a cache of their own | asked again at each refresh of the role | a few more calls, at the role's refresh only |
| A cache name that is not a host (`/`, a space, …) fails each connection's token (`building the ElastiCache IAM authentication request`) or signs part of it as a path | signed as the host as it is | ElastiCache names are `[a-z0-9-]` |
| With `AWS_DEFAULT_PROFILE`, the container's and IMDS' own settings (`ec2_metadata_service_endpoint`, …) are read from that profile | from `AWS_PROFILE`'s, or `default` | aws-config reads them so; the credentials' profile is Go's |
| The role session name Go sends by default (`aws-go-sdk-<ns>`, a web identity's `<ns>`) is the time of the first request | of the startup | the same form; only the time differs |
| On Memcached, the healthcheck and `grpc.health.v1` never report healthy: the Redis component starts unhealthy and only Redis marks it | healthy once the Memcached client is made | maintainer's decision: a service behind health probes could not run on Memcached; `memcached_serves_like_upstream` |
| Memcached over TLS on a Unix socket sends the socket's path as the TLS server name (which a server may refuse) | sends `memcached.invalid`, so the certificate is only accepted with `MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION` (as upstream's) | rustls names a server by DNS name or IP only |
| `MEMCACHE_HOST_PORT` accepts what `net.ResolveTCPAddr` does: an empty host (the local system), an empty port (0), a service name for the port | such an entry does not resolve: no server at all | none is a memcached address one writes |
| `LookupSRV` refuses an answer with a target that is not a domain name; gomemcache fails on a reply line over 4 KiB (`bufio: buffer full`), reads a negative item size, and panics on an `incr` reply of a bare `\n` | such targets kept; no line limit; a negative size and a bare `\n` are errors | servers that do not answer as memcached and DNS do (D1 for the panic) |
| A failed `MEMCACHE_SRV` lookup reads as Go's resolver words it (`lookup <name> on <server>: no such host` for a name without such records) | the same for a name without records; hickory-resolver's words otherwise (a timeout, a refused query) | best effort (D3) for the rarer failures |
| gomemcache's read and write errors on a connection read as Go's `net.OpError` (`read tcp <local>-><remote>: i/o timeout`) | the same for timeouts and system errors; rustls' words for TLS alerts during an operation | best effort (D3) |
| Memcached's background increments run on goroutines that a busy service reuses, one task after another | a task per request | the order of increments across requests is not atomic anyway (D24) |
| In a flow collection, an anchor or alias name directly followed by `:` or `?` (`[&a:b c]`): libyaml reads `:` as a value indicator, and refuses `?` | read on as a plain scalar (`":b c"`, `"?b c"`) | not a config one writes; the same name ends there |
| YAML syntax error text and line (Go's libyaml port) | the Rust parser's wording and position | different parser; the message structure is kept |
| `invalid map key` message escapes strings with Go's `%#v` | Rust escaping for unusual characters | only differs for non-ASCII or control characters |
| `!!binary` decoding to invalid UTF-8 ⇒ `error parsing metadata` | decoded lossily | Rust strings are UTF-8 |
| Negative or NaN `NEAR_LIMIT_RATIO`: the near threshold is converted to an integer by the CPU (amd64 wraps a negative value, arm64 gives 0) | 0 | Go leaves out-of-range float conversions to the platform |
| datadog-go sends its own telemetry every 10 s (`datadog.dogstatsd.client.*`, tagged `client:go`, `client_version:5.4.0` and the global tags) | not sent | a Rust client reporting as `client:go` would mislead; the agent only uses it for client health |
| A mogrifier's tags come in Go's random map order | sorted by tag name | deterministic |
| DogStatsD lines are packed by 32 workers, by name hash | one buffer: the same lines, in other datagrams | datagram boundaries carry no meaning for the agent |
| Mogrifier `PATTERN`s are Go RE2 | Rust `regex`, with `\d`, `\w`, `\s`, `\b` and their negations made ASCII as in Go; syntax only one of the two accepts (e.g. `\Q…\E`, `\b{start}`) differs | different engine |
| An invalid mogrifier ⇒ panic at startup | configuration error | D1 |
| Error texts: an invalid mogrifier `PATTERN` (after `failed to compile pattern for <NAME>: <PATTERN>: `), a DogStatsD address that does not resolve or cannot be dialled (e.g. port 0) | the Rust regex and resolver texts | different libraries; the message prefixes are kept |
| A `STATSD_HOST` starting with `unix://` sends DogStatsD over a Unix socket | not supported: the address does not resolve | not needed by the targeted deployments; can be added |
| The statsd sink logs Go `net` errors (`connection error: dial tcp 127.0.0.1:8125: connect: connection refused`) | the same messages with the operating system's error text | Rust has no `net.OpError`; the message prefixes are kept |
| After a failed write to statsd, gostats resends a buffer from the first unwritten byte, so the new connection can start mid-line | resends from the start of the first line not fully written | D1: a corrupt line is a bug |
| statsd buffers are cut at the same line boundaries as gostats' but are not timed alike (gostats also pushes its buffer every second), so while the server is unreachable which lines are dropped can differ; the queue is bounded alike (about 4 MiB) | buffers pushed only when full or flushed | Not observable while the server is reachable |
| Go's server refuses some requests itself with a `400` closed by the connection (no `Content-Length`); among them a request line it cannot parse (e.g. a raw space in the target) or an invalid header name or value | the same answers with a `Content-Length` for a bad `%` escape and a missing or malformed `Host`; hyper's empty `400` for the others | Different HTTP server; both refuse the request, and the bodies read alike |
| A gRPC health `Watch` stream that falls behind gets only the latest status (grpc-go keeps one pending update) | every change in order, duplicates skipped; past 64 pending changes the oldest are dropped | The final status is the same |
| On every HTTP port, a request target with a fragment (`/healthcheck#x`) keeps it in the path (and the query), so it is not found | the fragment is dropped (hyper's URI parser) | Clients do not send fragments |
| On the debug and Prometheus ports, an absolute target without a path (`GET http://h`) is redirected to `/` | served as `/` | hyper gives such a target the path `/` |
| A lost Redis connection is noticed when radix next uses it (a command, which fails, or its PING every 5 s / pool size): only then is it counted in `redis_pool.cx_local_close`, replaced (`cx_total`), and, with `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION`, reported to health | as soon as the socket closes, and reconnected at once | Earlier, not different: the same states in the same order |
| Each failed attempt of the startup loop (`REDIS_STARTUP_*`) adds to `redis_pool.cx_total` the connections radix made, and to `cx_local_close` those its `Close` found idle (some leak uncounted), by amounts that depend on timing | only the pool that started is counted | Not reproducible: radix's background dials race its `Close` |
| A request cancelled while its Redis reply is partly read makes radix discard the connection (`cx_local_close`, then a new one in `cx_total`) | the connection is kept | fred's connections are shared and outlive a cancelled request |
| `/json` refuses `unitMultiplier` (or `unit_multiplier`, even `null`) in a client limit with `400`: Go's type has no such field | read and applied as over gRPC (§8, D23), and echoed in the metadata's limit | Extension (see `unit_multiplier` above): requests upstream refuses, none it accepts |
| On `/json`, a client limit with an unknown unit (none, `0`, a negative number, or one above `7`) makes the service panic, and Go's server closes the connection with no answer | `400 Bad Request` | D1 |
| `/json` bodies over 2048 bytes are chunked (HTTP/1.1) or ended by closing the connection (HTTP/1.0), with no `Content-Length` | always a `Content-Length` | Different HTTP server; the bodies read alike |
| `/json` answers with a space after every comma, or after none, depending on the build (protojson's `detrand`) | never | Go randomises it on purpose |
| `/json` reads a body of any length | a body over 4 MiB (grpc-go's largest message) gets `400 Bad Request` | D1: an unbounded body can exhaust the memory and get the process killed |
| `/json`'s `error: %s` warning names protojson's decoding error (`proto: (line 1:2): unknown field \"x\"`) or the body's read error | the Rust decoder's and hyper's texts | Only logged; every such request gets the same answer |
| grpc-go's own logger writes what `GRPC_GO_LOG_SEVERITY_LEVEL` and `GRPC_GO_LOG_VERBOSITY_LEVEL` ask for | nothing | Nothing reaches it in a normal run at its default level (errors) |
| A listener that fails for another reason than its bind names that system call | `bind` | Only the bind fails in practice |
| A listener that fails names a host that is a name (`localhost`) by its resolved IP | the host as configured | Listen hosts are IPs or the wildcard in practice, which read as upstream's |
| Debug lines print messages (`%+v`) with one space between fields or two, depending on the build (prototext's `detrand`) | always one | Go randomises it on purpose |
| Debug lines print a request's fields unknown to upstream's protos by number (`3:"\x08\x03"` for a limit's `unit_multiplier`) | known fields of the current Envoy proto by name (`unit_multiplier:{value:3}`); other unknown fields are dropped | `unit_multiplier` is an extension; prost keeps no unknown fields |
| `share_threshold enabled for entry index …` lines come in Go's random map order | by entry index | deterministic |
| The config files usually load while Redis is still being connected to (the provider starts with the server, before the limiter): their debug lines come before Redis's health line and `Waiting for initial ratelimit config update event` | Redis first, then the config | the order of two independent startup steps |
| The config update loop and the runtime provider are separate goroutines: their debug lines (`Waiting for config update event`, `waiting for runtime update`, the reload's lines) interleave in a racy order | the provider's thread runs the update, then waits | one of upstream's orders |
| Span attributes holding a message (`request string`, `response`) have one space between fields or two, depending on the build (prototext's `detrand`) | always one | Go randomises it on purpose |
| A span's start and end follow Go's code paths (the Redis span also covers building the statuses) | the same points of ours, so durations differ slightly | different code; names, parents and attributes are the same |
| A failed export is retried with backoff (429, 502, 503, 504, gRPC's retryable codes) for up to a minute | not retried: the batch is dropped | best effort (D3); the next batches go out as usual |
| Export errors are written by Go's standard logger with OTel's texts | the same logger format, this exporter's texts | best effort (D3) |
| An invalid `OTEL_EXPORTER_OTLP_HEADERS` entry or endpoint is written whole in the error | the entry's position or the header's name, the variable's name | header values and URLs can hold credentials |
| A `TRACING_SAMPLING_RATE` of `NaN` samples depending on the platform (Go's `uint64(NaN)`) | never | deterministic |
| A failed TLS handshake with Redis names Go's error | Go's words for an unknown authority (Linux's: on macOS, Go asks the system, whose words differ), a name the certificate is not for (`x509.HostnameError`) and an expired certificate; rustls' for the others | the common failures read as upstream's |
| A key pair or CA file that does not parse fails with Go's `crypto/tls` error | Go's for missing files and for files with no PEM data; rustls' otherwise | the file and the reason are named either way |
| TLS to Redis over a Unix socket verifies the socket's path as the server's name (so works only with `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION`) | refused at startup: fred would open the socket in clear | nobody encrypts a local socket; refusing beats sending in clear |
| Once connected, a pool's reconnection errors read as radix's | fred's and rustls' | best effort (D3); the startup errors read as Go's |
| radix fills a pool one connection at a time in the background; stopping before it is full cancels the dials still running, which are logged (`creating redis connection error : context canceled`, `… operation was canceled`), as many as the machine is slow (TLS makes it likely) | the pool's connections are made at startup, nothing to cancel | racy upstream, at shutdown only |
| The expiration jitter of each key is drawn from `math/rand` seeded with the start's second | another random source | the extras are random either way; their range is the same |
| `OTEL_BSP_MAX_QUEUE_SIZE=0` lets a span through when the processor happens to wait for one; a negative size panics at startup | every span dropped | D1; nobody sets either |
| The exporter's `User-Agent` is `OTel OTLP Exporter Go/1.45.0` (and grpc-go's) | `ratelimit/<version>` (and tonic's) | it names the software that sends |
| The HTTP exporter honours `HTTPS_PROXY`/`HTTP_PROXY` and negotiates HTTP/2 over TLS | no proxy; HTTP/1.1 | best effort (D3) |
| Baggage is extracted and the propagators can inject | neither | nothing reads baggage and the service makes no traced call |
| The CLI client's tracing | not yet | with the client |
| On what upstream panics on (D1), the debug lines it writes before the panic (the cache's, for a client limit with an unknown unit) | the request stops at its check, after `caught error during call: <error>` | D1 |

## 8. Extensions

Not upstream's: what `docs/design.md` adds (B and C). Proven by tests of their own, on Redis and Memcached; upstream's behaviour is unchanged (§1–6).

| Item | Status | Test |
|---|---|---|
| `rate_limits:` (D7): named limits, exclusive with `rate_limit:`, `window` instead of `unit`, no `unlimited`; names per ADR 0001, unique in their list, `replaces` not aimed at the list itself; the descriptor's `shadow_mode`, `quota_mode`, `detailed_metric`, `metadata` shared; the new keys valid only in their place (unknown elsewhere, as upstream says); YAML only (D28) | ✅ | `tests/rate_limits.rs` |
| `fixed` windows of any duration (D8): whole numbers of `w` `d` `h` `m` `s`, in that order (`1h30m`); reported as the largest unit dividing them, with `unit_multiplier` when ≠ 1 (D13) | ✅ | `windows_are_reported_in_the_largest_unit_dividing_them`, `list_limits_count_in_their_own_windows` |
| `offset: <duration>` and `offset: hashed` (FNV-1a 64 of the entries after `share_threshold`, `offset_rounding` and its default) (ADR 0002) | ✅ | `offsets_are_read_as_adr_0002_writes_them`, `hashed_phases_follow_the_descriptor`, `hashed_offsets_and_escaped_keys_are_stored_as_adr_0001_says` |
| Keys `{descriptor}_shape_start_name`, the hash tag escaped (ADR 0001); TTL = duration + jitter; `duration_until_reset` from the window's origin (D12) | ✅ | `list_limits_count_in_their_own_windows`, `hashed_offsets_and_escaped_keys_are_stored_as_adr_0001_says` |
| A descriptor's representative status (D17); its shadow mode after the reduction; quota groups exhausted when one limit is | ✅ | `list_limits_count_in_their_own_windows`, `ties_are_won_by_the_first_list_limit`, `shadow_mode_applies_to_a_list_after_its_reduction`, `quota_groups_see_a_list_exhausted_when_one_limit_is` |
| `replaces` on list limits (D16): replacing and replaced by name, a fully replaced descriptor OK without a limit, out of its quota group but among `passedBackends` | ✅ | `list_limits_replace_and_are_replaced_by_name`, `quota_groups_see_a_list_exhausted_when_one_limit_is` |
| Refunds on every limit of a list (D21), floored at 0 | ✅ | `refunds_take_hits_back_from_every_list_limit` |
| Local cache for `fixed` (D18): over-limit keys kept for the window | ✅ | `list_keys_over_their_limit_are_kept_in_the_local_cache` |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` has no effect on lists (D20) | ✅ | `stopping_increments_over_the_limit_leaves_lists_alone` |
| `REDIS_PERSECOND` (D15): a list with a window under a minute goes whole to the per-second Redis | ✅ | `lists_with_a_short_window_go_to_the_per_second_redis` |
| Per-unit headers over every limit, `<n><Unit>s` for a multiplier (D13); `RateLimit-Reset` from the representative's window | ✅ | `headers_describe_list_limits` |
| Stats `<descriptor>~<name>` (D29); `/rlconfig` a line per limit with its window | ✅ | `list_limits_count_in_their_own_windows`, `lists_are_dumped_limit_by_limit`, `list_stats_keys_follow_the_descriptor_settings` |
| Memcached: list limits of `fixed` windows (D24) | ✅ | `list_limits_count_on_memcached` |
| `overflow: envoy` (D14) | ✅ | `overflow_envoy_is_accepted` |
| `overflow: strict` (D14) | ⬜ | refused at load for now |
| `anchored` (D9), `overflow: envoy`: opened by a request presented while none is (`open_on: check`) or by its first counted hit (`first_hit`), its anchor rounded down (`start_rounding`), never moved; closed at `anchor + duration`, a clock behind the anchor counting as none elapsed; the next request opens the next; refunds never open or close it (D21); `duration_until_reset` and `RateLimit-Reset` from its anchor; `opened_without_hits` for windows a denied request opened without hits; hits and counts saturated at 2^53 | ✅ | `anchored_windows_open_count_and_close_as_d9_says`, `anchored_windows_open_on_a_request_and_last_their_duration`, `first_hit_windows_wait_for_counted_hits`, `windows_opened_without_hits_are_counted` |
| `anchored` in one Redis script, the same decisions as the Rust window over random sequences (D31, D32.4) | ✅ | `the_anchored_script_steps_like_the_rust_window` |
| `anchored` refused on Memcached (D24) | ✅ | `invalid_anchored_windows_are_refused` |
| `anchored` never in the local cache (D18): Redis decides every time, and counts an amendment over the limit | ✅ | `anchored_windows_stay_out_of_the_local_cache` |
| `sliding` and `sliding_approx` (D10) | ⬜ | refused at load for now |
| `token_bucket` (D11, D12): GCRA, the deadline in microseconds; admitted while not empty, charged in full, denied hits too; `burst`, `max_debt` in tokens, as a duration, `0` or `unlimited`, truncated hits in `total_capped_hits`; the reset until full, or until it admits, at least 1 s | ✅ | `token_buckets_load_as_d11_says`, `token_buckets_step_as_d11_says`, `token_buckets_admit_until_empty_and_charge_in_full`, `token_buckets_follow_their_burst_and_max_debt` |
| `token_bucket` in one Redis script, the same decisions as the Rust window over random sequences through three instances on a Redis Cluster, racing charges all counted (D31, D32.4) | ✅ | `the_token_bucket_script_steps_like_the_rust_window_on_a_cluster` |
| `token_bucket`: refunds move the deadline back, never before now (D21); stats (D22); the local cache until it would admit, cleared by a refund, not in shadow mode (D18); no effect of `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` (D20); refused on Memcached (D24) | ✅ | `stopping_increments_leaves_token_buckets_alone`, `refunds_move_a_token_buckets_deadline_back`, `token_bucket_stats_count_as_d22_says`, `denied_token_buckets_are_kept_in_the_local_cache`, `token_buckets_load_as_d11_says` |
| `token_bucket`: warnings at load, a day or more to fill (D11), `quota_mode` with `max_debt: unlimited`, in a gauge too (D19); the quota lookup's `level` and time until full; headers from the status, in a list with other limits | ✅ | `token_buckets_warn_of_what_does_not_suit_them`, `quota_lookups_show_a_token_buckets_level`, `token_buckets_answer_headers_and_share_lists` |
| `concurrency` (D35, ADR 0004): a check takes a slot up to the limit, a hit or refund releases, floored at 0; `retry_after` on every answer; required `lease`, slots purged by every operation; the newest released first; one sorted set per limit, ADR 0001's key with the shape `cc` | ✅ | `concurrency_windows_load_as_adr_0004_says`, `slots_step_as_d35_says`, `concurrency_windows_take_and_give_back_slots`, `slots_end_with_their_lease_and_releases_take_the_newest`, `models_with_any_name_have_slots_of_their_own` |
| `concurrency`: the script steps like the Rust window over random sequences through three instances on a Redis Cluster, and the limit holds under racing checks (D32.4) | ✅ | `the_concurrency_script_steps_like_the_rust_window_on_a_cluster`, `several_instances_share_one_servers_slots` |
| `concurrency`: no slot held for a request another limit denies, slots given back when another `concurrency` limit denies | ✅ | `a_request_denied_by_another_limit_holds_no_slot`, `slots_are_given_back_when_another_concurrency_limit_denies` |
| `concurrency` with `shadow_mode`, `QUOTA_MODE`, `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`, the local cache and `REDIS_PERSECOND`; refused with `quota_mode`, with other limits in its list and on Memcached | ✅ | `shadow_mode_takes_slots_past_the_limit`, `other_settings_leave_concurrency_limits_alone`, `concurrency_windows_load_as_adr_0004_says` |
| `concurrency` in the quota lookup: `in_flight`, `limit`, `used_pct`; the `slots_*` and `releases_without_slot` stats, mapped by the shipped mapper | ✅ | `quota_lookups_show_slots_in_flight`, `concurrency_windows_take_and_give_back_slots`, `the_default_mapper_labels_list_limits` |
| A config's fingerprint (D36): a gauge per domain with a list, in `/rlconfig`, in a log line at each load; stable however it is written; nothing for upstream's configs | ✅ | `fingerprints_follow_what_decides_and_nothing_else`, `config_fingerprints_are_reported_at_each_load`, `the_default_mapper_labels_config_fingerprints` |
| The agentgateway guide's cost weighted by the cache, on a real agentgateway (`RATELIMIT_AGENTGATEWAY=<path>`, local): a response and a stream, a null or absent cached count; a failing or negative expression skips the descriptor | ✅ | `a_real_agentgateway_weighs_tokens_by_the_cache` |
| `concurrency` behind a real agentgateway (`RATELIMIT_AGENTGATEWAY=<path>`, local): released after a response, a 503, a cut stream, a client leaving a stream; refused with `retry-after`, no amendment after; a transport error and a client leaving before the headers orphan the slot | ✅ | `a_real_agentgateway_takes_and_releases_slots` |
| The shipped Prometheus mapper: a list limit's `~name` in a `limit` label, one key and deeper, `opened_without_hits` mapped (D29); upstream's globs rewritten as regexes after those rules, every other stat mapped as upstream's default mapper maps it | ✅ | `the_default_mapper_labels_list_limits`, `the_shipped_mapper_maps_upstream_stats_like_upstream` |
| The shipped Prometheus mapper's `total_capped_hits` (D11, D29) and `config_lasting_lockouts` (D19) | ✅ | `the_default_mapper_labels_list_limits`, `the_default_mapper_maps_the_lasting_lockouts_gauge` |
| Memcached: a warning at load when a list key's static part exceeds 250 − 64 bytes (ADR 0001) | ✅ | `long_memcached_keys_are_warned_about_at_load` |
| A client-sent `limit` with `unit_multiplier` (D23): read, applied, echoed, 0 refused, like any window at most 2^32 − 1 s; a multiplier of 1 is none | ✅ | `client_limits_apply_their_unit_multiplier`, `client_limits_apply_their_unit_multiplier_on_memcached`, `a_client_limit_multiplied_by_one_is_a_plain_one`, `client_limits_with_an_impossible_multiplier_are_refused` |
| Quota lookup (ADR 0003): `POST /quota` on the HTTP port; every limit of every descriptor (`replaces` and a client `limit` resolved as in a decision, unlimited ones, none for an unmatched descriptor), its window, `used`, `remaining`, `used_pct`, `resets_at`, `resets_in_s`, an anchored session's `open` (closed: no reset); 400, 404, 405, 500 | ✅ | `quota_lookups_show_every_limit`, `quota_lookups_resolve_rules_as_decisions_do`, `quota_lookups_are_served_over_http` |
| Quota lookup changes nothing: reads only (`GET`, `HMGET`), keys, TTLs and rate-limit stats untouched, no local cache access | ✅ | `quota_lookups_change_nothing` |
| Quota lookup announces what the next decision applies (OK ⇔ `used + N ≤ limit`) | ✅ | `quota_lookups_announce_what_the_next_decision_applies` |
| Quota lookup on Memcached: one `GetMulti`, nothing counted in `memcache.*` | ✅ | `quota_lookups_read_memcached` |
| `ratelimit.quota_lookup.*` stats; the shipped mapper's `ratelimit_quota_lookup_errors{cause}` and `ratelimit_quota_lookup_response_time_seconds` | ✅ | `quota_lookups_are_served_over_http`, `the_default_mapper_maps_quota_lookup_stats` |
