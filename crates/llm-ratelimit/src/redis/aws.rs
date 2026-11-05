//! `REDIS_AWS_IAM_AUTH`: ElastiCache IAM authentication as upstream's
//! `src/redis/credentials_aws.go` does it. Each connection authenticates as the user with a
//! token: a request to the cache presigned with SigV4, reused for 14 minutes (or until a minute
//! before the AWS credentials expire).

use aws_config::profile::{Profile, ProfileSet};
use aws_config::provider_config::ProviderConfig;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_types::region::Region;
use ring::{digest, hmac};
use std::time::Duration;

/// `elastiCacheIAMSigningService`.
const SIGNING_SERVICE: &str = "elasticache";
/// `elastiCacheIAMTokenValidity`, in seconds.
const TOKEN_VALIDITY: i64 = 15 * 60;
/// `elastiCacheIAMTokenReuseMargin`, in seconds.
const TOKEN_REUSE_MARGIN: i64 = 60;
/// `emptyPayloadHash`.
const EMPTY_PAYLOAD_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// AWS credentials, as the SDK's `aws.Credentials` holds them.
#[derive(Clone, PartialEq)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Empty without one.
    pub session_token: String,
    /// When they expire, if they do.
    pub expires: Option<jiff::Timestamp>,
}

impl std::fmt::Debug for AwsCredentials {
    /// The key's ID only: the secret and the session token stay out.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("expires", &self.expires)
            .finish_non_exhaustive()
    }
}

/// The cache and the user a token is for.
#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    pub region: String,
    /// Lowercased, as upstream's provider keeps it.
    pub cache_name: String,
    pub user_id: String,
    pub serverless: bool,
}

/// One pool's `*_AWS_IAM_*` settings.
#[derive(Debug, Clone, PartialEq)]
pub struct IamSettings {
    /// The variables' prefix (`REDIS`, `REDIS_PERSECOND`), for errors naming them.
    pub prefix: &'static str,
    pub cache_name: String,
    pub user_id: String,
    pub serverless: bool,
    /// Empty: the AWS configuration's.
    pub region: String,
}

/// `elastiCacheIAMCredentialProvider`: the user, and a token reused until its deadline.
pub struct IamCredentials {
    target: Target,
    source: CachedCredentials,
    /// The last token, and until when it is reused.
    token: tokio::sync::Mutex<Option<(String, jiff::Timestamp)>>,
}

impl std::fmt::Debug for IamCredentials {
    /// Without the token.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IamCredentials")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl IamCredentials {
    /// `newAwsIamCredentialProvider`: the settings checked, the AWS configuration loaded as
    /// `config.LoadDefaultConfig` loads it, the region resolved, and its line logged.
    pub async fn load(s: &IamSettings) -> Result<Self, String> {
        let p = s.prefix;
        if s.cache_name.is_empty() || s.user_id.is_empty() {
            return Err(format!(
                "{p}_AWS_IAM_CACHE_NAME and {p}_AWS_IAM_USER_ID are required when {p}_AWS_IAM_AUTH is enabled"
            ));
        }
        let config = AwsConfig::load().await?;
        let region = match s.region.as_str() {
            "" => config.region.clone(),
            region => region.to_string(),
        };
        if region.is_empty() {
            return Err(format!(
                "{p}_AWS_IAM_REGION is required when the AWS SDK cannot resolve a region"
            ));
        }
        crate::log::warn(&format!(
            "enabling AWS IAM authentication to ElastiCache cache {} as user {} in {region}",
            s.cache_name, s.user_id
        ));
        Ok(Self {
            target: Target {
                region,
                cache_name: s.cache_name.to_lowercase(),
                user_id: s.user_id.clone(),
                serverless: s.serverless,
            },
            source: config.credentials,
            token: tokio::sync::Mutex::new(None),
        })
    }

    /// `Credentials`: the user and the token, a new token once the last one's deadline has
    /// passed.
    pub async fn credentials(&self) -> Result<(String, String), String> {
        let mut token = self.token.lock().await;
        let now = jiff::Timestamp::now();
        if let Some((token, until)) = &*token
            && now < *until
        {
            return Ok((self.target.user_id.clone(), token.clone()));
        }
        let credentials = self.source.retrieve().await.map_err(|e| {
            format!("retrieving AWS credentials for ElastiCache IAM authentication: {e}")
        })?;
        let new = self::token(&credentials, &self.target, now);
        *token = Some((new.clone(), reuse_deadline(now, &credentials)));
        Ok((self.target.user_id.clone(), new))
    }
}

/// `tokenReuseDeadline`: 14 minutes, or a minute before the credentials expire if sooner.
fn reuse_deadline(now: jiff::Timestamp, credentials: &AwsCredentials) -> jiff::Timestamp {
    let margin = jiff::SignedDuration::from_secs(TOKEN_REUSE_MARGIN);
    let deadline = now + jiff::SignedDuration::from_secs(TOKEN_VALIDITY) - margin;
    match credentials.expires {
        Some(expires) if expires - margin < deadline => expires - margin,
        _ => deadline,
    }
}

/// What `config.LoadDefaultConfig` resolves: the region and the credentials.
struct AwsConfig {
    /// Empty when none is configured.
    region: String,
    credentials: CachedCredentials,
}

impl AwsConfig {
    async fn load() -> Result<Self, String> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let first = |names: &[&str]| names.iter().find_map(|name| var(name));
        let environment = Environment {
            keys: match (
                first(&["AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY"]),
                first(&["AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY"]),
            ) {
                (Some(access_key_id), Some(secret_access_key)) => Some(AwsCredentials {
                    access_key_id,
                    secret_access_key,
                    session_token: var("AWS_SESSION_TOKEN").unwrap_or_default(),
                    expires: None,
                }),
                _ => None,
            },
            web_identity_token_file: var("AWS_WEB_IDENTITY_TOKEN_FILE"),
            role_arn: var("AWS_ROLE_ARN").unwrap_or_default(),
            role_session_name: var("AWS_ROLE_SESSION_NAME").unwrap_or_default(),
            container_relative_uri: var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"),
            container_full_uri: var("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
        };
        // The shared files: a profile named by `AWS_PROFILE` must exist (`resolveConfigLoaders`).
        let named = var("AWS_PROFILE");
        let name = named
            .clone()
            .or_else(|| var("AWS_DEFAULT_PROFILE"))
            .unwrap_or_else(|| "default".into());
        // Go skips what it cannot parse; a file aws-config cannot parse is skipped whole.
        let profiles = aws_config::profile::load(
            &aws_types::os_shim_internal::Fs::real(),
            &aws_types::os_shim_internal::Env::real(),
            &Default::default(),
            None,
        )
        .await
        .ok();
        let shared = match SharedConfig::resolve(profiles.as_ref(), &name, &mut Vec::new()) {
            Ok(shared) => Some(shared),
            Err(SharedError::NotFound(_)) if named.is_none() => None,
            Err(e) => return Err(e.to_string()),
        };
        // `resolveCustomCABundle`: the environment's bundle, else the profile's.
        let ca_bundle = match var("AWS_CA_BUNDLE").or_else(|| {
            shared
                .as_ref()
                .map(|s| s.ca_bundle.clone())
                .filter(|b| !b.is_empty())
        }) {
            Some(path) => Some(ca_bundle(&path)?),
            None => None,
        };
        let region = first(&["AWS_REGION", "AWS_DEFAULT_REGION"])
            .or_else(|| shared.as_ref().map(|s| s.region.clone()))
            .unwrap_or_default();
        let sources = Sources::new(&region, &name, ca_bundle).await?;
        let shared = shared.unwrap_or_default();
        let (provider, cache) = match (&environment.keys, &environment.web_identity_token_file) {
            (Some(keys), _) => (fixed(keys), Cache::Plain),
            (None, Some(file)) => (
                sources.web_identity(
                    file,
                    &environment.role_arn,
                    &environment.role_session_name,
                )?,
                Cache::Plain,
            ),
            (None, None) => sources.profile(&shared, &environment).await?,
        };
        Ok(Self {
            region,
            credentials: CachedCredentials {
                provider,
                cache,
                cached: tokio::sync::Mutex::new(None),
            },
        })
    }
}

/// What `NewEnvConfig` reads for the credentials.
struct Environment {
    keys: Option<AwsCredentials>,
    web_identity_token_file: Option<String>,
    role_arn: String,
    role_session_name: String,
    container_relative_uri: Option<String>,
    container_full_uri: Option<String>,
}

/// A profile as Go's `SharedConfig` holds it, its `source_profile` resolved.
#[derive(Debug, Default, Clone)]
struct SharedConfig {
    region: String,
    keys: Option<AwsCredentials>,
    role_arn: String,
    source_profile: String,
    source: Option<Box<SharedConfig>>,
    credential_source: String,
    web_identity_token_file: String,
    role_session_name: String,
    external_id: String,
    mfa_serial: String,
    duration_seconds: Option<u64>,
    sso_session: String,
    sso_region: String,
    sso_account_id: String,
    sso_role_name: String,
    sso_start_url: String,
    login_session: String,
    credential_process: String,
    ca_bundle: String,
}

/// Why a profile could not be resolved, in Go's words.
#[derive(Debug)]
enum SharedError {
    /// `SharedConfigProfileNotExistError`.
    NotFound(String),
    Other(String),
}

impl std::fmt::Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SharedError::NotFound(profile) => {
                write!(f, "failed to get shared config profile, {profile}")
            }
            SharedError::Other(message) => f.write_str(message),
        }
    }
}

impl SharedConfig {
    /// `setFromIniSections`: profile `name`, `seen` the profiles already on the chain.
    fn resolve(
        profiles: Option<&ProfileSet>,
        name: &str,
        seen: &mut Vec<String>,
    ) -> Result<Self, SharedError> {
        let profile = profiles
            .and_then(|p| p.get_profile(name))
            .ok_or_else(|| SharedError::NotFound(name.to_string()))?;
        let mut c = Self::from_profile(profile);
        if seen.iter().any(|s| s == name) {
            // A profile met again only provides credentials (`clearAssumeRoleOptions`).
            c.role_arn.clear();
            c.source_profile.clear();
            c.role_session_name.clear();
            c.external_id.clear();
            c.mfa_serial.clear();
            c.duration_seconds = None;
        } else {
            // `validateCredentialsRequireARN`.
            let kind = if !c.source_profile.is_empty() {
                "source_profile"
            } else if !c.credential_source.is_empty() {
                "credential_source"
            } else if !c.web_identity_token_file.is_empty() {
                "web_identity_token_file"
            } else {
                ""
            };
            if !kind.is_empty() && c.role_arn.is_empty() {
                return Err(SharedError::Other(format!(
                    "credential type {kind} requires role_arn, profile {name}"
                )));
            }
        }
        if !seen.is_empty() && c.keys.is_some() {
            return Ok(c);
        }
        seen.push(name.to_string());
        let source_profile = c.source_profile.clone();
        // `validateCredentialType`: one or none.
        let kinds = [
            !source_profile.is_empty(),
            !c.credential_source.is_empty(),
            !c.credential_process.is_empty(),
            !c.web_identity_token_file.is_empty(),
        ];
        if kinds.iter().filter(|k| **k).count() > 1 {
            return Err(SharedError::Other("only one credential type may be specified per profile: source profile, credential source, credential process, web identity token".into()));
        }
        if !source_profile.is_empty() {
            // The source profile provides the credentials.
            c.credential_source.clear();
            c.credential_process.clear();
            c.web_identity_token_file.clear();
            let assume_role_error = |e: &dyn std::fmt::Display| {
                SharedError::Other(format!(
                    "failed to load assume role {}, of profile {source_profile}, {e}",
                    c.role_arn
                ))
            };
            let source = Self::resolve(profiles, &source_profile, seen).map_err(|e| match e {
                SharedError::NotFound(_) => assume_role_error(&e),
                other => other,
            })?;
            if !source.has_credentials() {
                return Err(assume_role_error(&"<nil>"));
            }
            c.source = Some(Box::new(source));
        }
        if !c.sso_session.is_empty() {
            // The session's section must exist; its region and start URL are the portal's.
            let Some(session) = profiles.and_then(|p| p.sso_session(c.sso_session.trim())) else {
                return Err(SharedError::Other(format!(
                    "failed to find SSO session section, {}",
                    c.sso_session
                )));
            };
            c.sso_region = session.get("sso_region").unwrap_or_default().to_string();
            c.sso_start_url = session.get("sso_start_url").unwrap_or_default().to_string();
        }
        Ok(c)
    }

    fn from_profile(profile: &Profile) -> Self {
        let value = |key| profile_value(profile, key);
        let (access_key_id, secret_access_key) =
            (value("aws_access_key_id"), value("aws_secret_access_key"));
        Self {
            region: value("region"),
            keys: (!access_key_id.is_empty() && !secret_access_key.is_empty()).then(|| {
                AwsCredentials {
                    access_key_id,
                    secret_access_key,
                    session_token: value("aws_session_token"),
                    expires: None,
                }
            }),
            role_arn: value("role_arn"),
            source_profile: value("source_profile"),
            source: None,
            credential_source: value("credential_source"),
            web_identity_token_file: value("web_identity_token_file"),
            role_session_name: value("role_session_name"),
            external_id: value("external_id"),
            mfa_serial: value("mfa_serial"),
            duration_seconds: value("duration_seconds").parse().ok(),
            sso_session: value("sso_session"),
            sso_region: value("sso_region"),
            sso_account_id: value("sso_account_id"),
            sso_role_name: value("sso_role_name"),
            sso_start_url: value("sso_start_url"),
            login_session: value("login_session"),
            credential_process: value("credential_process"),
            ca_bundle: value("ca_bundle"),
        }
    }

    /// `hasCredentials`.
    fn has_credentials(&self) -> bool {
        self.source.is_some()
            || !self.credential_source.is_empty()
            || !self.credential_process.is_empty()
            || !self.web_identity_token_file.is_empty()
            || self.has_sso()
            || self.keys.is_some()
    }

    /// `hasSSOConfiguration`.
    fn has_sso(&self) -> bool {
        !self.sso_session.is_empty()
            || !self.sso_region.is_empty()
            || !self.sso_account_id.is_empty()
            || !self.sso_start_url.is_empty()
            || !self.sso_role_name.is_empty()
    }
}

fn profile_value(profile: &Profile, key: &str) -> String {
    profile.get(key).unwrap_or_default().to_string()
}

/// A provider of fixed credentials.
fn fixed(credentials: &AwsCredentials) -> SharedCredentialsProvider {
    SharedCredentialsProvider::new(aws_credential_types::Credentials::new(
        credentials.access_key_id.clone(),
        credentials.secret_access_key.clone(),
        Some(credentials.session_token.clone()).filter(|t| !t.is_empty()),
        None,
        "Environment",
    ))
}

/// A provider that fails with its message (`StaticCredentialsEmptyError`).
#[derive(Debug)]
struct Failing(&'static str);

/// An error in Go's words, read without the SDK's own context around it.
#[derive(Debug)]
struct GoError(&'static str);

impl std::fmt::Display for GoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for GoError {}

impl ProvideCredentials for Failing {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::ready(Err(
            aws_credential_types::provider::error::CredentialsError::not_loaded(GoError(self.0)),
        ))
    }
}

/// A `credential_process` killed after a minute (`processcreds.DefaultTimeout`).
#[derive(Debug)]
struct TimedProcess(aws_config::credential_process::CredentialProcessProvider);

impl ProvideCredentials for TimedProcess {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async move {
            tokio::time::timeout(Duration::from_secs(60), self.0.provide_credentials())
                .await
                .unwrap_or_else(|_| {
                    Err(
                        aws_credential_types::provider::error::CredentialsError::provider_error(
                            "error in credential_process: context deadline exceeded",
                        ),
                    )
                })
        })
    }
}

/// A provider being resolved: it, and how its credentials are cached.
type Resolving<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<(SharedCredentialsProvider, Cache), String>>
            + Send
            + 'a,
    >,
>;

/// The SDK's providers of credentials, with the HTTP client and the region (the AWS
/// configuration's, not `*_AWS_IAM_REGION`) their AWS calls use.
struct Sources {
    config: ProviderConfig,
    sdk: aws_types::SdkConfig,
    /// For the container's endpoint, whose client Go makes apart: without the CA bundle.
    container: ProviderConfig,
}

impl Sources {
    async fn new(region: &str, profile: &str, ca_bundle: Option<Vec<u8>>) -> Result<Self, String> {
        let region = (!region.is_empty()).then(|| Region::new(region.to_string()));
        let http = http_client(ca_bundle)?;
        let config = ProviderConfig::default()
            .with_region(region.clone())
            .with_http_client(http.clone());
        let container = ProviderConfig::default()
            .with_region(region.clone())
            .with_http_client(http_client(None)?);
        // The region given, even none: aws-config would otherwise ask IMDS for one.
        let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .http_client(http)
            .no_credentials()
            .profile_name(profile)
            .region(region)
            .load()
            .await;
        Ok(Self {
            config,
            sdk,
            container,
        })
    }

    /// `resolveCredsFromProfile`: the provider, and how its credentials are cached.
    fn profile<'a>(&'a self, c: &'a SharedConfig, environment: &'a Environment) -> Resolving<'a> {
        Box::pin(async move {
            let (base, window) = if let Some(source) = &c.source {
                self.profile(source, environment).await?
            } else if let Some(keys) = &c.keys {
                (fixed(keys), Cache::Plain)
            } else if !c.credential_source.is_empty() {
                match c.credential_source.as_str() {
                    "Ec2InstanceMetadata" => (self.imds(), Cache::Imds),
                    "Environment" => (
                        match &environment.keys {
                            Some(keys) => fixed(keys),
                            None => SharedCredentialsProvider::new(Failing(
                                "static credentials are empty",
                            )),
                        },
                        Cache::Plain,
                    ),
                    "EcsContainer" => match (
                        &environment.container_relative_uri,
                        &environment.container_full_uri,
                    ) {
                        (None, None) => return Err("EcsContainer was specified as the credential_source, but neither 'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI' or AWS_CONTAINER_CREDENTIALS_FULL_URI' was set".into()),
                        (relative, full) => self.container(relative.is_some(), full.as_deref()).await?,
                    },
                    _ => {
                        return Err(
                            "credential_source values must be EcsContainer, Ec2InstanceMetadata, or Environment".into(),
                        );
                    }
                }
            } else if !c.web_identity_token_file.is_empty() {
                // Its role is the one it assumes.
                return Ok((
                    self.web_identity(
                        &c.web_identity_token_file,
                        &c.role_arn,
                        &c.role_session_name,
                    )?,
                    Cache::Plain,
                ));
            } else if c.has_sso() {
                let mut sso = aws_config::sso::SsoCredentialsProvider::builder()
                    .configure(&self.config)
                    .account_id(&c.sso_account_id)
                    .role_name(&c.sso_role_name)
                    .start_url(&c.sso_start_url)
                    .region(Region::new(c.sso_region.clone()));
                if !c.sso_session.is_empty() {
                    sso = sso.session_name(c.sso_session.trim());
                }
                (SharedCredentialsProvider::new(sso.build()), Cache::Plain)
            } else if !c.login_session.is_empty() {
                (
                    SharedCredentialsProvider::new(
                        aws_config::login::LoginCredentialsProvider::builder(&c.login_session)
                            .configure(&self.config)
                            .build(),
                    ),
                    Cache::Plain,
                )
            } else if !c.credential_process.is_empty() {
                (
                    SharedCredentialsProvider::new(TimedProcess(
                        aws_config::credential_process::CredentialProcessProvider::new(
                            c.credential_process.clone(),
                        ),
                    )),
                    Cache::Plain,
                )
            } else if environment.container_relative_uri.is_some()
                || environment.container_full_uri.is_some()
            {
                self.container(
                    environment.container_relative_uri.is_some(),
                    environment.container_full_uri.as_deref(),
                )
                .await?
            } else {
                (self.imds(), Cache::Imds)
            };
            if c.role_arn.is_empty() {
                return Ok((base, window));
            }
            // `credsFromAssumeRole`.
            if !c.mfa_serial.is_empty() {
                return Err(
                    "assume role with MFA enabled, but AssumeRoleTokenProvider session option not set."
                        .into(),
                );
            }
            let mut role = aws_config::sts::AssumeRoleProvider::builder(&c.role_arn)
                .configure(&self.sdk)
                .session_name(go_session_name("aws-go-sdk-", &c.role_session_name))
                // `DefaultDuration`, unless the profile's is over 15 minutes.
                .session_length(Duration::from_secs(
                    c.duration_seconds
                        .filter(|s| s / 60 > 15)
                        .unwrap_or(15 * 60),
                ));
            if !c.external_id.is_empty() {
                role = role.external_id(&c.external_id);
            }
            Ok((
                SharedCredentialsProvider::new(role.build_from_provider(base).await),
                Cache::Plain,
            ))
        })
    }

    /// `assumeWebIdentity`.
    fn web_identity(
        &self,
        file: &str,
        role_arn: &str,
        session_name: &str,
    ) -> Result<SharedCredentialsProvider, String> {
        if role_arn.is_empty() {
            return Err("role ARN is not set".into());
        }
        Ok(SharedCredentialsProvider::new(
            aws_config::web_identity_token::WebIdentityTokenCredentialsProvider::builder()
                .configure(&self.config)
                .static_configuration(aws_config::web_identity_token::StaticConfiguration {
                    web_identity_token_file: file.into(),
                    role_arn: role_arn.into(),
                    session_name: go_session_name("", session_name),
                })
                .build(),
        ))
    }

    /// The container's endpoint (`resolveHTTPCredProvider`), its full URI checked as
    /// `resolveLocalHTTPCredProvider` checks it; its cache expires credentials 5 minutes early.
    async fn container(
        &self,
        relative: bool,
        full_uri: Option<&str>,
    ) -> Result<(SharedCredentialsProvider, Cache), String> {
        if let (false, Some(uri)) = (relative, full_uri) {
            check_container_uri(uri).await?;
        }
        Ok((
            SharedCredentialsProvider::new(
                aws_config::ecs::EcsCredentialsProvider::builder()
                    .configure(&self.container)
                    .build(),
            ),
            Cache::Container,
        ))
    }

    fn imds(&self) -> SharedCredentialsProvider {
        SharedCredentialsProvider::new(
            aws_config::imds::credentials::ImdsCredentialsProvider::builder()
                .configure(&self.config)
                .build(),
        )
    }
}

/// The session name Go's providers send: the one configured, or the time in nanoseconds after
/// the provider's `prefix`.
fn go_session_name(prefix: &str, configured: &str) -> String {
    match configured {
        "" => format!("{prefix}{}", jiff::Timestamp::now().as_nanosecond()),
        name => name.to_string(),
    }
}

/// `resolveLocalHTTPCredProvider`'s checks: over plain HTTP, only a loopback, ECS or EKS host.
async fn check_container_uri(uri: &str) -> Result<(), String> {
    let parsed: http::Uri = uri.parse().map_err(|e| format!("invalid URL, {e}"))?;
    // `URL.Hostname`: without the port, nor an IPv6 address' brackets.
    let host = parsed
        .host()
        .unwrap_or_default()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    if host.is_empty() {
        return Err("unable to parse host from local HTTP cred provider URL".into());
    }
    if parsed.scheme_str() != Some("http") {
        return Ok(());
    }
    let allowed = |ip: std::net::IpAddr| {
        ip.is_loopback()
            || ip == std::net::IpAddr::from([169, 254, 170, 2])
            || ip == std::net::IpAddr::from([169, 254, 170, 23])
            || ip == "fd00:ec2::23".parse::<std::net::IpAddr>().unwrap()
    };
    let ok = match host.parse::<std::net::IpAddr>() {
        Ok(ip) => allowed(ip),
        Err(_) => {
            let addresses = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|e| format!("failed to resolve host {host:?}, {e}"))?;
            addresses.into_iter().all(|a| allowed(a.ip()))
        }
    };
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid endpoint host, {host:?}, only loopback/ecs/eks hosts are allowed"
        ))
    }
}

/// A CA bundle's PEM, which must hold a certificate (`AppendCertsFromPEM`).
fn ca_bundle(path: &str) -> Result<Vec<u8>, String> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let pem = crate::tls::read(path)?;
    let mut roots = rustls::RootCertStore::empty();
    let added = CertificateDer::pem_slice_iter(&pem)
        .filter_map(Result::ok)
        .filter(|c| roots.add(c.clone()).is_ok())
        .count();
    if added == 0 {
        return Err("failed to load custom CA bundle PEM file".into());
    }
    Ok(pem)
}

/// The SDK's HTTP client, over rustls with ring, through the environment's proxies as Go's;
/// with a CA bundle, its certificates are the only roots.
fn http_client(
    ca_bundle: Option<Vec<u8>>,
) -> Result<aws_types::sdk_config::SharedHttpClient, String> {
    use aws_smithy_http_client::{Connector, proxy::ProxyConfig, tls};
    let mut context = tls::TlsContext::builder();
    if let Some(pem) = ca_bundle {
        context = context.with_trust_store(
            tls::TrustStore::empty()
                .with_native_roots(false)
                .with_pem_certificate(pem),
        );
    }
    let context = context.build().map_err(|e| e.to_string())?;
    Ok(
        aws_smithy_http_client::Builder::new().build_with_connector_fn(
            move |settings, components| {
                let mut builder = Connector::builder();
                builder.set_connector_settings(settings.cloned());
                if let Some(components) = components {
                    builder.set_sleep_impl(components.sleep_impl());
                }
                builder
                    .proxy_config(ProxyConfig::from_env())
                    .tls_provider(tls::Provider::Rustls(
                        tls::rustls_provider::CryptoMode::Ring,
                    ))
                    .tls_context(context.clone())
                    .build()
            },
        ),
    )
}

/// How Go's `CredentialsCache` keeps a provider's credentials.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Cache {
    /// Until they expire.
    Plain,
    /// The container's: 5 minutes early (`ExpiryWindow`).
    Container,
    /// IMDS': stale ones (IMDS unreachable) kept 5 to 15 minutes more
    /// (`ec2rolecreds.Provider.HandleFailToRefresh`).
    Imds,
}

/// `aws.CredentialsCache`: the credentials kept until they expire.
struct CachedCredentials {
    provider: SharedCredentialsProvider,
    cache: Cache,
    cached: tokio::sync::Mutex<Option<AwsCredentials>>,
}

impl CachedCredentials {
    async fn retrieve(&self) -> Result<AwsCredentials, String> {
        let mut cached = self.cached.lock().await;
        if let Some(credentials) = &*cached
            && credentials
                .expires
                .is_none_or(|expires| jiff::Timestamp::now() < expires)
        {
            return Ok(credentials.clone());
        }
        let credentials =
            self.provider.provide_credentials().await.map_err(|e| {
                format!("failed to refresh cached credentials, {}", error_chain(&e))
            })?;
        let window = match self.cache {
            Cache::Container => jiff::SignedDuration::from_secs(5 * 60),
            _ => jiff::SignedDuration::ZERO,
        };
        let mut fresh = AwsCredentials {
            access_key_id: credentials.access_key_id().to_string(),
            secret_access_key: credentials.secret_access_key().to_string(),
            session_token: credentials.session_token().unwrap_or_default().to_string(),
            expires: credentials
                .expiry()
                .and_then(|e| jiff::Timestamp::try_from(e).ok())
                .map(|e| e - window),
        };
        // aws-config's IMDS provider answers an outage with the last credentials, expired.
        let now = jiff::Timestamp::now();
        if self.cache == Cache::Imds && fresh.expires.is_some_and(|e| e <= now) {
            let extension = 5.0 * 60.0 + crate::random::float64() * 10.0 * 60.0;
            fresh.expires = Some(now + jiff::SignedDuration::from_secs_f64(extension));
        }
        *cached = Some(fresh.clone());
        Ok(fresh)
    }
}

/// An SDK error with its causes, as Go's wrapped errors read (Go's own words alone).
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(go) = cause.downcast_ref::<GoError>() {
            return go.to_string();
        }
        text.push_str(&format!(", {cause}"));
        source = cause.source();
    }
    text
}

/// `buildToken`: `http://<cache>/?Action=connect&User=<user>&X-Amz-Expires=900` (with
/// `ResourceType=ServerlessCache` for a serverless cache) presigned at `now` as Go's
/// `v4.Signer.PresignHTTP` presigns it, without its `http://`.
pub fn token(credentials: &AwsCredentials, target: &Target, now: jiff::Timestamp) -> String {
    let date_time = now.strftime("%Y%m%dT%H%M%SZ").to_string();
    let date = &date_time[..8];
    let scope = format!("{date}/{}/{SIGNING_SERVICE}/aws4_request", target.region);
    let mut query = vec![
        ("Action", "connect".to_string()),
        ("User", target.user_id.clone()),
        ("X-Amz-Expires", TOKEN_VALIDITY.to_string()),
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".into()),
        (
            "X-Amz-Credential",
            format!("{}/{scope}", credentials.access_key_id),
        ),
        ("X-Amz-Date", date_time.clone()),
        ("X-Amz-SignedHeaders", "host".into()),
    ];
    if target.serverless {
        query.push(("ResourceType", "ServerlessCache".into()));
    }
    if !credentials.session_token.is_empty() {
        query.push(("X-Amz-Security-Token", credentials.session_token.clone()));
    }
    // `url.Values.Encode`: sorted by key, `+` for a space then made `%20`.
    query.sort_by(|a, b| a.0.cmp(b.0));
    let query = query
        .iter()
        .map(|(key, value)| format!("{}={}", query_escape(key), query_escape(value)))
        .collect::<Vec<_>>()
        .join("&");
    let host = &target.cache_name;
    let canonical_request = format!("GET\n/\n{query}\nhost:{host}\n\nhost\n{EMPTY_PAYLOAD_HASH}");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{date_time}\n{scope}\n{}",
        hex(digest::digest(&digest::SHA256, canonical_request.as_bytes()).as_ref())
    );
    let mut key = format!("AWS4{}", credentials.secret_access_key).into_bytes();
    for part in [date, &target.region, SIGNING_SERVICE, "aws4_request"] {
        key = sign(&key, part.as_bytes());
    }
    let signature = hex(&sign(&key, string_to_sign.as_bytes()));
    format!("{host}/?{query}&X-Amz-Signature={signature}")
}

fn sign(key: &[u8], data: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
        .as_ref()
        .to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Go's `url.QueryEscape`, a space as `%20`: all but `A-Za-z0-9-_.~` percent-encoded.
fn query_escape(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            escaped.push(b as char);
        } else {
            escaped.push_str(&format!("%{b:02X}"));
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials(session_token: &str) -> AwsCredentials {
        AwsCredentials {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            session_token: session_token.into(),
            expires: None,
        }
    }

    fn target(user_id: &str, serverless: bool) -> Target {
        Target {
            region: "us-east-1".into(),
            cache_name: "mycache".into(),
            user_id: user_id.into(),
            serverless,
        }
    }

    fn profiles(config: &str) -> ProfileSet {
        let fs = aws_types::os_shim_internal::Fs::from_slice(&[("/aws/config", config)]);
        let env = aws_types::os_shim_internal::Env::from_slice(&[
            ("AWS_CONFIG_FILE", "/aws/config"),
            ("AWS_SHARED_CREDENTIALS_FILE", "/aws/none"),
        ]);
        futures_ready(aws_config::profile::load(
            &fs,
            &env,
            &Default::default(),
            None,
        ))
        .unwrap()
    }

    fn futures_ready<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    /// A profile of an `sso-session` takes its region and start URL from the session's section.
    #[test]
    fn sso_sessions_give_the_portal() {
        let set = profiles(
            "[profile p]\nsso_session = s\nsso_account_id = 1\nsso_role_name = r\n\
             [sso-session s]\nsso_region = eu-west-1\nsso_start_url = https://start\n",
        );
        let c = SharedConfig::resolve(Some(&set), "p", &mut Vec::new()).unwrap();
        assert_eq!(
            (c.sso_region.as_str(), c.sso_start_url.as_str()),
            ("eu-west-1", "https://start")
        );
    }

    /// Stale credentials from IMDS (unreachable) are kept 5 to 15 minutes more; others are
    /// asked for again.
    #[test]
    fn stale_imds_credentials_are_extended() {
        let expired = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        for (cache, extended) in [(Cache::Imds, true), (Cache::Plain, false)] {
            let cached = CachedCredentials {
                provider: SharedCredentialsProvider::new(aws_credential_types::Credentials::new(
                    "AKID",
                    "secret",
                    None,
                    Some(expired),
                    "test",
                )),
                cache,
                cached: tokio::sync::Mutex::new(None),
            };
            let expires = futures_ready(cached.retrieve()).unwrap().expires.unwrap();
            let left = expires.duration_since(jiff::Timestamp::now());
            let in_range = left > jiff::SignedDuration::from_secs(4 * 60 + 59)
                && left < jiff::SignedDuration::from_secs(15 * 60);
            assert_eq!(in_range, extended, "{cache:?}: {left:?}");
        }
    }

    /// The tokens Go's `v4.Signer.PresignHTTP` (aws-sdk-go-v2 v1.45.1) gives for the same
    /// request at 2026-01-02T03:04:05Z.
    #[test]
    fn tokens_are_presigned_as_go_presigns_them() {
        let now: jiff::Timestamp = "2026-01-02T03:04:05Z".parse().unwrap();
        assert_eq!(
            token(&credentials(""), &target("app-user", false), now),
            "mycache/?Action=connect&User=app-user&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDEXAMPLE%2F20260102%2Fus-east-1%2Felasticache%2Faws4_request&X-Amz-Date=20260102T030405Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=1fef613546980232d67cdc38a6e5e6b5613efde8668c1d49aa854da913af026c"
        );
        assert_eq!(
            token(
                &credentials("FwoG/tok+en="),
                &target("app-user", false),
                now
            ),
            "mycache/?Action=connect&User=app-user&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDEXAMPLE%2F20260102%2Fus-east-1%2Felasticache%2Faws4_request&X-Amz-Date=20260102T030405Z&X-Amz-Expires=900&X-Amz-Security-Token=FwoG%2Ftok%2Ben%3D&X-Amz-SignedHeaders=host&X-Amz-Signature=8bd9c6853e77fe3294c3be8880af97ff7c1f794fdd273cd076722f541e50148b"
        );
        assert_eq!(
            token(&credentials(""), &target("we ird*user~", true), now),
            "mycache/?Action=connect&ResourceType=ServerlessCache&User=we%20ird%2Auser~&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDEXAMPLE%2F20260102%2Fus-east-1%2Felasticache%2Faws4_request&X-Amz-Date=20260102T030405Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=63d31ab8cbb4592ca7a1f0684eba6798061899a181618c970155f156a65667c6"
        );
    }
}
