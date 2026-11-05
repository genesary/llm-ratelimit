//! The `llm-ratelimit` service.

use llm_ratelimit::settings::Settings;

/// The image's static (musl) build: musl's allocator serialises threads.
#[cfg(target_env = "musl")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn environment(key: &str) -> Option<String> {
    std::env::var_os(key).map(|v| v.to_string_lossy().into_owned())
}

#[tokio::main]
async fn main() {
    llm_ratelimit::log::init();
    let result = match Settings::from_lookup(&environment) {
        Ok(settings) => llm_ratelimit::runner::run(settings, &environment).await,
        Err(e) => Err(e.to_string().into()),
    };
    if let Err(failure) = result {
        failure.exit();
    }
}
