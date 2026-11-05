fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/envoy/service/ratelimit/v3/rls.proto",
        "proto/envoy/extensions/common/ratelimit/v3/ratelimit.proto",
        "proto/envoy/type/v3/ratelimit_unit.proto",
        "proto/envoy/config/core/v3/base.proto",
        "proto/envoy/service/discovery/v3/discovery.proto",
        "proto/ratelimit/config/ratelimit/v3/rls_conf.proto",
        "proto/grpc/health/v1/health.proto",
        "proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
    ];
    let fds = protox::compile(protos, ["proto"])?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds(fds)?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
