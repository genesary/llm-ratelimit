//! Envoy rate limit service protos, compiled with `protox` (no `protoc` needed).

pub mod envoy {
    pub mod config {
        pub mod core {
            pub mod v3 {
                include!(concat!(env!("OUT_DIR"), "/envoy.config.core.v3.rs"));
            }
        }
    }
    pub mod extensions {
        pub mod common {
            pub mod ratelimit {
                pub mod v3 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/envoy.extensions.common.ratelimit.v3.rs"
                    ));
                }
            }
        }
    }
    pub mod service {
        pub mod discovery {
            pub mod v3 {
                include!(concat!(env!("OUT_DIR"), "/envoy.service.discovery.v3.rs"));
            }
        }
        pub mod ratelimit {
            pub mod v3 {
                include!(concat!(env!("OUT_DIR"), "/envoy.service.ratelimit.v3.rs"));
            }
        }
    }
    pub mod r#type {
        pub mod v3 {
            include!(concat!(env!("OUT_DIR"), "/envoy.r#type.v3.rs"));
        }
    }
}

/// The `google.rpc.Status` of xDS NACKs.
pub mod google {
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
}

/// envoyproxy/ratelimit's xDS resource (`RateLimitConfig`).
pub mod ratelimit {
    pub mod config {
        pub mod ratelimit {
            pub mod v3 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/ratelimit.config.ratelimit.v3.rs"
                ));
            }
        }
    }
}

/// gRPC's health checking protocol (`grpc.health.v1`).
pub mod grpc {
    pub mod health {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/grpc.health.v1.rs"));
        }
    }
}

/// OpenTelemetry's OTLP protos (opentelemetry-proto), for exporting traces.
pub mod opentelemetry {
    pub mod proto {
        pub mod collector {
            pub mod trace {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.collector.trace.v1.rs"
                    ));
                }
            }
        }
        pub mod common {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.common.v1.rs"
                ));
            }
        }
        pub mod resource {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.resource.v1.rs"
                ));
            }
        }
        pub mod trace {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.trace.v1.rs"));
            }
        }
    }
}
