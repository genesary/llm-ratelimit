//! `ConfigXdsProtoToYaml` (`src/config/config_xds.go`): an xDS `RateLimitConfig` as the YAML
//! schema it stands for. The proto has no `value_to_metric` nor `share_threshold`.

use super::schema::{YamlDescriptor, YamlRateLimit, YamlRoot};
use llm_ratelimit_proto::ratelimit::config::ratelimit::v3 as pb;
use prost_types::{Struct, Value, value::Kind};

pub(super) fn root(config: &pb::RateLimitConfig) -> YamlRoot {
    YamlRoot {
        domain: config.domain.clone(),
        descriptors: descriptors(&config.descriptors),
    }
}

fn descriptors(list: &[pb::RateLimitDescriptor]) -> Vec<YamlDescriptor> {
    list.iter()
        .map(|d| YamlDescriptor {
            key: d.key.clone(),
            value: d.value.clone(),
            rate_limit: d.rate_limit.as_ref().map(|policy| YamlRateLimit {
                requests_per_unit: policy.requests_per_unit,
                // `RateLimitUnit.String()`: its values are the response's units.
                unit: super::unit_name(policy.unit),
                unlimited: policy.unlimited,
                name: policy.name.clone(),
                replaces: policy.replaces.iter().map(|r| r.name.clone()).collect(),
            }),
            // The extensions are YAML only (D28).
            rate_limits: None,
            overflow: String::new(),
            descriptors: descriptors(&d.descriptors),
            shadow_mode: d.shadow_mode,
            quota_mode: d.quota_mode,
            detailed_metric: d.detailed_metric,
            value_to_metric: false,
            share_threshold: false,
            metadata: metadata(d.metadata.as_ref()),
        })
        .collect()
}

/// `Struct.AsMap`, then the loader's `structpb.NewStruct`: none when empty, and a value of no
/// kind is a null.
fn metadata(metadata: Option<&Struct>) -> Option<Struct> {
    metadata.filter(|m| !m.fields.is_empty()).map(as_struct)
}

fn as_struct(s: &Struct) -> Struct {
    Struct {
        fields: s
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), value(v)))
            .collect(),
    }
}

fn value(v: &Value) -> Value {
    let kind = match &v.kind {
        None => Kind::NullValue(0),
        Some(Kind::StructValue(s)) => Kind::StructValue(as_struct(s)),
        Some(Kind::ListValue(l)) => Kind::ListValue(prost_types::ListValue {
            values: l.values.iter().map(value).collect(),
        }),
        Some(other) => other.clone(),
    };
    Value { kind: Some(kind) }
}
