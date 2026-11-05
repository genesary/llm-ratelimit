#!/usr/bin/env bash
# Envoy Gateway with our image (docs/design.md D33): on a kind cluster, Envoy Gateway deploys
# the rate limit service (xDS config over TLS, gRPC over TLS, its own security context) for a
# global rate limit; the same requests through Envoy get the same answers with upstream's
# image and with ours, only the chart's `global.images.ratelimit.image` changed.
#
# Usage: test.sh IMAGE, a local image named REGISTRY/REPOSITORY:TAG (Envoy Gateway's chart
# splits it so, e.g. localhost/llm-ratelimit:ci). Needs kind, kubectl, helm, jq, curl and the container
# CLI (CONTAINER_CLI, docker by default). KEEP_CLUSTER=1 keeps the cluster.
set -euo pipefail

image=${1:?usage: test.sh REGISTRY/REPOSITORY:TAG}
case "$image" in
  */*:*) ;;
  *) echo "Envoy Gateway's chart needs REGISTRY/REPOSITORY:TAG, not $image" >&2; exit 2 ;;
esac
cli=${CONTAINER_CLI:-docker}
here=$(cd "$(dirname "$0")" && pwd)
chart=oci://docker.io/envoyproxy/gateway-helm
chart_version=v1.9.3
chart_digest=sha256:be1b8f163a338f94e7d6532336c08d6856fc70a0b790ac14a2826a54d23e0dd3
gateway_image=docker.io/envoyproxy/gateway:v1.9.3@sha256:192fc9bdadb6d2462e8cf7bd702ce3dd9648f3310c6038787448a1f509b7cd9f
upstream=docker.io/envoyproxy/ratelimit:f4e76690@sha256:83b1664a194ff9fc1ff30b0868573115a2d01a1b6e2a3f8961724f9a79bf1cd0
cluster=ratelimit-eg
ns=envoy-gateway-system
work=$(mktemp -d)
forward=

cleanup() {
  stop_forward
  [ -n "${KEEP_CLUSTER:-}" ] || kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

# Runs `$@` until it succeeds, up to `$1` times a second apart; fails saying what it waited for.
retry() {
  local tries=$1
  shift
  for _ in $(seq "$tries"); do
    "$@" && return
    sleep 1
  done
  echo "gave up waiting for: $*" >&2
  return 1
}

# Forwards local port `$2` to port `$3` of `$1`, until `stop_forward`.
start_forward() {
  kubectl -n "$ns" port-forward "$1" "$2:$3" >/dev/null 2>&1 &
  forward=$!
}

stop_forward() {
  if [ -n "$forward" ]; then
    kill "$forward" 2>/dev/null || true
    wait "$forward" 2>/dev/null || true
    forward=
  fi
}

# The rate limit pod running image `$1` (and not being deleted).
pod_with() {
  kubectl -n "$ns" get pod -l app.kubernetes.io/name=envoy-ratelimit -o json |
    jq -r --arg image "$1" '[.items[]
      | select(.metadata.deletionTimestamp == null)
      | select(.spec.containers[0].image == $image)
      | .metadata.name][0] // empty'
}

# Whether a rate limit pod runs image `$1`.
running() {
  [ -n "$(pod_with "$1")" ]
}

# Whether `$1` (a port-forwarded debug port) serves a config with the policy's two limits.
holds_policy() {
  curl -sf --max-time 2 -o "$work/rlconfig" "http://127.0.0.1:$1/rlconfig" &&
    grep -q "requests_per_unit=1," "$work/rlconfig" &&
    grep -q "requests_per_unit=3," "$work/rlconfig"
}

# Whether the Gateway's Envoy deployment exists.
envoy_deployed() {
  [ -n "$(kubectl -n "$ns" get deployment -l gateway.envoyproxy.io/owning-gateway-name=eg -o name)" ]
}

kind create cluster --name "$cluster" --wait 3m
"$cli" save "$image" -o "$work/image.tar"
kind load image-archive "$work/image.tar" --name "$cluster"
kubectl apply -f "$here/redis.yaml"
kubectl -n redis-system rollout status deployment/redis --timeout 3m

# The chart, checked against its digest.
helm pull "$chart" --version "$chart_version" -d "$work" 2>&1 | tee "$work/pull.txt"
grep -qF "Digest: $chart_digest" "$work/pull.txt"
chart_file=$(ls "$work"/gateway-helm-*.tgz)

# Envoy Gateway with `$1` as the rate limit image, its rate limit pod running that image.
install() {
  helm upgrade --install eg "$chart_file" -n "$ns" --create-namespace -f "$here/values.yaml" \
    --set global.images.envoyGateway.image="$gateway_image" \
    --set global.images.ratelimit.image="$1" --wait --timeout 5m
  # The controller reads its configuration at start.
  kubectl -n "$ns" rollout restart deployment/envoy-gateway
  kubectl -n "$ns" rollout status deployment/envoy-gateway --timeout 3m
  retry 240 running "$1"
  kubectl -n "$ns" wait --for=condition=Ready "pod/$(pod_with "$1")" --timeout 3m
}

# The config the pod running image `$1` loaded, once it holds the policy.
loaded() {
  start_forward "pod/$(pod_with "$1")" 16070 6070
  retry 120 holds_policy 16070
  stop_forward
  # Upstream lists the domains in Go's random map order.
  sort "$work/rlconfig"
}

# Envoy's count of rate limit calls on the route with outcome `$1` (`ok`, or `error`: failed,
# so let through), from its admin port.
calls() {
  local envoy
  envoy=$(kubectl -n "$ns" get pod -l gateway.envoyproxy.io/owning-gateway-name=eg \
    -o jsonpath='{.items[0].metadata.name}')
  start_forward "pod/$envoy" 19000 19000
  retry 30 curl -sf --max-time 2 -o "$work/stats" http://127.0.0.1:19000/stats
  stop_forward
  # Envoy creates a counter at its first count.
  awk -v outcome="$1" '$1 ~ "^cluster\\.httproute/.*\\.ratelimit\\." outcome ":$" { n += $2 }
    END { print n + 0 }' "$work/stats"
}

# The answers to a sequence of requests through Envoy: the status and the rate limit headers
# (the reset's value left out, it depends on the time).
probe() {
  kubectl -n redis-system exec deployment/redis -- redis-cli FLUSHALL >/dev/null
  local service
  service=$(kubectl -n "$ns" get service \
    -l gateway.envoyproxy.io/owning-gateway-name=eg -o jsonpath='{.items[0].metadata.name}')
  start_forward "service/$service" 18080 80
  # The route answers (for a user of its own, not counted below).
  retry 60 curl -sf -o /dev/null --max-time 2 -H "Host: www.example.com" \
    -H "x-user-id: ready" http://127.0.0.1:18080/
  for user in a a a a one one b; do
    curl -s -o /dev/null -D - --max-time 5 -H "Host: www.example.com" -H "x-user-id: $user" \
      http://127.0.0.1:18080/ |
      tr -d '\r' | grep -iE '^HTTP/|^x-ratelimit-(limit|remaining)' | sed "s/^/$user: /"
  done
  stop_forward
}

install "$upstream"
kubectl apply -f "$here/gateway.yaml"
# Accepted (kind has no load balancer to program it), its Envoy deployment available.
kubectl wait --for=condition=Accepted gateway/eg --timeout 3m
retry 60 envoy_deployed
kubectl -n "$ns" wait --for=condition=Available deployment \
  -l gateway.envoyproxy.io/owning-gateway-name=eg --timeout 3m
kubectl rollout status deployment/backend --timeout 3m
loaded "$upstream" >"$work/upstream.txt"
probe >>"$work/upstream.txt"

install "$image"
loaded "$image" >"$work/ours.txt"
ok_before=$(calls ok)
failed_before=$(calls error)
probe >>"$work/ours.txt"
ok_after=$(calls ok)
failed_after=$(calls error)

cat "$work/upstream.txt"
# The limit applies with upstream's image.
grep -q "a: HTTP/1.1 429" "$work/upstream.txt"
grep -q "one: HTTP/1.1 429" "$work/upstream.txt"
diff -u "$work/upstream.txt" "$work/ours.txt"
# Envoy asked our service (the counters are found), and none of its calls failed: none
# timed out at Envoy Gateway's 20 ms.
if [ "$ok_after" -le "$ok_before" ] || [ "$failed_after" != "$failed_before" ]; then
  echo "rate limit calls: ok $ok_before then $ok_after, failed $failed_before then $failed_after" >&2
  exit 1
fi
# Our service ran as Envoy Gateway runs it: as 65534, on a read-only file system, without
# capabilities.
kubectl -n "$ns" get pod "$(pod_with "$image")" -o json |
  jq -e '.spec.containers[0].securityContext
    | .runAsUser == 65534 and .readOnlyRootFilesystem == true
      and .capabilities.drop == ["ALL"]' >/dev/null
echo "same answers with $image as with upstream's image"
