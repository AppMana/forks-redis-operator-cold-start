#!/usr/bin/env bash
# Local fixture only: inject a stale quorum, then verify operator repair.
# No pod deletion, scaling, credential changes or PVC changes.
set -euo pipefail
trap 'echo "FAIL: quorum qualification at line $LINENO" >&2' ERR
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
namespace=redis-upgrade-canary-auth-syntax
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$namespace")
root="$(cd "$(dirname "$0")/../.." && pwd)"
health() {
  CANARY_AUTH_NAMESPACE="$namespace" CANARY_AUTH_VALUE=preserved-auth-syntax-20261021 \
    bash "$root/hack/recovery/check-auth-rotation.sh" "$config"
}
cli() {
  local pod="$1"; shift
  "${k[@]}" get secret sentinel-auth -o json | jq -r '.data.password|@base64d' |
    "${k[@]}" exec -i "$pod" -c sentinel -- sh -ec \
      'IFS= read -r REDISCLI_AUTH; export REDISCLI_AUTH; exec redis-cli --raw -p 26379 "$@"' sh "$@" 2>/dev/null
}
identity() {
  "${k[@]}" get pod upgrade-0 upgrade-1 upgrade-2 upgrade-s-0 upgrade-s-1 upgrade-s-2 -o json |
    jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'
  "${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'
  for i in 0 1 2; do cli "upgrade-s-$i" SENTINEL MYID; done
}
[[ "$("${k[@]}" get redisreplication upgrade -o jsonpath='{.spec.sentinel.size}')" == 3 ]]
health
before="$(identity)"
for i in 0 1 2; do
  [[ "$(cli "upgrade-s-$i" SENTINEL SET mymaster quorum 3)" == OK ]]
done
# Wait for the controller's periodic reconcile. CR annotations are propagated
# into Redis pod templates, so an annotation trigger would cause a rollout.
deadline=$((SECONDS+120)); converged=false
while (( SECONDS < deadline )); do
  converged=true
  for i in 0 1 2; do
    quorum="$(cli "upgrade-s-$i" SENTINEL MASTER mymaster | awk '$0=="quorum" {getline; print}')"
    [[ "$quorum" == 2 ]] || converged=false
  done
  [[ "$converged" == true ]] && break
  sleep 2
done
[[ "$converged" == true ]]
[[ "$(identity)" == "$before" ]]
health
echo 'PASS: operator restored quorum on all Sentinels, with pod/PVC identities, Sentinel IDs and data retained'

if [[ "${TEST_CONFIGURED_QUORUM:-false}" == true ]]; then
  # A QUORUM environment edit legitimately rolls Sentinel pods under the
  # default strategy. Require durable identity and Redis pods to survive.
  durable_identity() {
    "${k[@]}" get pod upgrade-0 upgrade-1 upgrade-2 -o json |
      jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'
    "${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'
    for i in 0 1 2; do cli "upgrade-s-$i" SENTINEL MYID; done
  }
  [[ "$("${k[@]}" get redisreplication upgrade -o jsonpath='{.spec.sentinel.quorum}')" == 2 ]]
  durable_before="$(durable_identity)"
  for desired in 3 2; do
    "${k[@]}" patch redisreplication upgrade --type=merge \
      -p "{\"spec\":{\"sentinel\":{\"quorum\":\"$desired\"}}}" >/dev/null
    deadline=$((SECONDS+180)); converged=false
    while (( SECONDS < deadline )); do
      converged=true
      for i in 0 1 2; do
        actual="$(cli "upgrade-s-$i" SENTINEL MASTER mymaster 2>/dev/null | awk '$0=="quorum" {getline; print}' || true)"
        [[ "$actual" == "$desired" ]] || converged=false
      done
      sts="$("${k[@]}" get sts upgrade-s -o json)"
      jq -e --arg value "$desired" '
        .status.observedGeneration == .metadata.generation and
        .status.readyReplicas == 3 and .status.updatedReplicas == 3 and
        .status.currentRevision == .status.updateRevision and
        any(.spec.template.spec.containers[].env[]; .name == "QUORUM" and .value == $value)
      ' <<<"$sts" >/dev/null || converged=false
      [[ "$converged" == true ]] && break
      sleep 2
    done
    [[ "$converged" == true ]]
    [[ "$(durable_identity)" == "$durable_before" ]]
    echo "PASS: durable identities retained at quorum $desired"
    # Ready probes can precede peer rediscovery after a Sentinel rollout.
    deadline=$((SECONDS+60)); healthy=false
    while (( SECONDS < deadline )); do
      if health; then healthy=true; break; fi
      sleep 2
    done
    [[ "$healthy" == true ]]
    echo "PASS: configured quorum $desired reached every Sentinel and completed rollout with data and durable identities retained"
  done
fi
