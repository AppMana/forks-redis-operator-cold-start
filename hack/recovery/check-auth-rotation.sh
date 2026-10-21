#!/usr/bin/env bash
# Read-only acceptance using current Secrets, never a pod's stale environment.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
namespace=redis-upgrade-canary-auth-rotation
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$namespace")
cli() {
  local secret="$1" pod="$2" container="$3"; shift 3
  "${k[@]}" get secret "$secret" -o json | jq -r '.data.password|@base64d' |
    "${k[@]}" exec -i "$pod" -c "$container" -- sh -ec 'read -r REDISCLI_AUTH; export REDISCLI_AUTH; exec redis-cli --raw "$@"' sh "$@" 2>/dev/null
}
ips="$("${k[@]}" get endpointslices -l kubernetes.io/service-name=upgrade-master -o json | jq -r '[.items[].endpoints[]?|select(.conditions.ready!=false)|.addresses[]]|unique|.[]')"
[[ "$(printf '%s\n' "$ips" | sed '/^$/d' | wc -l)" -eq 1 ]]
primary=""
for ordinal in 0 1 2; do
  pod="upgrade-$ordinal"
  info="$(cli redis-auth "$pod" upgrade INFO replication | tr -d '\r')"
  if [[ "$info" == *$'role:master\n'* ]]; then
    [[ -z "$primary" && "$info" == *$'connected_slaves:2\n'* ]]
    primary="$pod"
    [[ "$("${k[@]}" get pod "$pod" -o jsonpath='{.status.podIP}')" == "$ips" ]]
  else
    [[ "$info" == *$'role:slave\n'* && "$info" == *$'master_link_status:up\n'* ]]
  fi
  [[ "$(cli redis-auth "$pod" upgrade GET appmana:rotation:retained)" == preserved-auth-rotation-20261020 ]]
done
[[ -n "$primary" ]]
[[ -z "${PREVIOUS_PRIMARY:-}" || "$primary" != "$PREVIOUS_PRIMARY" ]]
for ordinal in 0 1 2; do
  pod="upgrade-s-$ordinal"
  [[ "$(cli sentinel-auth "$pod" sentinel -p 26379 SENTINEL CKQUORUM mymaster)" == OK* ]]
  master="$(cli sentinel-auth "$pod" sentinel -p 26379 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster | head -1)"
  [[ "$master" == "$ips" || "$master" == "$primary.upgrade-headless.$namespace.svc.cluster.local" ]]
done
echo "PASS: current credentials, primary $primary, single endpoint, linked replicas, Sentinel agreement and retained data"
