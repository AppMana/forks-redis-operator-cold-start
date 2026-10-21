#!/usr/bin/env bash
# Read-only checks for the isolated ephemeral cluster upgrade fixture.
set -euo pipefail
config="${1:?explicit local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --request-timeout=10s -n redis-upgrade-canary-cluster-guard)
baseline="$("${k[@]}" get cm cluster-baseline -o json | jq -r '.data["pod-identities"]')"
observed="$("${k[@]}" get pods -o json | jq -c '[.items[]|select(.metadata.name|startswith("guarded-"))|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
[[ "$baseline" == "$observed" && "$(jq length <<<"$observed")" == 3 ]]
for ordinal in 0 1 2; do
  pod="guarded-leader-$ordinal"
  info="$("${k[@]}" exec "$pod" -- redis-cli --raw CLUSTER INFO | tr -d '\r')"
  [[ "$info" == *$'cluster_state:ok\n'* && "$info" == *$'cluster_slots_ok:16384\n'* && "$info" == *$'cluster_known_nodes:3\n'* ]]
  for key in 0 1 2; do
    [[ "$("${k[@]}" exec "$pod" -- redis-cli --raw -c GET "appmana:upgrade:cluster:$key")" == preserved-cluster-guard-20261021 ]]
  done
done
echo 'PASS: three original cluster pods, 16384 healthy slots, three synthetic keys readable from every node'
