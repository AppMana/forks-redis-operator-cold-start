#!/usr/bin/env bash
# Read-only convergence checks against an explicitly selected local canary.
set -euo pipefail
config="${1:?usage: check.sh LOCAL_KUBECONFIG [EXPECTED_VALUE]}"
expected="${2:-preserved-20261020}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]] || { echo "Refusing non-canary context" >&2; exit 2; }
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary)
cli() { "${k[@]}" exec "$1" -- redis-cli --raw "${@:2}"; }
ips="$("${k[@]}" get endpointslices -l kubernetes.io/service-name=upgrade-master -o json |
  jq -r '[.items[].endpoints[]? | select(.conditions.ready != false) | .addresses[]] | unique | .[]')"
[[ "$(printf '%s\n' "$ips" | sed '/^$/d' | wc -l)" -eq 1 ]]
primary=""
for ordinal in 0 1 2; do
  pod="upgrade-$ordinal"
  info="$(cli "$pod" INFO replication | tr -d '\r')"
  if [[ "$info" == *$'role:master\n'* ]]; then
    [[ -z "$primary" ]]
    primary="$pod"
    [[ "$info" == *$'connected_slaves:2\n'* ]]
    [[ "$("${k[@]}" get pod "$pod" -o jsonpath='{.status.podIP}')" == "$ips" ]]
  else
    [[ "$info" == *$'role:slave\n'* && "$info" == *$'master_link_status:up\n'* ]]
  fi
  [[ "$(cli "$pod" GET appmana:upgrade:sentinel-regression)" == "$expected" ]]
done
[[ -n "$primary" ]]
for ordinal in 0 1 2; do
  pod="upgrade-sentinel-$ordinal"
  [[ "$(cli "$pod" -p 26379 SENTINEL CKQUORUM upgrade)" == OK* ]]
  master="$(cli "$pod" -p 26379 SENTINEL GET-MASTER-ADDR-BY-NAME upgrade | head -1)"
  [[ "$master" == "$ips" ]]
  metadata="$(cli "$pod" -p 26379 SENTINEL MASTER upgrade | tr -d '\r')"
  [[ "$(printf '%s\n' "$metadata" | awk '$0=="num-slaves" {getline; print}')" == 2 ]]
  [[ "$(printf '%s\n' "$metadata" | awk '$0=="num-other-sentinels" {getline; print}')" == 2 ]]
done
printf 'PASS: %s, one writable endpoint, two linked replicas, three agreeing Sentinels, key preserved\n' "$primary"
