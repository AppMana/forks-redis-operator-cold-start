#!/usr/bin/env bash
# Isolated fixture: observe a replacement replica before operator reconciliation.
set -euo pipefail
config="${1:?local kubeconfig required}"
expected_value="${2:?existing data value required}"
expected_min="${3:?expected bootstrap min-replicas-to-write required}"
[[ "$expected_min" == 0 || "$expected_min" == 1 ]]
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-20261020 ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s)
export CANARY_NAMESPACE=redis-upgrade-canary-migration
dir="$(cd "$(dirname "$0")" && pwd)"
health() { bash "$dir/check.sh" "$config" "$expected_value"; }
cli() { "${k[@]}" -n "$CANARY_NAMESPACE" exec "$victim" -c upgrade -- redis-cli --raw "$@"; }
[[ "$("${k[@]}" -n redis-operator get deployment redis-operator -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="WATCH_NAMESPACE")].value}')" == "$CANARY_NAMESPACE" ]]
[[ "$("${k[@]}" -n redis-operator get deployment redis-operator -o jsonpath='{.spec.replicas}')" == 1 ]]
health
primary="$("${k[@]}" -n "$CANARY_NAMESPACE" get redisreplication upgrade -o jsonpath='{.status.masterNode}')"
[[ "$primary" == upgrade-[012] ]]
victim=upgrade-2
[[ "$primary" != "$victim" ]] || victim=upgrade-1
cli INFO replication | tr -d '\r' | rg -q '^role:slave$'
[[ "$(cli CONFIG GET min-replicas-to-write | tail -1)" == 1 ]]
claims="$("${k[@]}" -n "$CANARY_NAMESPACE" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
old_uid="$("${k[@]}" -n "$CANARY_NAMESPACE" get pod "$victim" -o jsonpath='{.metadata.uid}')"
restore() { "${k[@]}" -n redis-operator scale deployment redis-operator --replicas=1 >/dev/null; }
trap restore EXIT
trap 'exit 130' INT TERM
"${k[@]}" -n redis-operator scale deployment redis-operator --replicas=0 >/dev/null
"${k[@]}" -n redis-operator wait pod --all --for=delete --timeout=60s
"${k[@]}" -n "$CANARY_NAMESPACE" delete pod "$victim" --wait=true --timeout=60s
deadline=$((SECONDS+90)); observed=false
while (( SECONDS < deadline )); do
  uid="$("${k[@]}" -n "$CANARY_NAMESPACE" get pod "$victim" --ignore-not-found -o jsonpath='{.metadata.uid}')"
  if [[ -n "$uid" && "$uid" != "$old_uid" ]] && [[ "$(cli PING 2>/dev/null || true)" == PONG ]]; then
    observed=true; break
  fi
  sleep 2
done
[[ "$observed" == true ]]
actual="$(cli CONFIG GET min-replicas-to-write | tail -1)"
echo "OBSERVED: replacement replica min-replicas-to-write=$actual with operator stopped"
[[ "$actual" == "$expected_min" ]]
restore
trap - EXIT
"${k[@]}" -n redis-operator rollout status deployment/redis-operator --timeout=90s
deadline=$((SECONDS+120)); recovered=false
while (( SECONDS < deadline )); do
  if [[ "$(cli CONFIG GET min-replicas-to-write 2>/dev/null | tail -1)" == 1 ]] && health >/dev/null 2>&1; then
    recovered=true; break
  fi
  sleep 2
done
[[ "$recovered" == true ]]
[[ "$claims" == "$("${k[@]}" -n "$CANARY_NAMESPACE" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
echo 'PASS: bootstrap behavior characterized; operator restored policy, topology and data; all PVC identities retained'
