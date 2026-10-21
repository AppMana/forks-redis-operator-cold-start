#!/usr/bin/env bash
# Mutates only the explicitly named, dedicated local canary; never deletes PVCs.
set -euo pipefail
config="${1:?usage: exercise.sh LOCAL_KUBECONFIG}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]] || { echo "Refusing non-canary context" >&2; exit 2; }
server="$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://127.0.0.1:* ]] || { echo "Refusing non-local server" >&2; exit 2; }
dir="$(cd "$(dirname "$0")" && pwd)"
export CANARY_NAMESPACE="${CANARY_NAMESPACE:-redis-upgrade-canary}"
[[ "$CANARY_NAMESPACE" == redis-upgrade-canary* ]] || exit 2
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$CANARY_NAMESPACE")
cli() { "${k[@]}" exec "$1" -- redis-cli --raw "${@:2}"; }
converge() {
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if timeout 20s bash "$dir/check.sh" "$config" 2>/dev/null; then return 0; fi
    sleep 2
  done
  echo "FAIL: convergence deadline" >&2
  return 1
}
primary() { "${k[@]}" get pods -l app=upgrade,redis-role=master -o json | jq -r '.items[0].metadata.name // empty'; }
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  count="$("${k[@]}" get pods -l 'app in (upgrade,upgrade-sentinel)' -o json | jq '.items|length')"
  [[ "$count" -eq 6 ]] && break
  sleep 2
done
[[ "$count" -eq 6 ]]
"${k[@]}" wait pods -l app=upgrade --for=condition=Ready --timeout=180s
"${k[@]}" wait pods -l app=upgrade-sentinel --for=condition=Ready --timeout=180s
for attempt in {1..90}; do
  [[ -n "$(primary)" ]] && break
  sleep 2
done
[[ -n "$(primary)" ]]
cli "$(primary)" SET appmana:upgrade:sentinel-regression preserved-20261020
cli "$(primary)" -n 1 SET appmana:upgrade:secondary-database preserved-20261020
converge
echo "Scenario: Sentinel failover"
before="$(primary)"
cli upgrade-sentinel-0 -p 26379 SENTINEL FAILOVER upgrade
for attempt in {1..60}; do
  [[ "$(primary)" != "$before" ]] && break
  sleep 1
done
[[ "$(primary)" != "$before" ]]
converge
echo "Scenario: replica IP replacement and stale discovery pruning"
victim=upgrade-0
[[ "$(primary)" != "$victim" ]] || victim=upgrade-1
old_uid="$("${k[@]}" get pod "$victim" -o jsonpath='{.metadata.uid}')"
"${k[@]}" delete pod "$victim" --wait=false
for attempt in {1..90}; do
  uid="$("${k[@]}" get pod "$victim" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  [[ -n "$uid" && "$uid" != "$old_uid" ]] && break
  sleep 2
done
[[ -n "$uid" && "$uid" != "$old_uid" ]]
converge
echo "Scenario: rolling restart overlapping a Sentinel failover"
"${k[@]}" rollout restart statefulset/upgrade
cli upgrade-sentinel-0 -p 26379 SENTINEL FAILOVER upgrade
"${k[@]}" rollout status statefulset/upgrade --timeout=180s
converge
echo "Scenario: simultaneous data and Sentinel restart, PVCs retained"
pods=(upgrade-0 upgrade-1 upgrade-2 upgrade-sentinel-0 upgrade-sentinel-1 upgrade-sentinel-2)
uids=()
sentinel_ids=()
for pod in "${pods[@]}"; do
  uids+=("$("${k[@]}" get pod "$pod" -o jsonpath='{.metadata.uid}')")
done
for ordinal in 0 1 2; do sentinel_ids+=("$(cli "upgrade-sentinel-$ordinal" -p 26379 SENTINEL MYID)"); done
persistent="$("${k[@]}" get redissentinel upgrade -o json | jq -r '.spec.configVolumeClaimTemplate != null')"
claims="$("${k[@]}" get pvc -o json | jq -c '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
"${k[@]}" delete pods -l 'app in (upgrade,upgrade-sentinel)' --wait=false
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  replaced=true
  for i in "${!pods[@]}"; do
    uid="$("${k[@]}" get pod "${pods[$i]}" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    [[ -n "$uid" && "$uid" != "${uids[$i]}" ]] || replaced=false
  done
  [[ "$replaced" == true ]] && break
  sleep 2
done
[[ "$replaced" == true ]]
converge
[[ "$claims" == "$("${k[@]}" get pvc -o json | jq -c '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
if [[ "$persistent" == true ]]; then
  for ordinal in 0 1 2; do
    [[ "${sentinel_ids[$ordinal]}" == "$(cli "upgrade-sentinel-$ordinal" -p 26379 SENTINEL MYID)" ]]
  done
  echo "PASS: all Sentinel identities and PVC UIDs retained across full restart"
fi
echo "PASS: recovery exercise"
