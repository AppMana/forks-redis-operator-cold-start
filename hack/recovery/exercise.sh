#!/usr/bin/env bash
# Mutates only the explicitly named, dedicated local canary; never deletes PVCs.
set -euo pipefail
config="${1:?usage: exercise.sh LOCAL_KUBECONFIG}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]] || { echo "Refusing non-canary context" >&2; exit 2; }
server="$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://127.0.0.1:* ]] || { echo "Refusing non-local server" >&2; exit 2; }
dir="$(cd "$(dirname "$0")" && pwd)"
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary)
cli() { "${k[@]}" exec "$1" -- redis-cli --raw "${@:2}"; }
converge() {
  for attempt in {1..90}; do
    if bash "$dir/check.sh" "$config" 2>/dev/null; then return 0; fi
    sleep 2
  done
  echo "FAIL: convergence deadline" >&2
  return 1
}
primary() { "${k[@]}" get pods -l app=upgrade,redis-role=master -o json | jq -r '.items[0].metadata.name // empty'; }
"${k[@]}" wait pods -l app=upgrade --for=condition=Ready --timeout=180s
"${k[@]}" wait pods -l app=upgrade-sentinel --for=condition=Ready --timeout=180s
for attempt in {1..90}; do
  [[ -n "$(primary)" ]] && break
  sleep 2
done
[[ -n "$(primary)" ]]
cli "$(primary)" SET appmana:upgrade:sentinel-regression preserved-20261020
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
"${k[@]}" delete pods -l 'app in (upgrade,upgrade-sentinel)' --wait=false
converge
echo "PASS: recovery exercise"
