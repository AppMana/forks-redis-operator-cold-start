#!/usr/bin/env bash
# Isolate Sentinel-to-primary traffic only inside a dedicated kind pod netns.
# Redis/replica/operator connectivity remains intact: tests election authority.
set -euo pipefail
config="${1:?usage: sentinel-partition.sh LOCAL_KUBECONFIG EXPECTED_VALUE}"
expected="${2:?existing value required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-20261020 ]] || exit 2
server="$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://127.0.0.1:* ]] || exit 2
export CANARY_NAMESPACE="${CANARY_NAMESPACE:?explicit canary namespace required}"
[[ "$CANARY_NAMESPACE" == redis-upgrade-canary* ]] || exit 2
node=redis-recovery-20261020-control-plane
dir="$(cd "$(dirname "$0")" && pwd)"
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$CANARY_NAMESPACE")
bash "$dir/check.sh" "$config" "$expected"
primary="$("${k[@]}" get pods -l app=upgrade,redis-role=master -o json | jq -er 'if (.items|length)==1 then .items[0].metadata.name else error("ambiguous primary") end')"
[[ "$("${k[@]}" get pod "$primary" -o jsonpath='{.spec.nodeName}')" == "$node" ]]
container="$("${k[@]}" get pod "$primary" -o jsonpath='{.status.containerStatuses[?(@.name=="upgrade")].containerID}')"
[[ "$container" == containerd://* ]] || exit 2
container="${container#containerd://}"
pid="$(docker exec "$node" crictl inspect "$container" | jq -er '.info.pid')"
[[ "$pid" =~ ^[0-9]+$ && "$pid" -gt 1 ]] || exit 2
mapfile -t peers < <("${k[@]}" get pods -l app=upgrade-sentinel -o json | jq -r '.items[].status.podIP')
[[ "${#peers[@]}" == 3 ]]
for peer in "${peers[@]}"; do [[ "$peer" =~ ^10\.244\.[0-9]+\.[0-9]+$ ]] || exit 2; done
# Unique owned chain. Never flush or alter existing chains/rules.
chain="AMTEST$$"
net() { docker exec "$node" nsenter -t "$pid" -n -- iptables -w 3 "$@"; }
cleanup() {
  net -D INPUT -j "$chain" 2>/dev/null || true
  net -F "$chain" 2>/dev/null || true
  net -X "$chain" 2>/dev/null || true
}
trap cleanup EXIT
net -N "$chain"
for peer in "${peers[@]}"; do net -A "$chain" -s "$peer" -p tcp --dport 6379 -j DROP; done
# Independent bounded cleanup also survives termination of this driver.
docker exec -d "$node" sh -c 'sleep 120; nsenter -t "$1" -n -- iptables -w 3 -D INPUT -j "$2"; nsenter -t "$1" -n -- iptables -w 3 -F "$2"; nsenter -t "$1" -n -- iptables -w 3 -X "$2"' sh "$pid" "$chain"
net -I INPUT 1 -j "$chain"
echo "Partitioned Sentinel traffic to $primary; operator and replicas remain connected"
changed=false
for attempt in {1..45}; do
  elected="$("${k[@]}" get pods -l app=upgrade,redis-role=master -o json | jq -r 'if (.items|length)==1 then .items[0].metadata.name else "" end')"
  if [[ -n "$elected" && "$elected" != "$primary" ]]; then changed=true; break; fi
  sleep 2
done
[[ "$changed" == true ]] || { echo 'FAIL: partition did not yield a new published primary' >&2; exit 1; }
demoted=false
for attempt in {1..20}; do
  if "${k[@]}" exec "$primary" -c upgrade -- redis-cli --raw INFO replication | tr -d '\r' | rg -q '^role:slave$'; then demoted=true; break; fi
  sleep 1
done
[[ "$demoted" == true ]] || { echo 'FAIL: old primary was not demoted' >&2; exit 1; }
dropped="$(net -L "$chain" -n -v -x | awk '$3=="DROP" {sum+=$1} END {print sum+0}')"
[[ "$dropped" -gt 0 ]] || { echo 'FAIL: no Sentinel packets were dropped' >&2; exit 1; }
elected_ip="$("${k[@]}" get pod "$elected" -o jsonpath='{.status.podIP}')"
for ordinal in 0 1 2; do
  monitored="$("${k[@]}" exec "upgrade-sentinel-$ordinal" -c upgrade-sentinel -- redis-cli --raw -p 26379 SENTINEL GET-MASTER-ADDR-BY-NAME upgrade | head -1)"
  [[ "$monitored" == "$elected_ip" || "$monitored" == "$elected.upgrade-headless.$CANARY_NAMESPACE.svc.cluster.local" ]]
done
echo "PASS: $dropped Sentinel packets dropped; all three Sentinels agree with the published primary before healing"
cleanup
rules="$(net -S)"
[[ "$rules" != *"$chain"* ]] || { echo 'FAIL: owned partition rules remain' >&2; exit 1; }
trap - EXIT
for attempt in {1..60}; do
  if timeout 20s bash "$dir/check.sh" "$config" "$expected" 2>/dev/null; then
    echo 'PASS: asymmetric Sentinel partition recovered without reversing election or losing test data'
    exit 0
  fi
  sleep 2
done
echo 'FAIL: partition healed but topology did not converge' >&2
exit 1
