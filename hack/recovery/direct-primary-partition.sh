#!/usr/bin/env bash
# Characterize direct writes during a full old-primary partition, local fixture only.
# Rewrites only an existing synthetic key to its existing value; retains all PVCs.
set -euo pipefail
config="${1:?local kubeconfig required}"
expected="${2:?existing fixture value required}"
expected_reply="${3:-OK}"
[[ "$expected_reply" == OK || "$expected_reply" == 'NOREPLICAS Not enough good replicas to write.' ]]
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-20261020 ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
export CANARY_NAMESPACE=redis-upgrade-canary-migration
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$CANARY_NAMESPACE")
[[ "$(kubectl --kubeconfig "$config" --context "$context" -n redis-operator get deployment redis-operator -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="WATCH_NAMESPACE")].value}')" == "$CANARY_NAMESPACE" ]]
dir="$(cd "$(dirname "$0")" && pwd)"
bash "$dir/check.sh" "$config" "$expected"
primary="$("${k[@]}" get redisreplication upgrade -o jsonpath='{.status.masterNode}')"
[[ "$primary" == upgrade-[012] ]]
primary_uid="$("${k[@]}" get pod "$primary" -o jsonpath='{.metadata.uid}')"
node="$("${k[@]}" get pod "$primary" -o jsonpath='{.spec.nodeName}')"
[[ "$node" == redis-recovery-20261020-control-plane ]]
[[ "$(docker inspect --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}' "$node")" == redis-recovery-20261020 ]]
container="$("${k[@]}" get pod "$primary" -o jsonpath='{.status.containerStatuses[?(@.name=="upgrade")].containerID}')"
[[ "$container" =~ ^containerd://[a-f0-9]{64}$ ]]
pid="$(docker exec "$node" crictl inspect "${container#containerd://}" | jq -er '.info.pid')"
[[ "$pid" =~ ^[0-9]+$ && "$pid" -gt 1 ]]
chain="AMDIRECT$$"
net() { docker exec "$node" nsenter -t "$pid" -n -- iptables -w 3 "$@"; }
cleanup() {
  net -D INPUT -j "$chain" 2>/dev/null || true
  net -F "$chain" 2>/dev/null || true
  net -X "$chain" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 130' INT TERM
net -N "$chain"
# Local exec probes survive; operator, replicas and Sentinels cannot reach Redis.
net -A "$chain" ! -i lo -p tcp --dport 6379 -j DROP
docker exec -d "$node" sh -c 'sleep 120; nsenter -t "$1" -n -- iptables -w 3 -D INPUT -j "$2"; nsenter -t "$1" -n -- iptables -w 3 -F "$2"; nsenter -t "$1" -n -- iptables -w 3 -X "$2"' sh "$pid" "$chain"
net -I INPUT 1 -j "$chain"
deadline=$((SECONDS+75)); elected=""
while (( SECONDS < deadline )); do
  elected="$("${k[@]}" get redisreplication upgrade -o jsonpath='{.status.masterNode}')"
  if [[ -n "$elected" && "$elected" != "$primary" ]]; then break; fi
  sleep 2
done
[[ "$elected" == upgrade-[012] && "$elected" != "$primary" ]]
"${k[@]}" exec "$elected" -c upgrade -- redis-cli --raw INFO replication | tr -d '\r' | rg -q '^role:master$'
"${k[@]}" exec "$primary" -c upgrade -- redis-cli --raw INFO replication | tr -d '\r' | rg -q '^role:master$'
# Wait beyond the fixture's default 10-second replica acknowledgement lag.
sleep 12
[[ "$("${k[@]}" get pod "$primary" -o jsonpath='{.metadata.uid}')" == "$primary_uid" ]]
[[ "$("${k[@]}" get pod "$primary" -o jsonpath='{.status.containerStatuses[?(@.name=="upgrade")].containerID}')" == "$container" ]]
reply="$("${k[@]}" exec "$primary" -c upgrade -- redis-cli --raw EVAL 'if redis.call("GET",KEYS[1]) ~= ARGV[1] then return redis.error_reply("fixture value mismatch") end; return redis.call("SET",KEYS[1],ARGV[1],"KEEPTTL")' 1 appmana:upgrade:sentinel-regression "$expected")"
[[ "$reply" == "$expected_reply" ]] || { echo "FAIL: unexpected direct-write result: $reply" >&2; exit 1; }
dropped="$(net -L "$chain" -n -v -x | awk '$3=="DROP" {sum+=$1} END {print sum+0}')"
[[ "$dropped" -gt 0 ]]
echo "OBSERVED: old primary $primary direct-write reply=$reply; elected primary=$elected; dropped=$dropped"
cleanup
[[ "$(net -S)" != *"$chain"* ]]
trap - EXIT
deadline=$((SECONDS+120))
while (( SECONDS < deadline )); do
  if timeout 20s bash "$dir/check.sh" "$config" "$expected" 2>/dev/null; then
    echo 'PASS: expected direct-write behavior characterized; partition removed and both database values retained'
    exit 0
  fi
  sleep 2
done
echo 'FAIL: topology did not converge after healing' >&2
exit 1
