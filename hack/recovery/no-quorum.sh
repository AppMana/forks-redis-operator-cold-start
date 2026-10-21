#!/usr/bin/env bash
# Dedicated local canary only: no writes while a second primary is injected.
set -euo pipefail
config="${1:?usage: no-quorum.sh LOCAL_KUBECONFIG EXPECTED_VALUE}"
expected="${2:?existing value required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-20261020 ]] || exit 2
server="$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://127.0.0.1:* ]] || exit 2
export CANARY_NAMESPACE=redis-upgrade-canary-migration
node=redis-recovery-20261020-control-plane
base=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s)
k=("${base[@]}" -n "$CANARY_NAMESPACE")
dir="$(cd "$(dirname "$0")" && pwd)"
"${k[@]}" rollout status statefulset/upgrade --timeout=120s
bash "$dir/check.sh" "$config" "$expected"
data_uids="$("${k[@]}" get pods -l app=upgrade -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
primary="$("${k[@]}" get redisreplication upgrade -o jsonpath='{.status.masterNode}')"
[[ "$primary" == upgrade-[012] ]]
victim=upgrade-0
[[ "$primary" != "$victim" ]] || victim=upgrade-1
tasks=()
for n in 0 1 2; do
  [[ "$("${k[@]}" get pod upgrade-sentinel-$n -o jsonpath='{.spec.nodeName}')" == "$node" ]]
  task="$("${k[@]}" get pod upgrade-sentinel-$n -o jsonpath='{.status.containerStatuses[?(@.name=="upgrade-sentinel")].containerID}')"
  [[ "$task" =~ ^containerd://[a-f0-9]{64}$ ]] || exit 2
  tasks+=("${task#containerd://}")
done
resume() {
  for task in "${tasks[@]}"; do
    docker exec "$node" ctr -n k8s.io tasks resume "$task" >/dev/null 2>&1 || true
  done
}
trap resume EXIT
trap 'exit 130' INT TERM
# Backup cleanup survives driver termination; only the verified task IDs.
docker exec -d "$node" sh -c 'sleep 30; for task do ctr -n k8s.io tasks resume "$task" >/dev/null 2>&1 || true; done' sh "${tasks[@]}"
for task in "${tasks[@]}"; do docker exec "$node" ctr -n k8s.io tasks pause "$task"; done
"${k[@]}" annotate pod "$primary" appmana-test/no-quorum="$(date +%s)" --overwrite
observed=false
for attempt in {1..8}; do
  if "${base[@]}" -n redis-operator logs deployment/redis-operator --since=20s 2>/dev/null | rg -q 'Sentinel authority unresolved; refusing fallback election'; then observed=true; break; fi
  sleep 1
done
[[ "$observed" == true ]]
"${k[@]}" exec "$victim" -c upgrade -- redis-cli REPLICAOF NO ONE
"${k[@]}" annotate pod "$primary" appmana-test/no-quorum-split="$(date +%s)" --overwrite
sleep 8
[[ "$data_uids" == "$("${k[@]}" get pods -l app=upgrade -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
for task in "${tasks[@]}"; do
  docker exec "$node" ctr -n k8s.io tasks list | awk -v task="$task" '$1==task && $3=="PAUSED" {found=1} END {exit !found}'
done
"${k[@]}" exec "$victim" -c upgrade -- redis-cli --raw INFO replication | tr -d '\r' | rg -q '^role:master$'
[[ "$("${k[@]}" get redisreplication upgrade -o jsonpath='{.status.masterNode}')" == "$primary" ]]
[[ "$("${k[@]}" get pod "$victim" -o jsonpath='{.metadata.labels.redis-role}')" != master ]]
echo 'PASS: no Sentinel answers; operator withheld fallback repair and did not publish the injected primary'
resume
for attempt in {1..60}; do
  if timeout 20s bash "$dir/check.sh" "$config" "$expected" 2>/dev/null; then
    echo 'PASS: Sentinel authority restored; replicas, quorum and both database values converged'
    exit 0
  fi
  sleep 2
done
echo 'FAIL: recovery after restoring Sentinel tasks' >&2
exit 1
