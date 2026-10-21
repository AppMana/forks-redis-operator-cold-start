#!/usr/bin/env bash
# Abruptly terminate only the Redis task in the dedicated disposable fixture.
# Preserve pod/PVC objects and all retained data; never target a host PID.
set -euo pipefail
config="${1:?qualified lab kubeconfig required}"
context=kind-redis-recovery-qualified-20261021
namespace=redis-upgrade-canary-empty-disk
node=redis-recovery-qualified-20261021-control-plane
[[ "$(kubectl --kubeconfig "$config" config current-context)" == "$context" ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
[[ "$(docker inspect --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}' "$node")" == redis-recovery-qualified-20261021 ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$namespace")
dir="$(cd "$(dirname "$0")" && pwd)"
export CANARY_NAMESPACE="$namespace"
bash "$dir/check.sh" "$config" preserved-empty-disk-20261021
primary="$("${k[@]}" get redisreplication upgrade -o jsonpath='{.status.masterNode}')"
[[ "$primary" == upgrade-[012] ]]
[[ "$("${k[@]}" get pod "$primary" -o jsonpath='{.spec.nodeName}')" == "$node" ]]
pod_uid="$("${k[@]}" get pod "$primary" -o jsonpath='{.metadata.uid}')"
claims="$("${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
task="$("${k[@]}" get pod "$primary" -o jsonpath='{.status.containerStatuses[?(@.name=="upgrade")].containerID}')"
[[ "$task" =~ ^containerd://[a-f0-9]{64}$ ]]
docker exec "$node" ctr -n k8s.io tasks kill --signal SIGKILL "${task#containerd://}"
echo "Terminated Redis task for $primary; waiting for container restart and strict health"
deadline=$((SECONDS+150))
while ((SECONDS<deadline)); do
  current="$("${k[@]}" get pod "$primary" -o jsonpath='{.status.containerStatuses[?(@.name=="upgrade")].containerID}')"
  if [[ "$current" =~ ^containerd://[a-f0-9]{64}$ && "$current" != "$task" ]] && timeout 20s bash "$dir/check.sh" "$config" preserved-empty-disk-20261021 2>/dev/null; then
    [[ "$("${k[@]}" get pod "$primary" -o jsonpath='{.metadata.uid}')" == "$pod_uid" ]]
    [[ "$("${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" == "$claims" ]]
    echo 'PASS: abrupt primary process death recovered with pod/PVC identities and baseline keys retained; concurrent-write durability requires separate client results'
    exit 0
  fi
  sleep 2
done
echo 'FAIL: abrupt primary process death did not recover before deadline' >&2
exit 1
