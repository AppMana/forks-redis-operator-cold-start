#!/usr/bin/env bash
# Pause only a verified Redis task in the dedicated local kind node.
set -euo pipefail
config="${1:?usage: hung-primary.sh LOCAL_KUBECONFIG}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]] || exit 2
server="$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://127.0.0.1:* ]] || exit 2
export CANARY_NAMESPACE="${CANARY_NAMESPACE:-redis-upgrade-canary-persistent}"
[[ "$CANARY_NAMESPACE" == redis-upgrade-canary* ]] || exit 2
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$CANARY_NAMESPACE")
dir="$(cd "$(dirname "$0")" && pwd)"
primary="$("${k[@]}" get pods -l app=upgrade,redis-role=master -o json | jq -er 'if (.items|length)==1 then .items[0].metadata.name else error("not one primary") end')"
expected="$("${k[@]}" exec "$primary" -- redis-cli --raw GET appmana:upgrade:sentinel-regression)"
[[ "$expected" == preserved-* ]]
bash "$dir/check.sh" "$config" "$expected"
pod="$("${k[@]}" get pod "$primary" -o json)"
node="$(jq -r '.spec.nodeName' <<<"$pod")"
[[ "$node" == "${context#kind-}-control-plane" ]] || exit 2
task="$(jq -er '.status.containerStatuses[]|select(.name=="upgrade")|.containerID' <<<"$pod")"
[[ "$task" =~ ^containerd://[a-f0-9]{64}$ ]] || exit 2
task="${task#containerd://}"
paused=false
resume() {
  if [[ "$paused" == true ]]; then
    if docker exec "$node" ctr -n k8s.io tasks resume "$task"; then
      paused=false
    elif ! docker exec "$node" ctr -n k8s.io tasks list -q | grep -Fxq "$task"; then
      # Kubelet may already have replaced a liveness-failed task.
      paused=false
    else
      echo "FAIL: could not resume local task $task" >&2
      return 1
    fi
  fi
}
trap resume EXIT
trap 'exit 130' INT TERM
paused=true
docker exec "$node" ctr -n k8s.io tasks pause "$task"
echo "Paused local primary $primary; waiting for a different writable endpoint"
deadline=$((SECONDS + 120))
replacement=""
while (( SECONDS < deadline )); do
  replacement="$("${k[@]}" get pods -l app=upgrade,redis-role=master -o json | jq -r 'if (.items|length)==1 then .items[0].metadata.name else empty end')"
  if [[ -n "$replacement" && "$replacement" != "$primary" ]]; then
    value="$("${k[@]}" exec "$replacement" -- redis-cli --raw GET appmana:upgrade:sentinel-regression)"
    [[ "$value" == "$expected" ]] || exit 1
    break
  fi
  sleep 1
done
[[ -n "$replacement" && "$replacement" != "$primary" ]]
echo "PASS: hung primary replaced by $replacement without losing the test value"
resume
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  if timeout 20s bash "$dir/check.sh" "$config" "$expected" 2>/dev/null; then
    echo "PASS: resumed primary safely rejoined"
    exit 0
  fi
  sleep 2
done
echo "FAIL: resumed primary did not converge" >&2
exit 1
