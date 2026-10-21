#!/bin/sh
# Test namespaces on kind only. Bound the fault independently of the test runner.
set -eu
: "${NAMESPACE:?test namespace required}"
case "$NAMESPACE" in chainsaw-*|redis-upgrade-canary-*) ;; *) exit 2;; esac
context=$(kubectl config current-context)
case "$context" in kind-*) ;; *) exit 2;; esac
name="${REPLICATION_NAME:-redis-replication}"
primary=$(kubectl -n "$NAMESPACE" get redisreplication "$name" -o jsonpath='{.status.masterNode}')
case "$primary" in "$name"-[0-9]*) ;; *) exit 2;; esac
node=$(kubectl -n "$NAMESPACE" get pod "$primary" -o jsonpath='{.spec.nodeName}')
case "$node" in "${context#kind-}-control-plane"|"${context#kind-}-worker"*) ;; *) exit 2;; esac
task=$(kubectl -n "$NAMESPACE" get pod "$primary" -o "jsonpath={.status.containerStatuses[?(@.name=='$name')].containerID}")
printf '%s\n' "$task" | grep -Eq '^containerd://[a-f0-9]{64}$'
task=${task#containerd://}
role() {
  kubectl --request-timeout=5s -n "$NAMESPACE" exec "$1" -c "$name" -- sh -ec '
    REDISCLI_AUTH="$REDIS_PASSWORD"; export REDISCLI_AUTH
    redis-cli --raw INFO replication' 2>/dev/null | tr -d '\r' | awk -F: '$1=="role" {print $2}'
}
[ "$(role "$primary")" = master ]
paused=false
resume() {
  if [ "$paused" = true ]; then
    if docker exec "$node" ctr -n k8s.io tasks list -q | grep -Fxq "$task"; then
      docker exec "$node" ctr -n k8s.io tasks resume "$task"
    fi
  fi
}
trap resume EXIT
trap 'exit 130' INT TERM
paused=true
# Watchdog lives in the kind node, outside both the paused task and runner.
docker exec -d "$node" sh -c 'sleep 120; ctr -n k8s.io tasks resume "$1" >/dev/null 2>&1 || true' sh "$task"
docker exec "$node" ctr -n k8s.io tasks pause "$task"
stopped=$(docker exec "$node" ctr -n k8s.io tasks list | awk -v task="$task" '$1==task {print $3}')
[ "$stopped" = PAUSED ]
echo "Verified paused primary $primary; waiting for a different serving primary"
deadline=$(($(date +%s)+90))
while [ "$(date +%s)" -lt "$deadline" ]; do
  replacement=$(kubectl --request-timeout=5s -n "$NAMESPACE" get redisreplication "$name" -o jsonpath='{.status.masterNode}')
  case "$replacement" in
    "$name"-[0-9]*)
      if [ "$replacement" != "$primary" ] && [ "$(role "$replacement")" = master ]; then
        endpoint=$(kubectl --request-timeout=5s -n "$NAMESPACE" get endpoints "$name-master" -o jsonpath='{.subsets[*].addresses[*].targetRef.name}')
        if [ "$endpoint" = "$replacement" ]; then
          stopped=$(docker exec "$node" ctr -n k8s.io tasks list | awk -v task="$task" '$1==task {print $3}')
          [ "$stopped" = PAUSED ] || { echo 'FAIL: original task did not stay paused through failover' >&2; exit 1; }
          resume
          paused=false
          echo "PASS:paused-primary $primary replaced by serving primary $replacement"
          exit 0
        fi
      fi
      ;;
  esac
  sleep 2
done
echo 'FAIL: no different serving primary before deadline' >&2
exit 1
