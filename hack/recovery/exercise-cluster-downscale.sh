#!/usr/bin/env bash
# Downscale only the named disposable fixture. Keep all PVCs and verify data.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
namespace=redis-upgrade-canary-cluster-downscale
leaders=1
followers=0
shrinking_role=leader
case "${2:-baseline}" in
  baseline) ;;
  interrupted) namespace=redis-upgrade-canary-cluster-downscale-interrupted-routable ;;
  followers) namespace=redis-upgrade-canary-cluster-follower-downscale; leaders=3; followers=1; shrinking_role=follower ;;
  combined) namespace=redis-upgrade-canary-cluster-combined-downscale; followers=1 ;;
  *) echo 'Expected baseline, interrupted, followers or combined'; exit 1 ;;
esac
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$namespace")
cluster=redis-cluster-v1beta2
"${k[@]}" wait "rediscluster/$cluster" --for=jsonpath='{.status.state}'=Ready --timeout=180s
[[ "$("${k[@]}" get sts "$cluster-leader" -o jsonpath='{.spec.replicas}')" == 3 ]]
[[ "$("${k[@]}" get sts "$cluster-$shrinking_role" -o jsonpath='{.spec.replicas}')" == 3 ]]
pvc_uids() { "${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'; }
pvcs="$(pvc_uids)"
leader_uid="$("${k[@]}" get pod "$cluster-leader-0" -o jsonpath='{.metadata.uid}')"
retained_uids() {
  for role in leader follower; do
    count="$leaders"; [[ "$role" == leader ]] || count="$followers"
    for ((i=0; i<count; i++)); do
      "${k[@]}" get pod "$cluster-$role-$i" -o jsonpath='{.metadata.uid}{"\n"}'
    done
  done
}
before_retained="$(retained_uids)"
"${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- sh -eu -c '
  for i in $(seq 0 99); do
    redis-cli -c SET "appmana:downscale:$i" "retained-$i" NX >/dev/null
    [ "$(redis-cli -c --raw GET "appmana:downscale:$i")" = "retained-$i" ]
  done'
for i in 0 1 2; do
  [[ "$("${k[@]}" exec "$cluster-leader-$i" -c "$cluster-leader" -- redis-cli --raw DBSIZE)" -gt 0 ]]
  [[ "$("${k[@]}" exec "$cluster-leader-$i" -c "$cluster-leader" -- redis-cli SAVE)" == OK ]]
done
if [[ "${2:-baseline}" == combined ]]; then
  # Guarantee an obsolete follower is attached to a retained shard. Attachment
  # to retiring leaders alone must not determine which followers get retired.
  retained_id="$("${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- redis-cli --raw CLUSTER MYID)"
  [[ "$retained_id" =~ ^[a-f0-9]{40}$ ]]
  [[ "$("${k[@]}" exec "$cluster-follower-2" -c "$cluster-follower" -- redis-cli --raw CLUSTER REPLICATE "$retained_id")" == OK ]]
  "${k[@]}" exec "$cluster-follower-2" -c "$cluster-follower" -- redis-cli --raw CLUSTER NODES | awk -v id="$retained_id" '$3 ~ /myself/ {found=1; good=($3 ~ /slave/ && $4==id)} END {exit !(found && good)}'
  echo 'Verified obsolete follower-2 follows retained leader-0 before combined shrink'
fi
if [[ "${2:-baseline}" == interrupted ]]; then
  # Model the persisted boundary after the first retirement succeeded and the
  # reconcile stopped. Do not let the controller race this setup. This is a
  # boundary-state injection, not a claim that the operator process was killed.
  "${k[@]}" patch rediscluster "$cluster" --type=merge -p '{"metadata":{"annotations":{"rediscluster.opstreelabs.in/skip-reconcile":"true"}},"spec":{"clusterSize":1,"redisLeader":{"replicas":1}}}'
  source_id="$("${k[@]}" exec "$cluster-leader-2" -c "$cluster-leader" -- redis-cli --raw CLUSTER MYID)"
  target_id="$("${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- redis-cli --raw CLUSTER MYID)"
  target_ip="$("${k[@]}" get pod "$cluster-leader-0" -o jsonpath='{.status.podIP}')"
  [[ "$target_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ && "$target_ip" != 127.* ]]
  [[ "$source_id" =~ ^[a-f0-9]{40}$ && "$target_id" =~ ^[a-f0-9]{40}$ && "$source_id" != "$target_id" ]]
  slots="$("${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- redis-cli --json CLUSTER SLOTS | jq --arg id "$source_id" '[.[]|select(.[2][2]==$id)|.[1]-.[0]+1]|add')"
  [[ "$slots" =~ ^[0-9]+$ && "$slots" -gt 0 && "$slots" -lt 16384 ]]
  timeout 60s "${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- redis-cli --cluster reshard "$target_ip:6379" --cluster-from "$source_id" --cluster-to "$target_id" --cluster-slots "$slots" --cluster-yes >/dev/null
  "${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- redis-cli --json CLUSTER SLOTS | jq -e --arg id "$source_id" 'all(.[]; .[2][2]!=$id)' >/dev/null
  timeout 30s "${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- redis-cli --cluster del-node "$target_ip:6379" "$source_id" >/dev/null
  [[ "$("${k[@]}" get sts "$cluster-leader" -o jsonpath='{.spec.replicas}')" == 3 ]]
  "${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- redis-cli --raw CLUSTER NODES | awk -v source="$source_id" 'NF {n++; if ($1==source) bad=1} END {exit !(n==2 && !bad)}'
  echo 'Injected interrupted retirement boundary: two members remain, StatefulSet still has three replicas'
  "${k[@]}" annotate rediscluster "$cluster" rediscluster.opstreelabs.in/skip-reconcile-
else
  patch="$(jq -cn --argjson leaders "$leaders" --argjson followers "$followers" '{spec:{clusterSize:$leaders,redisLeader:{replicas:$leaders},redisFollower:{replicas:$followers}}}')"
  "${k[@]}" patch rediscluster "$cluster" --type=merge -p "$patch"
fi
deadline=$((SECONDS+180))
converged=false
while (( SECONDS < deadline )); do
  converged=true
  for role in leader follower; do
    expected="$leaders"; [[ "$role" == leader ]] || expected="$followers"
    replicas="$("${k[@]}" get sts "$cluster-$role" -o jsonpath='{.spec.replicas}')"
    count="$("${k[@]}" get pods -l app="$cluster-$role" -o json | jq '.items|length')"
    if [[ "$replicas" != "$expected" || "$count" != "$expected" ]]; then converged=false; fi
  done
  [[ "$converged" == true ]] && break
  sleep 2
done
[[ "$converged" == true ]] || { echo 'FAIL: three-to-one downscale did not finish within 180s; fixture and PVCs retained'; exit 1; }
"${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- sh -eu -c '
  for i in $(seq 0 99); do
    [ "$(redis-cli -c --raw GET "appmana:downscale:$i")" = "retained-$i" ]
  done
  redis-cli --raw CLUSTER INFO' | tr -d '\r' | awk -F: -v expected="$((leaders+followers))" 'BEGIN { ok=0; slots=0; nodes=0 } /^cluster_state:ok$/ {ok=1} /^cluster_slots_ok:16384$/ {slots=1} $1=="cluster_known_nodes" && $2==expected {nodes=1} END {exit !(ok && slots && nodes)}'
[[ "$(pvc_uids)" == "$pvcs" ]]
[[ "$("${k[@]}" get pod "$cluster-leader-0" -o jsonpath='{.metadata.uid}')" == "$leader_uid" ]]
[[ "$(retained_uids)" == "$before_retained" ]]
"${k[@]}" wait "rediscluster/$cluster" --for=jsonpath='{.status.state}'=Ready --timeout=45s
"${k[@]}" wait "rediscluster/$cluster" --for=jsonpath='{.status.readyLeaderReplicas}'="$leaders" --timeout=45s
# Zero is omitted by the API's omitempty tag; it is not the string "0".
deadline=$((SECONDS+45))
while ! "${k[@]}" get rediscluster "$cluster" -o json | jq -e --argjson expected "$followers" '(.status.readyFollowerReplicas // 0)==$expected' >/dev/null; do
  (( SECONDS < deadline )) || { echo 'FAIL: ready follower count did not converge'; exit 1; }
  sleep 1
done
echo "PASS: $shrinking_role three-to-one downscale retained all 100 values, survivor pod UIDs and every PVC UID"
