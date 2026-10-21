#!/usr/bin/env bash
# Restarts only the three explicitly named local fixture pods; retains all PVCs.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary-cluster-restart)
cluster=redis-cluster-v1beta2
"${k[@]}" wait "rediscluster/$cluster" --for=jsonpath='{.status.state}'=Ready --timeout=180s
pvc_uids() { "${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'; }
pod_uids() { "${k[@]}" get pod "$cluster-leader-0" "$cluster-leader-1" "$cluster-leader-2" -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'; }
pvcs="$(pvc_uids)"
before="$(pod_uids)"
"${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- sh -eu -c '
  for i in $(seq 0 99); do
    redis-cli -c SET "appmana:restart:$i" "retained-$i" NX >/dev/null
    [ "$(redis-cli -c --raw GET "appmana:restart:$i")" = "retained-$i" ]
  done'
for i in 0 1 2; do
  [[ "$("${k[@]}" exec "$cluster-leader-$i" -c "$cluster-leader" -- redis-cli --raw DBSIZE)" -gt 0 ]]
  [[ "$("${k[@]}" exec "$cluster-leader-$i" -c "$cluster-leader" -- redis-cli SAVE)" == OK ]]
done
"${k[@]}" delete pod "$cluster-leader-0" "$cluster-leader-1" "$cluster-leader-2" --wait=false
deadline=$((SECONDS+180))
disrupted=false
recovered=false
while (( SECONDS < deadline )); do
  state="$("${k[@]}" get rediscluster "$cluster" -o jsonpath='{.status.state}')"
  if [[ "$state" != Ready ]]; then disrupted=true; fi
  if [[ "$state" == Ready && "$disrupted" == true ]]; then recovered=true; break; fi
  sleep 1
done
[[ "$recovered" == true ]] || { echo 'FAIL: no observed non-Ready to Ready recovery within 180s'; exit 1; }
for i in 0 1 2; do
  info="$("${k[@]}" exec "$cluster-leader-$i" -c "$cluster-leader" -- redis-cli --raw CLUSTER INFO | tr -d '\r')"
  [[ "$info" == *$'cluster_state:ok\n'* && "$info" == *$'cluster_slots_ok:16384\n'* ]]
done
"${k[@]}" exec "$cluster-leader-0" -c "$cluster-leader" -- sh -eu -c '
  for i in $(seq 0 99); do
    [ "$(redis-cli -c --raw GET "appmana:restart:$i")" = "retained-$i" ]
  done'
[[ "$(pvc_uids)" == "$pvcs" ]]
after="$(pod_uids)"
jq -en --argjson before "$before" --argjson after "$after" \
  '$before | all(. as $old | $after | any(.name==$old.name and .uid!=$old.uid))' >/dev/null
echo 'PASS: all three leaders replaced; Ready serves all 100 retained values; PVC UIDs unchanged'
