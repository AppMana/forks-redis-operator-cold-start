#!/usr/bin/env bash
# Local diagnostic: retain the old test credential and report rotation behavior.
# Does not restart pods, force a primary, or delete any data.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
namespace=redis-upgrade-canary-auth-rotation
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$namespace")
cli() {
  local secret="$1" pod="$2"; shift 2
  "${k[@]}" get secret "$secret" -o json | jq -r '.data.password|@base64d' |
    "${k[@]}" exec -i "$pod" -c upgrade -- sh -ec 'read -r REDISCLI_AUTH; export REDISCLI_AUTH; exec redis-cli --raw "$@"' sh "$@" 2>/dev/null
}
[[ -z "$("${k[@]}" get secret redis-auth-before-rotation --ignore-not-found -o name)" ]]
primary=""
for ordinal in 0 1 2; do
  pod="upgrade-$ordinal"
  info="$(cli redis-auth "$pod" INFO replication | tr -d '\r')"
  if [[ "$info" == *$'role:master\n'* ]]; then
    [[ -z "$primary" && "$info" == *$'connected_slaves:2\n'* ]]
    primary="$pod"
  else
    [[ "$info" == *$'role:slave\n'* && "$info" == *$'master_link_status:up\n'* ]]
  fi
  [[ "$("${k[@]}" exec "upgrade-s-$ordinal" -c sentinel -- sh -ec 'REDISCLI_AUTH="$REDIS_PASSWORD"; export REDISCLI_AUTH; redis-cli --raw -p 26379 SENTINEL CKQUORUM mymaster')" == OK* ]]
done
[[ -n "$primary" ]]
[[ "$(cli redis-auth "$primary" SET appmana:rotation:retained preserved-auth-rotation-20261020)" == OK ]]
[[ "$(cli redis-auth "$primary" WAIT 2 5000)" == 2 ]]
uids="$("${k[@]}" get pods -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
"${k[@]}" get secret redis-auth -o json | jq '{apiVersion,kind,metadata:{name:"redis-auth-before-rotation",namespace:.metadata.namespace},data}' | "${k[@]}" create -f -
openssl rand -hex 24 | jq -Rs --arg ns "$namespace" '{apiVersion:"v1",kind:"Secret",metadata:{name:"redis-auth",namespace:$ns},stringData:{password:rtrimstr("\n")}}' | "${k[@]}" apply -f -
echo 'Updated only Redis test Secret; old credential retained separately; observing for 90 seconds'
for interval in 1 2 3; do
  sleep 30
  echo "Observation $interval/3"
  for ordinal in 0 1 2; do
    pod="upgrade-$ordinal"
    old_ok=false; new_ok=false; data_ok=false
    [[ "$(cli redis-auth-before-rotation "$pod" PING)" != PONG ]] || old_ok=true
    [[ "$(cli redis-auth "$pod" PING)" != PONG ]] || new_ok=true
    [[ "$(cli redis-auth-before-rotation "$pod" GET appmana:rotation:retained)" != preserved-auth-rotation-20261020 ]] || data_ok=true
    echo "$pod old_auth=$old_ok new_auth=$new_ok retained_data_via_old_auth=$data_ok"
  done
done
[[ "$uids" == "$("${k[@]}" get pods -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]] && echo 'Pod UIDs unchanged' || echo 'Pod UIDs changed'
