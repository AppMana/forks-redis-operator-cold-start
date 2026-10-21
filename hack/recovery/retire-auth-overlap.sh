#!/usr/bin/env bash
# Local-only explicit ACL retirement. Preserve rollback Secrets and every PVC.
# This qualifies a manual protocol; it is not automatic Secret rotation.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
namespace=redis-upgrade-canary-auth-rotation
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$namespace")
dir="$(cd "$(dirname "$0")" && pwd)"
[[ "$(kubectl --kubeconfig "$config" -n redis-operator get deployment redis-operator -o json | jq -r '.spec.template.spec.containers[].env[]?|select(.name=="WATCH_NAMESPACE")|.value')" == "$namespace" ]]
[[ "$("${k[@]}" get sts upgrade upgrade-s -o json | jq 'all(.items[]; .spec.updateStrategy.type=="OnDelete")')" == true ]]
bash "$dir/check-auth-rotation.sh" "$config"
[[ -z "$("${k[@]}" get secret rotation-acl-before-retirement --ignore-not-found -o name)" ]]
cli() {
  local secret="$1" pod="$2"; shift 2
  "${k[@]}" get secret "$secret" -o json | jq -r '.data.password|@base64d' |
    "${k[@]}" exec -i "$pod" -c upgrade -- sh -ec 'IFS= read -r REDISCLI_AUTH; export REDISCLI_AUTH; exec redis-cli --raw "$@"' sh "$@" 2>/dev/null
}
identity() {
  "${k[@]}" get pod upgrade-0 upgrade-1 upgrade-2 upgrade-s-0 upgrade-s-1 upgrade-s-2 -o json |
    jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'
  "${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'
}
before="$(identity)"
for i in 0 1 2; do
  [[ "$(cli redis-auth-before-rotation "upgrade-$i" PING)" == PONG ]]
  [[ "$(cli redis-auth "upgrade-$i" PING)" == PONG ]]
done
# Back up the complete ACL before editing; never overwrite an existing backup.
"${k[@]}" get secret rotation-acl -o json |
  jq '{apiVersion,kind,metadata:{name:"rotation-acl-before-retirement",namespace:.metadata.namespace},data,type}' |
  "${k[@]}" create -f - >/dev/null
old_hash="$("${k[@]}" get secret redis-auth-before-rotation -o json | jq -jr '.data.password|@base64d' | sha256sum)"; old_hash="${old_hash%% *}"
new_hash="$("${k[@]}" get secret redis-auth -o json | jq -jr '.data.password|@base64d' | sha256sum)"; new_hash="${new_hash%% *}"
[[ "$old_hash" != "$new_hash" ]]
"${k[@]}" get secret rotation-acl -o json |
  jq -ce --arg old "#$old_hash" --arg new "#$new_hash" '
    (.data["user.acl"]|@base64d|split("\n")) as $lines |
    [$lines[]|split(" ")|map(select(length>0))|select(.[0]=="user" and .[1]=="default")] as $defaults |
    if ($defaults|length)!=1 or ($defaults[0]|index($old))==null or ($defaults[0]|index($new))==null then error("unexpected ACL fixture") else
    ($lines|map(if (split(" ")|map(select(length>0))|.[0:2])==["user","default"] then split(" ")|map(select(.!=$old))|join(" ") else . end)|join("\n")) as $next |
    [{op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},
     {op:"replace",path:"/data/user.acl",value:($next|@base64)}] end' |
  "${k[@]}" patch secret rotation-acl --type=json --patch-file=/dev/stdin >/dev/null
digest="$("${k[@]}" get secret rotation-acl -o json | jq -jr '.data["user.acl"]|@base64d' | sha256sum)"; digest="${digest%% *}"
deadline=$((SECONDS+120)); projected=false
while (( SECONDS < deadline )); do
  ready=true
  for i in 0 1 2; do
    actual="$("${k[@]}" exec "upgrade-$i" -c upgrade -- sha256sum /etc/redis/acl/user.acl)"
    [[ "${actual%% *}" == "$digest" ]] || ready=false
  done
  if [[ "$ready" == true ]]; then projected=true; break; fi
  sleep 2
done
[[ "$projected" == true ]]
for i in 0 1 2; do
  actual="$("${k[@]}" exec "upgrade-$i" -c upgrade -- sha256sum /etc/redis/acl/user.acl)"
  [[ "${actual%% *}" == "$digest" ]]
  [[ "$(cli redis-auth "upgrade-$i" ACL LOAD)" == OK ]]
  [[ "$(cli redis-auth "upgrade-$i" PING)" == PONG ]]
  # A failed transport is not proof of rejection: require Redis's explicit
  # unauthenticated command response after redis-cli's AUTH attempt fails.
  reply="$(cli redis-auth-before-rotation "upgrade-$i" PING)"
  [[ "$reply" == 'NOAUTH Authentication required.' ]]
done
[[ "$(identity)" == "$before" ]]
bash "$dir/check-auth-rotation.sh" "$config"
echo 'PASS: old credential rejected, current credential and data retained; pod/PVC identities unchanged; rollback Secrets retained'
