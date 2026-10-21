#!/usr/bin/env bash
# Local-only runtime ACL qualification, preserving all credential rules.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary-auth-rotation)
dir="$(cd "$(dirname "$0")" && pwd)"
bash "$dir/check-auth-rotation.sh" "$config"
uids="$("${k[@]}" get pod upgrade-0 upgrade-1 upgrade-2 -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
probe="rotation-disabled-$(date +%s)-$RANDOM"
"${k[@]}" get secret rotation-acl -o json | jq -ce --arg probe "$probe" '
  (.data["user.acl"] | @base64d) as $acl |
  [{op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},
   {op:"replace",path:"/data/user.acl",value:(($acl + "\nuser " + $probe + " off -@all\n") | @base64)}]' |
  "${k[@]}" patch secret rotation-acl --type=json --patch-file=/dev/stdin
digest="$("${k[@]}" get secret rotation-acl -o json | jq -jr '.data["user.acl"]|@base64d' | sha256sum | cut -d ' ' -f1)"
deadline=$((SECONDS + 120)); projected=false
while (( SECONDS < deadline )); do
  ready=true
  for ordinal in 0 1 2; do
    actual="$("${k[@]}" exec "upgrade-$ordinal" -c upgrade -- sha256sum /etc/redis/acl/user.acl | cut -d ' ' -f1)"
    [[ "$actual" == "$digest" ]] || ready=false
  done
  if [[ "$ready" == true ]]; then projected=true; break; fi
  sleep 2
done
[[ "$projected" == true ]]
cli() {
  local pod="$1"; shift
  "${k[@]}" get secret redis-auth -o json | jq -r '.data.password|@base64d' |
    "${k[@]}" exec -i "$pod" -c upgrade -- sh -ec 'read -r REDISCLI_AUTH; export REDISCLI_AUTH; exec redis-cli --raw "$@"' sh "$@" 2>/dev/null
}
for ordinal in 0 1 2; do
  users="$(cli "upgrade-$ordinal" ACL USERS)"
  if ! grep -Fxq "$probe" <<<"$users"; then
    echo "upgrade-$ordinal: file projected but runtime ACL unchanged"
  fi
done
for ordinal in 0 1 2; do
  # Recheck projection immediately before mutation. Do not print Redis errors,
  # which can echo ACL contents. Redis validates each ACL LOAD atomically;
  # this is not an atomic transaction across the three Redis servers.
  actual="$("${k[@]}" exec "upgrade-$ordinal" -c upgrade -- sha256sum /etc/redis/acl/user.acl | cut -d ' ' -f1)"
  [[ "$actual" == "$digest" ]]
  reply="$(cli "upgrade-$ordinal" ACL LOAD)"
  [[ "$reply" == OK ]] || { echo 'FAIL: ACL reload rejected; inspect securely' >&2; exit 1; }
  users="$(cli "upgrade-$ordinal" ACL USERS)"
  grep -Fxq "$probe" <<<"$users"
done
[[ "$uids" == "$("${k[@]}" get pod upgrade-0 upgrade-1 upgrade-2 -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
for ordinal in 0 1 2; do
  reply="$("${k[@]}" get secret redis-auth-before-rotation -o json | jq -r '.data.password|@base64d' |
    "${k[@]}" exec -i "upgrade-$ordinal" -c upgrade -- sh -ec 'read -r REDISCLI_AUTH; export REDISCLI_AUTH; redis-cli --raw PING' 2>/dev/null)"
  [[ "$reply" == PONG ]]
done
bash "$dir/check-auth-rotation.sh" "$config"
echo 'PASS: runtime ACL reload, unchanged pod UIDs, both credentials and retained data'
