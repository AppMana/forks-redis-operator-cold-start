#!/usr/bin/env bash
# Local-only projection check. Adds a harmless marker, not an ACL/password rule.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary-auth-rotation)
uids="$("${k[@]}" get pod upgrade-0 upgrade-1 upgrade-2 -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
marker="projection-$(date +%s)-$RANDOM"
jq -n --arg marker "$marker" '{stringData:{"projection-check":$marker}}' |
  "${k[@]}" patch secret rotation-acl --type=merge --patch-file=/dev/stdin
deadline=$((SECONDS+120)); projected=false
while (( SECONDS < deadline )); do
  ready=true
  for ordinal in 0 1 2; do
    if ! "${k[@]}" exec "upgrade-$ordinal" -c upgrade -- sh -ec 'test "$(cat /etc/redis/acl/projection-check)" = "$1"' sh "$marker" 2>/dev/null; then ready=false; fi
  done
  if [[ "$ready" == true ]]; then projected=true; break; fi
  sleep 2
done
[[ "$projected" == true ]]
[[ "$uids" == "$("${k[@]}" get pod upgrade-0 upgrade-1 upgrade-2 -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
bash "$(dirname "$0")/check-auth-rotation.sh" "$config"
echo 'PASS: Secret projection updated in all three running Redis pods without replacement; ACL rules unchanged'
