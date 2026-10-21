#!/usr/bin/env bash
# Local-only credential restart qualification; retain all PVCs and Secrets.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary-auth-rotation)
dir="$(cd "$(dirname "$0")" && pwd)"
bash "$dir/check-auth-rotation.sh" "$config"
[[ "$("${k[@]}" get redisreplication upgrade -o json | jq '.spec.sentinel.configVolumeClaimTemplate != null')" == true ]]
claims="$("${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
pods=(upgrade-0 upgrade-1 upgrade-2 upgrade-s-0 upgrade-s-1 upgrade-s-2)
uids=(); identities=()
for pod in "${pods[@]}"; do uids+=("$("${k[@]}" get pod "$pod" -o jsonpath='{.metadata.uid}')"); done
for ordinal in 0 1 2; do
  identities+=("$("${k[@]}" exec "upgrade-s-$ordinal" -c sentinel -- sh -ec 'REDISCLI_AUTH="$REDIS_PASSWORD"; export REDISCLI_AUTH; redis-cli --raw -p 26379 SENTINEL MYID')")
done
"${k[@]}" delete pod "${pods[@]}" --wait=false
deadline=$((SECONDS + 180)); passed=false
while (( SECONDS < deadline )); do
  replaced=true
  for i in "${!pods[@]}"; do
    uid="$("${k[@]}" get pod "${pods[$i]}" --ignore-not-found -o jsonpath='{.metadata.uid}')"
    [[ -n "$uid" && "$uid" != "${uids[$i]}" ]] || replaced=false
  done
  if [[ "$replaced" == true ]] && timeout 20s bash "$dir/check-auth-rotation.sh" "$config"; then passed=true; break; fi
  sleep 2
done
[[ "$passed" == true ]]
[[ "$claims" == "$("${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
for ordinal in 0 1 2; do
  [[ "${identities[$ordinal]}" == "$("${k[@]}" exec "upgrade-s-$ordinal" -c sentinel -- sh -ec 'REDISCLI_AUTH="$REDIS_PASSWORD"; export REDISCLI_AUTH; redis-cli --raw -p 26379 SENTINEL MYID')" ]]
  # Report whether bootstrap retained the temporary overlap; never print AUTH errors.
  reply="$("${k[@]}" get secret redis-auth-before-rotation -o json | jq -r '.data.password|@base64d' |
    "${k[@]}" exec -i "upgrade-$ordinal" -c upgrade -- sh -ec 'read -r REDISCLI_AUTH; export REDISCLI_AUTH; redis-cli --raw PING' 2>/dev/null)"
  if [[ "$reply" == PONG ]]; then echo "upgrade-$ordinal old credential still accepted"; else echo "upgrade-$ordinal old credential rejected"; fi
  if [[ "${REQUIRE_PASSWORD_OVERLAP:-false}" == true ]]; then
    [[ "$reply" == PONG ]] || { echo 'FAIL: password overlap did not survive bootstrap' >&2; exit 1; }
  fi
done
echo 'PASS: new credentials survive full pod replacement with retained data, PVC UIDs and Sentinel identities'
