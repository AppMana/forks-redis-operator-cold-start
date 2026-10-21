#!/usr/bin/env bash
# Local rotation protocol rehearsal. Keeps both passwords valid; does not
# retire credentials, change Secrets, restart pods, or claim automatic rotation.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary-auth-rotation)
cli() {
  local secret="$1" pod="$2"; shift 2
  "${k[@]}" get secret "$secret" -o json | jq -r '.data.password|@base64d' |
    "${k[@]}" exec -i "$pod" -c upgrade -- sh -ec 'read -r REDISCLI_AUTH; export REDISCLI_AUTH; exec redis-cli --raw "$@"' sh "$@" 2>/dev/null
}
uids="$("${k[@]}" get pods -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
# Preflight every pod before any mutation; this fixture has no custom ACL file.
for ordinal in 0 1 2; do
  pod="upgrade-$ordinal"
  [[ "$(cli redis-auth-before-rotation "$pod" PING)" == PONG ]]
  [[ "$(cli redis-auth-before-rotation "$pod" CONFIG GET aclfile)" == aclfile ]]
  [[ "$(cli redis-auth-before-rotation "$pod" GET appmana:rotation:retained)" == preserved-auth-rotation-20261020 ]]
done
# Add only the new password hash. No reset/resetpass or permission changes.
for ordinal in 0 1 2; do
  pod="upgrade-$ordinal"
  result="$("${k[@]}" get secret redis-auth-before-rotation redis-auth -o json |
    jq -r '(.items[]|select(.metadata.name=="redis-auth-before-rotation")|.data.password|@base64d),(.items[]|select(.metadata.name=="redis-auth")|.data.password|@base64d)' |
    "${k[@]}" exec -i "$pod" -c upgrade -- sh -ec '
      read -r REDISCLI_AUTH; read -r desired; export REDISCLI_AUTH
      digest=$(printf %s "$desired" | sha256sum); digest=${digest%% *}
      printf "#%s" "$digest" | redis-cli --raw -x ACL SETUSER default' 2>/dev/null)"
  [[ "$result" == OK ]]
done
# All nodes must accept both passwords before any replica auth changes.
for ordinal in 0 1 2; do
  for secret in redis-auth-before-rotation redis-auth; do
    [[ "$(cli "$secret" "upgrade-$ordinal" PING)" == PONG ]]
  done
done
echo 'PASS: all three Redis nodes accept both credentials'
for ordinal in 0 1 2; do
  pod="upgrade-$ordinal"
  result="$("${k[@]}" get secret redis-auth -o json | jq -r '.data.password|@base64d' |
    "${k[@]}" exec -i "$pod" -c upgrade -- sh -ec '
      read -r desired; REDISCLI_AUTH=$desired; export REDISCLI_AUTH
      printf %s "$desired" | redis-cli --raw -x CONFIG SET masterauth' 2>/dev/null)"
  [[ "$result" == OK ]]
  [[ "$(cli redis-auth "$pod" CONFIG REWRITE)" == OK ]]
done
for ordinal in 0 1 2; do
  pod="upgrade-$ordinal"
  [[ "$(cli redis-auth "$pod" GET appmana:rotation:retained)" == preserved-auth-rotation-20261020 ]]
done
[[ "$uids" == "$("${k[@]}" get pods -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
echo 'PASS: replica authentication switched, configuration rewritten, test data and pod UIDs retained'
converged=false
for attempt in {1..30}; do
  all_ready=true
  for ordinal in 0 1 2; do
    if ! "${k[@]}" get secret redis-auth -o json | jq -r '.data.password|@base64d' |
      "${k[@]}" exec -i "upgrade-s-$ordinal" -c sentinel -- sh -ec '
        read -r desired
        actual=$(awk '\''$1=="sentinel" && $2=="auth-pass" && $3=="mymaster" {print $4}'\'' /etc/redis/sentinel.conf)
        test "$actual" = "$desired"'; then all_ready=false; fi
    quorum="$("${k[@]}" exec "upgrade-s-$ordinal" -c sentinel -- sh -ec 'REDISCLI_AUTH="$REDIS_PASSWORD"; export REDISCLI_AUTH; redis-cli --raw -p 26379 SENTINEL CKQUORUM mymaster')"
    [[ "$quorum" == OK* ]] || all_ready=false
  done
  if [[ "$all_ready" == true ]]; then converged=true; break; fi
  sleep 2
done
[[ "$converged" == true ]]
echo 'PASS: all Sentinels persisted the new auth-pass and report quorum'
echo 'Old credentials remain accepted. Restarts, retirement and automatic orchestration still require qualification.'
