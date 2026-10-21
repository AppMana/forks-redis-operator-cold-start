#!/usr/bin/env bash
# Local-only: reuse the existing ACL Secret API to retain password overlap.
# Leaves OnDelete enabled; pod replacement is a separate explicit test.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary-auth-rotation)
bash "$(dirname "$0")/check-auth-rotation.sh" "$config"
[[ "$("${k[@]}" get redisreplication upgrade -o json | jq '.spec.acl == null')" == true ]]
[[ -z "$("${k[@]}" get secret rotation-acl --ignore-not-found -o name)" ]]
uids="$("${k[@]}" get pods -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
# Hold both controllers before changing templates.
"${k[@]}" patch redisreplication upgrade --type=merge -p '{"spec":{"kubernetesConfig":{"updateStrategy":{"type":"OnDelete"}},"sentinel":{"updateStrategy":{"type":"OnDelete"}}}}'
held=false
for attempt in {1..30}; do
  if [[ "$("${k[@]}" get sts upgrade upgrade-s -o json | jq 'all(.items[]; .spec.updateStrategy.type=="OnDelete")')" == true ]]; then held=true; break; fi
  sleep 2
done
[[ "$held" == true ]]
# Only hashes are written into the ACL Secret; values never reach stdout.
old_hash="$("${k[@]}" get secret redis-auth-before-rotation -o json | jq -j '.data.password|@base64d' | sha256sum)"; old_hash="${old_hash%% *}"
new_hash="$("${k[@]}" get secret redis-auth -o json | jq -j '.data.password|@base64d' | sha256sum)"; new_hash="${new_hash%% *}"
printf 'user default on #%s #%s ~* &* +@all\n' "$old_hash" "$new_hash" |
  jq -Rs '{apiVersion:"v1",kind:"Secret",metadata:{name:"rotation-acl",namespace:"redis-upgrade-canary-auth-rotation"},stringData:{"user.acl":.}}' |
  "${k[@]}" create -f -
"${k[@]}" patch redisreplication upgrade --type=merge -p '{"spec":{"acl":{"secret":{"secretName":"rotation-acl"}}}}'
prepared=false
for attempt in {1..30}; do
  if "${k[@]}" get sts upgrade -o json | jq -e 'any(.spec.template.spec.volumes[]?; .secret.secretName=="rotation-acl")' >/dev/null; then prepared=true; break; fi
  sleep 2
done
[[ "$prepared" == true ]]
[[ "$uids" == "$("${k[@]}" get pods -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
echo 'PASS: existing ACL API configured with both password hashes; every pod retained under OnDelete'
