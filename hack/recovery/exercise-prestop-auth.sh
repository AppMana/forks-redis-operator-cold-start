#!/usr/bin/env bash
# Execute the actual installed preStop hook without deleting pods, then verify
# its graceful demotion using current (separate) Redis and Sentinel Secrets.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary-auth-rotation)
dir="$(cd "$(dirname "$0")" && pwd)"
bash "$dir/check-auth-rotation.sh" "$config"
primary="$("${k[@]}" get redisreplication upgrade -o jsonpath='{.status.masterNode}')"
[[ "$primary" =~ ^upgrade-[012]$ ]]
"${k[@]}" get pod "$primary" -o json | jq -e 'any(.spec.containers[]|select(.name=="upgrade")|.env[]?; .name=="SENTINELCLI_AUTH" and .valueFrom.secretKeyRef.name=="sentinel-auth")' >/dev/null
uids="$("${k[@]}" get pods -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
hook="$("${k[@]}" get pod "$primary" -o json | jq -r '.spec.containers[]|select(.name=="upgrade")|.lifecycle.preStop.exec.command[2]')"
[[ "$hook" == *SENTINELCLI_AUTH* ]]
printf '%s\n' "$hook" | timeout 40s "${k[@]}" exec -i "$primary" -c upgrade -- sh -s
passed=false
for attempt in {1..45}; do
  if PREVIOUS_PRIMARY="$primary" bash "$dir/check-auth-rotation.sh" "$config"; then passed=true; break; fi
  sleep 2
done
[[ "$passed" == true ]]
[[ "$uids" == "$("${k[@]}" get pods -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
echo 'PASS: installed preStop hook triggered failover with separate credentials; all pods and data retained'
