#!/usr/bin/env bash
# Qualify controller/probes and deployed preStop with punctuation-heavy Secrets.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
export CANARY_AUTH_NAMESPACE=redis-upgrade-canary-auth-syntax
export CANARY_AUTH_VALUE=preserved-auth-syntax-20261021
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$CANARY_AUTH_NAMESPACE")
dir="$(cd "$(dirname "$0")" && pwd)"
"${k[@]}" wait redisreplication/upgrade --for=jsonpath='{.status.masterNode}' --timeout=180s
primary="$("${k[@]}" get redisreplication upgrade -o jsonpath='{.status.masterNode}')"
[[ "$primary" =~ ^upgrade-[012]$ ]]
reply="$("${k[@]}" get secret redis-auth -o json | jq -r '.data.password|@base64d' |
  "${k[@]}" exec -i "$primary" -c upgrade -- sh -ec 'IFS= read -r REDISCLI_AUTH; export REDISCLI_AUTH; redis-cli --raw SET appmana:rotation:retained "$1" NX' sh "$CANARY_AUTH_VALUE" 2>/dev/null)"
[[ -z "$reply" || "$reply" == OK ]]
passed=false
for attempt in {1..60}; do
  if bash "$dir/check-auth-rotation.sh" "$config"; then passed=true; break; fi
  sleep 2
done
[[ "$passed" == true ]]
bash "$dir/exercise-prestop-auth.sh" "$config"
echo 'PASS: Kubernetes bootstrap, current Secret clients, replication and graceful failover with special passwords'
