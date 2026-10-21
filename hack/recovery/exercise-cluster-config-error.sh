#!/usr/bin/env bash
# Dedicated local fixture; synthetic invalid input only, no credentials.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary-cluster-dynamic)
[[ "$("${k[@]}" get rediscluster dynamic -o jsonpath='{.status.state}')" == Ready ]]
[[ "$("${k[@]}" get rediscluster dynamic -o json | jq -c '.spec.redisConfig.dynamicConfig')" == '["slowlog-log-slower-than 5000"]' ]]
restore() {
  "${k[@]}" patch rediscluster dynamic --type=merge -p '{"spec":{"redisConfig":{"dynamicConfig":["slowlog-log-slower-than 5000"]}}}' >/dev/null
}
trap restore EXIT
marker="synthetic-config-value-$RANDOM"
started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq -n --arg value "slowlog-log-slower-than $marker" '{spec:{redisConfig:{dynamicConfig:[$value]}}}' |
  "${k[@]}" patch rediscluster dynamic --type=merge --patch-file=/dev/stdin
deadline=$((SECONDS+60)); reported=false
while (( SECONDS < deadline )); do
  logs="$(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-operator logs deploy/redis-operator --since-time="$started")"
  if grep -Fq "$marker" <<<"$logs"; then
    echo 'FAIL: configuration value appeared in logs; contents suppressed' >&2; exit 1
  fi
  if grep -Fq 'failed to set dynamic configuration entry 0 for pod dynamic-leader-0' <<<"$logs"; then
    reported=true; break
  fi
  sleep 2
done
[[ "$reported" == true ]]
for role in leader follower; do
  value="$("${k[@]}" exec "dynamic-$role-0" -c "dynamic-$role" -- redis-cli --raw CONFIG GET slowlog-log-slower-than | tail -n1)"
  [[ "$value" == 5000 ]]
done
restore
trap - EXIT
bash "$(dirname "$0")/exercise-ready-cluster-config.sh" "$config"
echo 'PASS: invalid config reported without value disclosure; valid updates recover'
