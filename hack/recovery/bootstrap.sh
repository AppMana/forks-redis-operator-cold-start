#!/usr/bin/env bash
# Initialize synthetic keys only in the explicitly selected local canary.
set -euo pipefail
config="${1:?usage: bootstrap.sh LOCAL_KUBECONFIG}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]] || exit 2
server="$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://127.0.0.1:* ]] || exit 2
export CANARY_NAMESPACE="${CANARY_NAMESPACE:?set a dedicated canary namespace}"
[[ "$CANARY_NAMESPACE" == redis-upgrade-canary* ]] || exit 2
dir="$(cd "$(dirname "$0")" && pwd)"
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$CANARY_NAMESPACE")
deadline=$((SECONDS+180))
while ((SECONDS<deadline)); do
  count=$("${k[@]}" get pods -l 'app in (upgrade,upgrade-sentinel)' -o json | jq '.items|length')
  [[ "$count" == 6 ]] && break
  sleep 2
done
[[ "$count" == 6 ]]
"${k[@]}" wait pods -l 'app in (upgrade,upgrade-sentinel)' --for=condition=Ready --timeout=180s
deadline=$((SECONDS+120))
primary=''
while ((SECONDS<deadline)); do
  primary=$("${k[@]}" get pods -l app=upgrade,redis-role=master -o json | jq -r 'if (.items|length)==1 then .items[0].metadata.name else empty end')
  [[ -n "$primary" ]] && break
  sleep 2
done
[[ -n "$primary" ]]
expected="${CANARY_VALUE:-preserved-bootstrap-$(date +%s)}"
"${k[@]}" exec "$primary" -- redis-cli SET appmana:upgrade:sentinel-regression "$expected"
"${k[@]}" exec "$primary" -- redis-cli -n 1 SET appmana:upgrade:secondary-database "$expected"
deadline=$((SECONDS+180))
while ((SECONDS<deadline)); do
  if timeout 20s bash "$dir/check.sh" "$config" "$expected" 2>/dev/null; then
    echo "PASS: bootstrap and replicated value $expected"
    exit 0
  fi
  sleep 2
done
echo "FAIL: bootstrap convergence deadline" >&2
exit 1
