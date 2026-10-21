#!/usr/bin/env bash
# Local canary only: prove a template update does not bypass OnDelete.
set -euo pipefail
config="${1:?usage: embedded-ondelete.sh LOCAL_KUBECONFIG EXPECTED_VALUE}"
expected="${2:?expected value required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]] || exit 2
server="$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://127.0.0.1:* ]] || exit 2
export CANARY_NAMESPACE="${CANARY_NAMESPACE:?explicit namespace required}"
[[ "$CANARY_NAMESPACE" == redis-upgrade-canary* ]] || exit 2
export CANARY_EMBEDDED_SENTINEL=true
dir="$(cd "$(dirname "$0")" && pwd)"
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$CANARY_NAMESPACE")
uids() { "${k[@]}" get pod upgrade-s-0 upgrade-s-1 upgrade-s-2 -o json | jq -Sc '[.items[] | {name:.metadata.name,uid:.metadata.uid}] | sort_by(.name)'; }
bash "$dir/check.sh" "$config" "$expected"
original="$(uids)"
cpu="$("${k[@]}" get redisreplication upgrade -o jsonpath='{.spec.sentinel.resources.requests.cpu}')"
[[ "$cpu" == 25m ]] || { echo 'Fixture must start at 25m CPU' >&2; exit 2; }
"${k[@]}" patch redisreplication upgrade --type merge -p '{"spec":{"sentinel":{"updateStrategy":{"type":"OnDelete","rollingUpdate":null}}}}'
deadline=$((SECONDS+60))
while [[ "$("${k[@]}" get sts upgrade-s -o jsonpath='{.spec.updateStrategy.type}')" != OnDelete ]]; do
  ((SECONDS<deadline)) || exit 1
  sleep 2
done
"${k[@]}" patch redisreplication upgrade --type merge -p '{"spec":{"sentinel":{"resources":{"requests":{"cpu":"26m"}}}}}'
deadline=$((SECONDS+60))
while [[ "$("${k[@]}" get sts upgrade-s -o jsonpath='{.spec.template.spec.containers[0].resources.requests.cpu}')" != 26m ]]; do
  ((SECONDS<deadline)) || exit 1
  sleep 2
done
for iteration in {1..15}; do
  [[ "$(uids)" == "$original" ]] || { echo 'FAIL: OnDelete replaced a Sentinel pod' >&2; exit 1; }
  sleep 2
done
bash "$dir/check.sh" "$config" "$expected"
# Restore the original template before releasing the hold. Leave OnDelete in
# place on any earlier failure so a failed test cannot trigger an extra roll.
"${k[@]}" patch redisreplication upgrade --type merge -p '{"spec":{"sentinel":{"resources":{"requests":{"cpu":"25m"}}}}}'
deadline=$((SECONDS+60))
while [[ "$("${k[@]}" get sts upgrade-s -o jsonpath='{.spec.template.spec.containers[0].resources.requests.cpu}')" != 25m ]]; do
  ((SECONDS<deadline)) || exit 1
  sleep 2
done
"${k[@]}" patch redisreplication upgrade --type merge -p '{"spec":{"sentinel":{"updateStrategy":{"type":"RollingUpdate"}}}}'
deadline=$((SECONDS+60))
while ! "${k[@]}" get sts upgrade-s -o json | jq -e '.spec.updateStrategy.type == "RollingUpdate" and .status.observedGeneration == .metadata.generation and .status.currentRevision == .status.updateRevision' >/dev/null; do
  ((SECONDS<deadline)) || exit 1
  sleep 2
done
[[ "$(uids)" == "$original" ]]
echo 'PASS: OnDelete held all three Sentinel pod UIDs across a template change; data and quorum preserved'
