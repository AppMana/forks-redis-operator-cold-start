#!/usr/bin/env bash
# Local-only acceptance for migrating retained DNS election state to IP mode.
set -euo pipefail
config="${1:?usage: embedded-disable-hostnames.sh LOCAL_KUBECONFIG EXPECTED_VALUE}"
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
cli() { "${k[@]}" exec "upgrade-s-$1" -c sentinel -- redis-cli --raw -p 26379 "${@:2}"; }
bash "$dir/check.sh" "$config" "$expected"
[[ "$("${k[@]}" get redisreplication upgrade -o json | jq '.spec.sentinel.configVolumeClaimTemplate != null and .spec.sentinel.resolveHostnames == "yes" and .spec.sentinel.announceHostnames == "yes"')" == true ]]
identities=()
for ordinal in 0 1 2; do identities+=("$(cli "$ordinal" SENTINEL MYID)"); done
claims="$("${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
"${k[@]}" patch redisreplication upgrade --type merge -p '{"spec":{"sentinel":{"resolveHostnames":"no","announceHostnames":"no"}}}'
deadline=$((SECONDS+180))
passed=false
while ((SECONDS<deadline)); do
  flags=true
  if ! "${k[@]}" get sts upgrade upgrade-s -o json | jq -e 'all(.items[]; .status.observedGeneration == .metadata.generation and .status.currentRevision == .status.updateRevision and .status.readyReplicas == .spec.replicas and any(.spec.template.spec.containers[0].env[]; .name == "ANNOUNCE_HOSTNAMES" and .value == "no"))' >/dev/null; then
    flags=false
  fi
  for ordinal in 0 1 2; do
    [[ "$(cli "$ordinal" SENTINEL CONFIG GET resolve-hostnames 2>/dev/null | tail -1)" == no ]] || flags=false
    [[ "$(cli "$ordinal" SENTINEL CONFIG GET announce-hostnames 2>/dev/null | tail -1)" == no ]] || flags=false
  done
  if [[ "$flags" == true ]] && timeout 20s bash "$dir/check.sh" "$config" "$expected" 2>/dev/null; then
    passed=true
    break
  fi
  sleep 2
done
[[ "$passed" == true ]] || { echo 'FAIL: hostname disable did not converge' >&2; exit 1; }
[[ "$claims" == "$("${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
for ordinal in 0 1 2; do
  [[ "${identities[$ordinal]}" == "$(cli "$ordinal" SENTINEL MYID)" ]]
done
echo 'PASS: hostname mode disabled with data, Sentinel identities and PVC UIDs preserved'
