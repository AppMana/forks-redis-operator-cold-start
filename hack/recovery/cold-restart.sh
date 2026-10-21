#!/usr/bin/env bash
# Local-only restart acceptance; preserve all PVCs and existing test values.
set -euo pipefail
config="${1:?usage: cold-restart.sh LOCAL_KUBECONFIG EXPECTED_VALUE}"
expected="${2:?expected pre-migration value required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]] || exit 2
server="$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://127.0.0.1:* ]] || exit 2
export CANARY_NAMESPACE="${CANARY_NAMESPACE:?explicit canary namespace required}"
[[ "$CANARY_NAMESPACE" == redis-upgrade-canary* ]] || exit 2
dir="$(cd "$(dirname "$0")" && pwd)"
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$CANARY_NAMESPACE")
cli() { "${k[@]}" exec "$1" -c upgrade-sentinel -- redis-cli --raw -p 26379 "${@:2}"; }
bash "$dir/check.sh" "$config" "$expected"
[[ "$("${k[@]}" get redissentinel upgrade -o json | jq '.spec.configVolumeClaimTemplate != null')" == true ]]
pods=(upgrade-0 upgrade-1 upgrade-2 upgrade-sentinel-0 upgrade-sentinel-1 upgrade-sentinel-2)
uids=()
identities=()
for pod in "${pods[@]}"; do
  uids+=("$("${k[@]}" get pod "$pod" -o jsonpath='{.metadata.uid}')")
done
for ordinal in 0 1 2; do identities+=("$(cli "upgrade-sentinel-$ordinal" SENTINEL MYID)"); done
claims="$("${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
# Exact pod names only; no PVC or namespace deletion.
"${k[@]}" delete pod "${pods[@]}" --wait=false
deadline=$((SECONDS + 180))
passed=false
while (( SECONDS < deadline )); do
  replaced=true
  for i in "${!pods[@]}"; do
    uid="$("${k[@]}" get pod "${pods[$i]}" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    [[ -n "$uid" && "$uid" != "${uids[$i]}" ]] || replaced=false
  done
  if [[ "$replaced" == true ]] && timeout 20s bash "$dir/check.sh" "$config" "$expected" 2>/dev/null; then
    passed=true
    break
  fi
  sleep 2
done
[[ "$passed" == true ]] || { echo 'FAIL: cold restart did not converge' >&2; exit 1; }
[[ "$claims" == "$("${k[@]}" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" ]]
for ordinal in 0 1 2; do
  [[ "${identities[$ordinal]}" == "$(cli "upgrade-sentinel-$ordinal" SENTINEL MYID)" ]]
done
echo 'PASS: full cold restart preserved original data, Sentinel identities and all PVC UIDs'
