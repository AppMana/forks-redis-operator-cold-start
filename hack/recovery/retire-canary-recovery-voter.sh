#!/usr/bin/env bash
# Local-only, one-voter contraction rehearsal. Not a production rollout tool.
# Preserve every source config and require a healthy, unpartitioned baseline.
set -euo pipefail
config="${1:?qualified local kubeconfig required}"
target="${2:?desired recovery replicas, one fewer than current, required}"
[[ "$target" =~ ^[0-3]$ ]]
namespace=redis-upgrade-canary-migration
[[ "$(kubectl --kubeconfig "$config" config current-context)" == kind-redis-recovery-qualified-20261021 ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --request-timeout=10s -n "$namespace")
dir="$(cd "$(dirname "$0")" && pwd)"
current="$("${k[@]}" get deployment upgrade-recovery-sentinel -o jsonpath='{.spec.replicas}')"
[[ "$current" -eq "$((target+1))" ]]
health() {
  local count="$1"
  if ((count>3)); then
    CANARY_NAMESPACE="$namespace" CANARY_SENTINEL_COUNT="$count" CANARY_RECOVERY_SENTINEL_SELECTOR=app=upgrade-recovery-sentinel bash "$dir/check.sh" "$config" preserved-qualified-migration-20261021
  else
    CANARY_NAMESPACE="$namespace" CANARY_SENTINEL_COUNT=3 CANARY_RECOVERY_SENTINEL_SELECTOR= bash "$dir/check.sh" "$config" preserved-qualified-migration-20261021
  fi
}
cli() { "${k[@]}" exec "$1" -- redis-cli --raw -p 26379 "${@:2}" 2>/dev/null; }
inventory() {
  printf '%s\n' upgrade-sentinel-0 upgrade-sentinel-1 upgrade-sentinel-2
  "${k[@]}" get pods -l app=upgrade-recovery-sentinel -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
}
data_identity() {
  "${k[@]}" get pods,pvc -o json | jq -Sc '[.items[]|select(.kind=="PersistentVolumeClaim" or (.metadata.name|test("^upgrade-[012]$")))|{kind,name:.metadata.name,uid:.metadata.uid}]|sort_by(.kind,.name)'
}
health "$((current+3))"
before="$(data_identity)"
deployment="$("${k[@]}" get deployment upgrade-recovery-sentinel -o json)"
archive="$("${k[@]}" get configmap "retire-to-$target-deployment" --ignore-not-found -o json)"
if [[ -n "$archive" ]]; then
  [[ "$(jq -Sc '.data["object.json"]|fromjson|{uid:.metadata.uid,spec:.spec}' <<<"$archive")" == "$(jq -Sc '{uid:.metadata.uid,spec:.spec}' <<<"$deployment")" ]]
else
  printf '%s\n' "$deployment" | "${k[@]}" create configmap "retire-to-$target-deployment" --from-file=object.json=/dev/stdin
fi
mapfile -t original < <(inventory)
for i in "${!original[@]}"; do
  pod="${original[$i]}"
  path=/config/sentinel.conf
  [[ "$pod" != upgrade-sentinel-* ]] || path=/etc/redis/sentinel.conf
  # Configuration can contain ACL/auth directives: retain it as a Secret and
  # print only hashes/completion, never the configuration or credential values.
  hash_before="$("${k[@]}" exec "$pod" -- sha256sum "$path" 2>/dev/null | awk '{print $1}')"
  "${k[@]}" exec "$pod" -- cat "$path" 2>/dev/null | "${k[@]}" create secret generic "retire-to-$target-config-$i" --from-file=sentinel.conf=/dev/stdin
  copied="$("${k[@]}" get secret "retire-to-$target-config-$i" -o json | jq -j '.data["sentinel.conf"]|@base64d' | sha256sum | awk '{print $1}')"
  hash_after="$("${k[@]}" exec "$pod" -- sha256sum "$path" 2>/dev/null | awk '{print $1}')"
  [[ "$hash_before" == "$copied" && "$hash_after" == "$copied" ]]
  "${k[@]}" annotate secret "retire-to-$target-config-$i" "source-pod=$pod" "source-uid=$("${k[@]}" get pod "$pod" -o jsonpath='{.metadata.uid}')" >/dev/null
done
"${k[@]}" scale deployment upgrade-recovery-sentinel --current-replicas="$current" --replicas="$target"
deadline=$((SECONDS+120)); stopped=false
while ((SECONDS<deadline)); do
  count="$("${k[@]}" get pods -l app=upgrade-recovery-sentinel -o json | jq '.items|length')"
  if [[ "$count" -eq "$target" ]]; then stopped=true; break; fi
  sleep 2
done
[[ "$stopped" == true ]]
mapfile -t remaining < <(inventory)
[[ "${#remaining[@]}" -eq "$((target+3))" ]]
for pod in "${remaining[@]}"; do
  [[ "$(cli "$pod" SENTINEL CKQUORUM upgrade)" == OK* ]]
done
for pod in "${remaining[@]}"; do
  id="$(cli "$pod" SENTINEL MYID)"
  epoch="$(cli "$pod" SENTINEL MASTER upgrade | awk '$0=="config-epoch" {getline; print}')"
  [[ "$(cli "$pod" SENTINEL RESET upgrade)" == 1 ]]
  sleep 30
  [[ "$(cli "$pod" SENTINEL MYID)" == "$id" ]]
  metadata="$(cli "$pod" SENTINEL MASTER upgrade)"
  [[ "$(awk '$0=="config-epoch" {getline; print}' <<<"$metadata")" -ge "$epoch" ]]
  [[ "$(awk '$0=="num-other-sentinels" {getline; print}' <<<"$metadata")" -eq "$((target+2))" ]]
  [[ "$(cli "$pod" SENTINEL CKQUORUM upgrade)" == OK* ]]
  echo "PASS: $pod rediscovered $((target+3)) members with identity and election epoch retained"
done
health "$((target+3))"
[[ "$(data_identity)" == "$before" ]]
echo "PASS: one recovery voter retired; $target recovery voters remain; data/PVC identities and source configs retained"
