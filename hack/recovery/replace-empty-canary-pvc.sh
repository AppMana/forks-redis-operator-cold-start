#!/usr/bin/env bash
# Isolated storage-loss rehearsal. Retain the old PV/data and archive metadata;
# replace only one synthetic replica's claim with a newly provisioned claim.
set -euo pipefail
config="${1:?qualified local kubeconfig required}"
namespace=redis-upgrade-canary-empty-disk
[[ "$(kubectl --kubeconfig "$config" config current-context)" == kind-redis-recovery-qualified-20261021 ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --request-timeout=10s -n "$namespace")
operator=(kubectl --kubeconfig "$config" --request-timeout=10s -n redis-operator)
dir="$(cd "$(dirname "$0")" && pwd)"
export CANARY_NAMESPACE="$namespace"
unset CANARY_SENTINEL_COUNT CANARY_RECOVERY_SENTINEL_SELECTOR
health() { bash "$dir/check.sh" "$config" preserved-empty-disk-20261021; }
health
primary="$("${k[@]}" get redisreplication upgrade -o jsonpath='{.status.masterNode}')"
victim=upgrade-0
[[ "$primary" != "$victim" ]] || victim=upgrade-1
[[ "$("${operator[@]}" get deployment redis-operator -o jsonpath='{.spec.replicas}')" == 1 ]]
pod="$("${k[@]}" get pod "$victim" -o json)"
claim="$(jq -r '.spec.volumes[]|select(.name=="upgrade")|.persistentVolumeClaim.claimName' <<<"$pod")"
[[ "$claim" == "upgrade-$victim" ]]
old_uid="$("${k[@]}" get pvc "$claim" -o jsonpath='{.metadata.uid}')"
old_pv="$("${k[@]}" get pvc "$claim" -o jsonpath='{.spec.volumeName}')"
[[ -n "$old_uid" && "$old_pv" == pvc-* ]]
other_claims="$("${k[@]}" get pvc -o json | jq -Sc --arg name "$claim" '[.items[]|select(.metadata.name!=$name)|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')"
# create (not apply) prevents overwriting evidence from an earlier attempt.
for resource in statefulset/upgrade "pvc/$claim" "pv/$old_pv"; do
  case "$resource" in statefulset/*) archive=empty-disk-original-sts;; pvc/*) archive=empty-disk-original-pvc;; pv/*) archive=empty-disk-original-pv;; esac
  "${k[@]}" get "$resource" -o json | "${k[@]}" create configmap "$archive" --from-file=object.json=/dev/stdin
done
"${k[@]}" patch pv "$old_pv" --type=merge -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
[[ "$("${k[@]}" get pv "$old_pv" -o jsonpath='{.spec.persistentVolumeReclaimPolicy}')" == Retain ]]
restore_operator() { "${operator[@]}" scale deployment redis-operator --replicas=1 >/dev/null; }
trap restore_operator EXIT
trap 'exit 130' INT TERM
"${operator[@]}" scale deployment redis-operator --replicas=0
"${operator[@]}" wait pods -l name=redis-operator --for=delete --timeout=60s
"${k[@]}" delete statefulset upgrade --cascade=orphan --wait=true --timeout=45s
"${k[@]}" delete pod "$victim" --wait=true --timeout=45s
# Only disposable claim metadata is removed. The original data volume is Retain.
"${k[@]}" delete pvc "$claim" --wait=true --timeout=45s
"${k[@]}" get configmap empty-disk-original-pvc -o json | jq '.data["object.json"]|fromjson|del(.status,.metadata.uid,.metadata.resourceVersion,.metadata.creationTimestamp,.metadata.managedFields,.metadata.finalizers,.metadata.ownerReferences,.metadata.annotations,.spec.volumeName)' | "${k[@]}" create -f -
"${k[@]}" get configmap empty-disk-original-sts -o json | jq '.data["object.json"]|fromjson|del(.status,.metadata.uid,.metadata.resourceVersion,.metadata.creationTimestamp,.metadata.managedFields)' | "${k[@]}" create -f -
restore_operator
trap - EXIT
"${operator[@]}" rollout status deployment/redis-operator --timeout=120s
deadline=$((SECONDS+180)); recovered=false
while ((SECONDS<deadline)); do
  if health >/dev/null 2>&1; then recovered=true; break; fi
  sleep 2
done
[[ "$recovered" == true ]]
[[ "$("${k[@]}" get pvc "$claim" -o jsonpath='{.metadata.uid}')" != "$old_uid" ]]
[[ "$("${k[@]}" get pvc "$claim" -o jsonpath='{.spec.volumeName}')" != "$old_pv" ]]
[[ "$("${k[@]}" get pv "$old_pv" -o jsonpath='{.spec.persistentVolumeReclaimPolicy}')" == Retain ]]
[[ "$("${k[@]}" get pv "$old_pv" -o jsonpath='{.status.phase}')" == Released ]]
[[ "$("${k[@]}" get pvc -o json | jq -Sc --arg name "$claim" '[.items[]|select(.metadata.name!=$name)|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)')" == "$other_claims" ]]
health
echo "PASS: $victim recovered onto a new PVC/PV; original volume $old_pv and archived claim metadata retained"
