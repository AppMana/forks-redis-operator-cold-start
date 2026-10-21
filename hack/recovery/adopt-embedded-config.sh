#!/usr/bin/env bash
# Local rehearsal only. Retains every PVC and seed copy; leaves OnDelete enabled.
set -euo pipefail
config="${1:?usage: adopt-embedded-config.sh LOCAL_KUBECONFIG EXPECTED_VALUE}"
expected="${2:?expected test value required}"
namespace="${CANARY_NAMESPACE:-redis-upgrade-canary-embedded-migration}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* && "$namespace" == redis-upgrade-canary-embedded-* ]]
server="$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$namespace")
check() { CANARY_NAMESPACE="$namespace" CANARY_EMBEDDED_SENTINEL=true bash "$(dirname "$0")/check.sh" "$config" "$expected"; }
data_pods() { "${k[@]}" get pod upgrade-0 upgrade-1 upgrade-2 -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'; }
claims() { "${k[@]}" get pvc upgrade-upgrade-0 upgrade-upgrade-1 upgrade-upgrade-2 config-upgrade-s-0 config-upgrade-s-1 config-upgrade-s-2 -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid,volume:.spec.volumeName}]|sort_by(.name)'; }
election() {
  "${k[@]}" exec "$1" -c "$2" -- awk '$1=="sentinel" && ($2=="myid" || $2=="monitor" || $2=="current-epoch" || $2=="config-epoch" || $2=="leader-epoch")' "$3" | LC_ALL=C sort
}
check
original_data="$(data_pods)"
original_claims="$(claims)"
for sts in upgrade upgrade-s; do
  [[ "$("${k[@]}" get sts "$sts" -o jsonpath='{.spec.updateStrategy.type}')" == OnDelete ]]
done
[[ "$("${k[@]}" get redisreplication upgrade -o json | jq '.spec.sentinel.configVolumeClaimTemplate == null')" == true ]]
declare -a elections uids
for ordinal in 0 1 2; do
  pod="upgrade-s-$ordinal"
  elections[$ordinal]="$(election "$pod" sentinel /etc/redis/sentinel.conf)"
  [[ -n "${elections[$ordinal]}" ]]
  [[ "${elections[$ordinal]}" == "$(election "seed-$pod" seed /seed/sentinel.conf)" ]]
  uids[$ordinal]="$("${k[@]}" get pod "$pod" -o jsonpath='{.metadata.uid}')"
done
# The controller deliberately rejects this immutable storage change in place.
# Set desired state first, then orphan only the old controller, retaining pods.
"${k[@]}" patch redisreplication upgrade --type=merge -p '{"spec":{"sentinel":{"configVolumeClaimTemplate":{"spec":{"storageClassName":"standard","accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"128Mi"}}}}}}}'
"${k[@]}" delete sts upgrade-s --cascade=orphan --wait=true --timeout=30s
adopted=false
for attempt in {1..60}; do
  sts="$("${k[@]}" get sts upgrade-s --ignore-not-found -o json)"
  if [[ -n "$sts" ]] && jq -e '.spec.updateStrategy.type == "OnDelete" and any(.spec.volumeClaimTemplates[]?; .metadata.name == "config")' <<<"$sts" >/dev/null; then
    owner="$(jq -r '.metadata.uid' <<<"$sts")"
    if "${k[@]}" get pod upgrade-s-0 upgrade-s-1 upgrade-s-2 -o json | jq -e --arg owner "$owner" 'all(.items[]; any(.metadata.ownerReferences[]?; .uid == $owner))' >/dev/null; then adopted=true; break; fi
  fi
  sleep 2
done
[[ "$adopted" == true ]]
for ordinal in 0 1 2; do
  [[ "${uids[$ordinal]}" == "$("${k[@]}" get pod "upgrade-s-$ordinal" -o jsonpath='{.metadata.uid}')" ]]
done
for ordinal in 0 1 2; do
  pod="upgrade-s-$ordinal"
  check
  # Refuse a stale copy if an election occurred since the snapshot.
  [[ "${elections[$ordinal]}" == "$(election "$pod" sentinel /etc/redis/sentinel.conf)" ]]
  [[ "$original_data" == "$(data_pods)" && "$original_claims" == "$(claims)" ]]
  "${k[@]}" delete pod "$pod" --wait=true --timeout=30s
  ready=false
  for attempt in {1..90}; do
    state="$("${k[@]}" get pod "$pod" --ignore-not-found -o json)"
    if [[ -n "$state" ]] && jq -e --arg old "${uids[$ordinal]}" '.metadata.uid != $old and any(.status.conditions[]?; .type == "Ready" and .status == "True")' <<<"$state" >/dev/null; then ready=true; break; fi
    sleep 2
  done
  [[ "$ready" == true ]]
  jq -e --arg claim "config-$pod" 'any(.spec.volumes[]?; .name == "config" and .persistentVolumeClaim.claimName == $claim) and any(.spec.initContainers[]?; .name == "init-config")' <<<"$state" >/dev/null
  [[ "${elections[$ordinal]}" == "$(election "$pod" sentinel /etc/redis/sentinel.conf)" ]]
  healthy=false
  for attempt in {1..30}; do
    if check; then healthy=true; break; fi
    sleep 2
  done
  [[ "$healthy" == true ]]
  [[ "$original_data" == "$(data_pods)" && "$original_claims" == "$(claims)" ]]
  echo "PASS: $pod migrated with election identity intact; data pods and all PVCs retained"
done
echo 'PASS: embedded config adoption complete; both StatefulSets remain OnDelete'
