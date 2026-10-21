#!/usr/bin/env bash
# Rehearsal only: copy live election state into new PVCs without replacing pods.
# Leaves source pods, seed pods and every PVC intact on success or failure.
set -euo pipefail
config="${1:?usage: seed-sentinel-config.sh LOCAL_KUBECONFIG}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]] || exit 2
server="$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://127.0.0.1:* ]] || exit 2
namespace="${CANARY_NAMESPACE:-redis-upgrade-canary-migration}"
[[ "$namespace" == redis-upgrade-canary* ]] || exit 2
sentinel_prefix=upgrade-sentinel
sentinel_container=upgrade-sentinel
master_group=upgrade
if [[ "${CANARY_EMBEDDED_SENTINEL:-false}" == true ]]; then
  sentinel_prefix=upgrade-s
  sentinel_container=sentinel
  master_group=mymaster
fi
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n "$namespace")
source_pods=(upgrade-0 upgrade-1 upgrade-2 "$sentinel_prefix-0" "$sentinel_prefix-1" "$sentinel_prefix-2")
pod_uids() { "${k[@]}" get pod "${source_pods[@]}" -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'; }
data_uids() { "${k[@]}" get pvc upgrade-upgrade-0 upgrade-upgrade-1 upgrade-upgrade-2 -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'; }
original_pods="$(pod_uids)"
original_data="$(data_uids)"
for name in upgrade "$sentinel_prefix"; do
  [[ "$("${k[@]}" get sts "$name" -o jsonpath='{.spec.updateStrategy.type}')" == OnDelete ]]
done
if [[ "${CANARY_EMBEDDED_SENTINEL:-false}" == true ]]; then
  [[ "$("${k[@]}" get redisreplication upgrade -o json | jq '.spec.sentinel.configVolumeClaimTemplate == null')" == true ]]
else
  [[ "$("${k[@]}" get redissentinel upgrade -o json | jq '.spec.configVolumeClaimTemplate == null')" == true ]]
fi
for ordinal in 0 1 2; do
  for resource in "pvc/config-$sentinel_prefix-$ordinal" "pod/seed-$sentinel_prefix-$ordinal"; do
    owner="$("${k[@]}" get "$resource" --ignore-not-found -o json | jq -r '.metadata.labels["appmana-test"] // ""')"
    [[ -z "$owner" || "$owner" == sentinel-config-seed ]] || exit 2
    if [[ -z "$owner" && -n "$("${k[@]}" get "$resource" --ignore-not-found -o name)" ]]; then
      echo "Refusing unowned existing seed resource" >&2; exit 2
    fi
  done
done
for ordinal in 0 1 2; do
  source="$sentinel_prefix-$ordinal"
  claim="config-$source"
  seed="seed-$source"
  node="$("${k[@]}" get pod "$source" -o jsonpath='{.spec.nodeName}')"
  [[ "$node" == "${context#kind-}-control-plane" ]] || exit 2
  [[ "$("${k[@]}" exec "$source" -c "$sentinel_container" -- redis-cli --raw -p 26379 SENTINEL CKQUORUM "$master_group")" == OK* ]]
  jq -n --arg ns "$namespace" --arg claim "$claim" --arg seed "$seed" --arg node "$node" '{
    apiVersion:"v1",kind:"List",items:[
      {apiVersion:"v1",kind:"PersistentVolumeClaim",metadata:{name:$claim,namespace:$ns,labels:{"appmana-test":"sentinel-config-seed"}},
       spec:{storageClassName:"standard",accessModes:["ReadWriteOnce"],resources:{requests:{storage:"128Mi"}}}},
      {apiVersion:"v1",kind:"Pod",metadata:{name:$seed,namespace:$ns,labels:{"appmana-test":"sentinel-config-seed"}},
       spec:{restartPolicy:"Never",nodeSelector:{"kubernetes.io/hostname":$node},securityContext:{runAsUser:1000,fsGroup:1000},
        containers:[{name:"seed",image:"quay.io/opstree/redis:v7.0.15",imagePullPolicy:"IfNotPresent",
          command:["sh","-c","sleep 3600"],resources:{requests:{cpu:"10m",memory:"16Mi"},limits:{memory:"64Mi"}},
          volumeMounts:[{name:"config",mountPath:"/seed"}]}],volumes:[{name:"config",persistentVolumeClaim:{claimName:$claim}}]}}
    ]}' | "${k[@]}" apply -f -
  "${k[@]}" wait pod "$seed" --for=condition=Ready --timeout=120s
  "${k[@]}" exec "$seed" -- test ! -e /seed/sentinel.conf
  stable=false
  for attempt in {1..10}; do
    before="$("${k[@]}" exec "$source" -c "$sentinel_container" -- sha256sum /etc/redis/sentinel.conf | awk '{print $1}')"
    [[ "$before" =~ ^[a-f0-9]{64}$ ]]
    "${k[@]}" exec "$source" -c "$sentinel_container" -- cat /etc/redis/sentinel.conf |
      "${k[@]}" exec -i "$seed" -- sh -ec 'umask 077; test ! -e /seed/sentinel.conf; cat > /seed/sentinel.conf.pending'
    copied="$("${k[@]}" exec "$seed" -- sha256sum /seed/sentinel.conf.pending | awk '{print $1}')"
    after="$("${k[@]}" exec "$source" -c "$sentinel_container" -- sha256sum /etc/redis/sentinel.conf | awk '{print $1}')"
    if [[ "$before" == "$copied" && "$before" == "$after" ]]; then stable=true; break; fi
    echo "Source rewrote config during copy; retry $attempt/10"
    sleep 1
  done
  [[ "$stable" == true ]] || {
    echo "Election state changed while copying; source retained, do not migrate" >&2; exit 1;
  }
  "${k[@]}" exec "$seed" -- sh -ec 'test ! -e /seed/sentinel.conf; mv /seed/sentinel.conf.pending /seed/sentinel.conf; sync'
  echo "PASS: $source config copied unchanged into $claim; original pod retained"
done
[[ "$original_pods" == "$(pod_uids)" ]]
[[ "$original_data" == "$(data_uids)" ]]
echo 'PASS: all source pod UIDs and data PVC UIDs unchanged'
