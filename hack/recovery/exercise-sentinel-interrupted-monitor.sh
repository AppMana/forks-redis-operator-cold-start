#!/usr/bin/env bash
# Stop only the local operator, remove one test monitor, then start a fresh
# operator process. No Redis/Sentinel pod or PVC is deleted or scaled.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s)
namespace=redis-upgrade-canary-auth-syntax
root="$(cd "$(dirname "$0")/../.." && pwd)"
health() {
  CANARY_AUTH_NAMESPACE="$namespace" CANARY_AUTH_VALUE=preserved-auth-syntax-20261021 \
    bash "$root/hack/recovery/check-auth-rotation.sh" "$config"
}
cli() {
  local pod="$1"; shift
  "${k[@]}" -n "$namespace" get secret sentinel-auth -o json | jq -r '.data.password|@base64d' |
    "${k[@]}" -n "$namespace" exec -i "$pod" -c sentinel -- sh -ec \
      'IFS= read -r REDISCLI_AUTH; export REDISCLI_AUTH; exec redis-cli --raw -p 26379 "$@"' sh "$@" 2>/dev/null
}
identity() {
  "${k[@]}" -n "$namespace" get pod upgrade-0 upgrade-1 upgrade-2 upgrade-s-0 upgrade-s-1 upgrade-s-2 -o json |
    jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'
  "${k[@]}" -n "$namespace" get pvc -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'
  for i in 0 1 2; do cli "upgrade-s-$i" SENTINEL MYID; done
}
[[ "$("${k[@]}" -n redis-operator get deployment redis-operator -o jsonpath='{.spec.replicas}')" == 1 ]]
watch="$("${k[@]}" -n redis-operator get deployment redis-operator -o json | jq -r '.spec.template.spec.containers[].env[]?|select(.name=="WATCH_NAMESPACE")|.value')"
[[ "$watch" == "$namespace" ]]
health
before="$(identity)"
old_holder="$("${k[@]}" -n redis-operator get lease 6cab913b.redis.opstreelabs.in -o jsonpath='{.spec.holderIdentity}')"
old_pod="${old_holder%%_*}"
[[ "$old_pod" == redis-operator-* ]]
stopped=false
restore() {
  if [[ "$stopped" == true ]]; then "${k[@]}" -n redis-operator scale deployment redis-operator --replicas=1 >/dev/null; fi
}
trap restore EXIT
stopped=true
"${k[@]}" -n redis-operator scale deployment redis-operator --replicas=0 >/dev/null
"${k[@]}" -n redis-operator wait "pod/$old_pod" --for=delete --timeout=60s
[[ "$(cli upgrade-s-1 SENTINEL REMOVE mymaster)" == OK ]]
[[ "$(cli upgrade-s-1 SENTINEL MASTER mymaster)" == *'No such master with that name'* ]]
for i in 0 2; do
  [[ "$(cli "upgrade-s-$i" SENTINEL MASTER mymaster | awk '$0=="name" {getline; print}')" == mymaster ]]
done
echo 'Injected interruption: operator stopped, middle monitor absent, other monitors retained'
"${k[@]}" -n redis-operator scale deployment redis-operator --replicas=1 >/dev/null
stopped=false
"${k[@]}" -n redis-operator rollout status deployment/redis-operator --timeout=90s
deadline=$((SECONDS+180)); recovered=false
while (( SECONDS < deadline )); do
  if [[ "$(cli upgrade-s-1 SENTINEL MASTER mymaster | awk '$0=="role-reported" {getline; print}')" == master ]] && health >/dev/null 2>&1; then
    recovered=true; break
  fi
  sleep 2
done
[[ "$recovered" == true ]] || { echo 'FAIL: monitor recovery did not converge; all fixture PVCs retained'; exit 1; }
new_holder="$("${k[@]}" -n redis-operator get lease 6cab913b.redis.opstreelabs.in -o jsonpath='{.spec.holderIdentity}')"
[[ -n "$new_holder" && "$new_holder" != "$old_holder" ]]
[[ "$(identity)" == "$before" ]]
health
echo 'PASS: fresh operator repaired interrupted monitor; data, all pod/PVC identities and Sentinel IDs retained'
