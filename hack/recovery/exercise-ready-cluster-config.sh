#!/usr/bin/env bash
# Dedicated local fixture only; no pod/PVC deletion or credential changes.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary-cluster-dynamic)
"${k[@]}" wait rediscluster/dynamic --for=jsonpath='{.status.state}'=Ready --timeout=180s
uids() { "${k[@]}" get pod dynamic-leader-0 dynamic-follower-0 -o json | jq -Sc '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)'; }
before="$(uids)"
cli() { local role="$1"; shift; "${k[@]}" exec "dynamic-$role-0" -c "dynamic-$role" -- redis-cli --raw "$@"; }
expected=preserved-cluster-dynamic-20261021
cli leader SET appmana:dynamic:retained "$expected" NX >/dev/null
[[ "$(cli leader GET appmana:dynamic:retained)" == "$expected" ]]
for desired in 6000 5000; do
  [[ "$("${k[@]}" get rediscluster dynamic -o jsonpath='{.status.state}')" == Ready ]]
  jq -n --arg value "slowlog-log-slower-than $desired" '{spec:{redisConfig:{dynamicConfig:[$value]}}}' |
    "${k[@]}" patch rediscluster dynamic --type=merge --patch-file=/dev/stdin
  deadline=$((SECONDS+60)); converged=false
  while (( SECONDS < deadline )); do
    converged=true
    for role in leader follower; do
      value="$(cli "$role" CONFIG GET slowlog-log-slower-than | tail -n1)"
      [[ "$value" == "$desired" ]] || converged=false
    done
    [[ "$converged" == true ]] && break
    sleep 2
  done
  [[ "$converged" == true ]]
  [[ "$(uids)" == "$before" ]]
  [[ "$(cli leader GET appmana:dynamic:retained)" == "$expected" ]]
  [[ "$(cli follower INFO replication | tr -d '\r')" == *$'master_link_status:up\n'* ]]
  echo "PASS: Ready cluster converged to $desired on leader and follower without pod replacement"
done
