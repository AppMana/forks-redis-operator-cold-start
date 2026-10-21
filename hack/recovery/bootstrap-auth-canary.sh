#!/usr/bin/env bash
# Create a fresh local-only authenticated fixture; never print credential data.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
namespace=redis-upgrade-canary-auth-rotation
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s)
[[ -z "$("${k[@]}" get namespace "$namespace" --ignore-not-found -o name)" ]]
"${k[@]}" create namespace "$namespace"
# Random test credentials go through stdin, never process arguments or stdout.
openssl rand -hex 24 | jq -Rs --arg ns "$namespace" '{apiVersion:"v1",kind:"Secret",metadata:{name:"redis-auth",namespace:$ns},stringData:{password:rtrimstr("\n")}}' | "${k[@]}" create -f -
openssl rand -hex 24 | jq -Rs --arg ns "$namespace" '{apiVersion:"v1",kind:"Secret",metadata:{name:"sentinel-auth",namespace:$ns},stringData:{password:rtrimstr("\n")}}' | "${k[@]}" create -f -
"${k[@]}" create --dry-run=client -f "$(dirname "$0")/canary-embedded-persistent.yaml" -o json |
  jq --arg ns "$namespace" 'select(.kind=="RedisReplication")|.metadata.namespace=$ns|.spec.kubernetesConfig.redisSecret={name:"redis-auth",key:"password"}|.spec.sentinel.redisSecret={name:"sentinel-auth",key:"password"}' |
  "${k[@]}" create -f -
echo 'Created authenticated canary; credential values were not printed'
