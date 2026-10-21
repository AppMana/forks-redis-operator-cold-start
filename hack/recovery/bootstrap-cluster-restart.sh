#!/usr/bin/env bash
# Reuse the CI restart topology in a fresh local-only namespace.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s)
namespace=redis-upgrade-canary-cluster-restart
[[ -z "$("${k[@]}" get namespace "$namespace" --ignore-not-found -o name)" ]]
"${k[@]}" create namespace "$namespace"
"${k[@]}" create --dry-run=client -f "$(dirname "$0")/../../tests/e2e-chainsaw/v1beta2/redis-cluster-restart/cluster.yaml" -o json |
  jq --arg ns "$namespace" '.metadata.namespace=$ns | .spec.kubernetesConfig.image="quay.io/opstree/redis:v7.0.15" | .spec.redisExporter.enabled=false' |
  "${k[@]}" create -f -
echo 'Created local three-leader persistent restart fixture; no existing data modified'
