#!/usr/bin/env bash
# A fresh local-only fixture; never reuse or overwrite retained recovery data.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s)
namespace=redis-upgrade-canary-cluster-downscale
followers=0
case "${2:-baseline}" in
  baseline) ;;
  interrupted) namespace=redis-upgrade-canary-cluster-downscale-interrupted-routable ;;
  followers) namespace=redis-upgrade-canary-cluster-follower-downscale; followers=3 ;;
  *) echo 'Expected baseline, interrupted or followers'; exit 1 ;;
esac
[[ -z "$("${k[@]}" get namespace "$namespace" --ignore-not-found -o name)" ]]
"${k[@]}" create namespace "$namespace"
"${k[@]}" create --dry-run=client -f "$(dirname "$0")/../../tests/e2e-chainsaw/v1beta2/redis-cluster-restart/cluster.yaml" -o json |
  jq --arg ns "$namespace" --argjson followers "$followers" '.metadata.namespace=$ns | .spec.redisFollower.replicas=$followers | .spec.kubernetesConfig.image="quay.io/opstree/redis:v7.0.15" | .spec.redisExporter.enabled=false' |
  "${k[@]}" create -f -
echo 'Created fresh three-leader downscale fixture; no existing data modified'
