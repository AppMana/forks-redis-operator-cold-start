#!/usr/bin/env bash
# Fresh, isolated six-node TLS fixture. Retains all generated credentials/PVCs.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s)
namespace=redis-upgrade-canary-cluster-tls-dynamic
[[ -z "$("${k[@]}" get namespace "$namespace" --ignore-not-found -o name)" ]]
umask 077
certdir="$(mktemp -d /tmp/redis-cluster-tls-dynamic.XXXXXX)"
openssl req -x509 -newkey rsa:2048 -nodes -days 7 \
  -keyout "$certdir/tls.key" -out "$certdir/tls.crt" \
  -subj /CN=redis-cluster-v1beta2 \
  -addext "subjectAltName=DNS:*.redis-cluster-v1beta2-leader-headless.$namespace.svc.cluster.local,DNS:*.redis-cluster-v1beta2-follower-headless.$namespace.svc.cluster.local" \
  >/dev/null 2>&1
"${k[@]}" create namespace "$namespace"
"${k[@]}" -n "$namespace" create secret generic redis-tls-cert \
  --from-file="ca.crt=$certdir/tls.crt" --from-file="tls.crt=$certdir/tls.crt" --from-file="tls.key=$certdir/tls.key"
openssl rand -hex 24 | jq -Rs --arg ns "$namespace" \
  '{apiVersion:"v1",kind:"Secret",metadata:{name:"redis-secret",namespace:$ns},stringData:{password:rtrimstr("\n")}}' |
  "${k[@]}" create -f -
"${k[@]}" apply -f "$(dirname "$0")/cluster-tls-dynamic-canary.yaml"
echo 'Created six-node authenticated TLS cluster fixture; credentials were not printed'
