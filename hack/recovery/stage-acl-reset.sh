#!/usr/bin/env bash
# Exercise ordered ACL bootstrap without changing either retained credential.
set -euo pipefail
config="${1:?local kubeconfig required}"
context="$(kubectl --kubeconfig "$config" config current-context)"
[[ "$context" == kind-redis-recovery-* ]]
[[ "$(kubectl --kubeconfig "$config" config view --minify -o jsonpath='{.clusters[0].cluster.server}')" == https://127.0.0.1:* ]]
k=(kubectl --kubeconfig "$config" --context "$context" --request-timeout=10s -n redis-upgrade-canary-auth-rotation)
for sts in upgrade upgrade-s; do
  [[ "$("${k[@]}" get sts "$sts" -o jsonpath='{.spec.updateStrategy.type}')" == OnDelete ]]
  [[ "$("${k[@]}" get sts "$sts" -o jsonpath='{.spec.template.spec.initContainers[0].image}')" == appmana-redis-recovery:acl-validation1 ]]
done
# Patch only the rule prefix. Optimistic concurrency prevents overwriting a
# concurrent Secret update; neither credentials nor hashes reach stdout.
"${k[@]}" get secret rotation-acl -o json | jq -ce '
  (.data["user.acl"] | @base64d) as $acl |
  if ($acl | startswith("user default reset on ")) then
    [{op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion}]
  elif ($acl | startswith("user default on ")) then
    [{op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion},
     {op:"replace",path:"/data/user.acl",value:($acl | sub("^user default on ";"user default reset on ") | @base64)}]
  else error("unexpected ACL layout; refusing transformation") end' |
  "${k[@]}" patch secret rotation-acl --type=json --patch-file=/dev/stdin
echo 'PASS: ordered reset staged; all password hashes and other Secret keys retained'
