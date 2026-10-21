#!/usr/bin/env bash
# Fresh network-isolated Redis only; no production or retained data access.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
testdir="$(mktemp -d /tmp/redis-serving-test.XXXXXX)"
cd "$root"
CGO_ENABLED=0 go test -buildvcs=false -c -o "$testdir/k8sutils.test" ./internal/k8sutils
timeout 30s docker run --rm --network none \
  --mount "type=bind,src=$testdir/k8sutils.test,dst=/k8sutils.test,readonly" \
  -e REDIS_SERVING_TEST=1 redis:7.0.15 \
  /k8sutils.test -test.run '^TestClusterServingIntegration$' -test.v
