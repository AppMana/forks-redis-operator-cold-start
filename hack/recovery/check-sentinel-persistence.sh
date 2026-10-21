#!/usr/bin/env bash
# Disposable Redis only; no host ports or existing cluster/PVC access.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
testdir="$(mktemp -d /tmp/redis-sentinel-persistence-test.XXXXXX)"
cd "$root"
CGO_ENABLED=0 go test -buildvcs=false -c -o "$testdir/sentinel.test" ./internal/agent/bootstrap/sentinel
docker run --rm --network none \
  --mount "type=bind,src=$testdir/sentinel.test,dst=/sentinel.test,readonly" \
  -e REDIS_SENTINEL_PERSISTENCE_TEST=1 \
  redis:7.0.15 \
  /sentinel.test -test.run '^TestSentinelRetainedReplicaRewriteIntegration$' -test.v
