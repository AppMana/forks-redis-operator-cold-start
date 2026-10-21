#!/usr/bin/env bash
# Exercise the actual service against a disposable Sentinel; no host ports,
# existing data, or production cluster access.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
testdir="$(mktemp -d /tmp/redis-quorum-test.XXXXXX)"
cd "$root"
CGO_ENABLED=0 go test -buildvcs=false -c -o "$testdir/redis.test" ./internal/service/redis
docker run --rm --network none \
  --mount "type=bind,src=$testdir/redis.test,dst=/redis.test,readonly" \
  -e REDIS_SENTINEL_QUORUM_TEST=1 \
  "${REDIS_QUORUM_TEST_IMAGE:-redis:7.0.15}" \
  /redis.test -test.run '^TestSentinelQuorumIntegration$' -test.v
