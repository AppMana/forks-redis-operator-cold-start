#!/usr/bin/env bash
# Reconcile a replication group whose replaced Redis pods are empty masters and
# whose replaced Sentinels monitor the placeholder address, against disposable
# redis-server and redis-sentinel processes; no host ports, existing data, or
# cluster access.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
testdir="$(mktemp -d /tmp/redis-placeholder-recovery-test.XXXXXX)"
cd "$root"
CGO_ENABLED=0 go test -buildvcs=false -c -o "$testdir/redisreplication.test" ./internal/controller/redisreplication
docker run --rm --network none \
  --mount "type=bind,src=$testdir/redisreplication.test,dst=/redisreplication.test,readonly" \
  -e REDIS_SENTINEL_RECOVERY_TEST=1 \
  "${REDIS_RECOVERY_TEST_IMAGE:-redis:7.0.15}" \
  /redisreplication.test -test.run '^TestPlaceholderSentinelRecoveryIntegration$' -test.v
