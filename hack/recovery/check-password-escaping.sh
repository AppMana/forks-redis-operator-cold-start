#!/usr/bin/env bash
# Build ./operator first. Runs actual generated configs with synthetic secrets
# inside one network-isolated container; no ports or writable host mounts.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
[[ -x "$root/operator" ]]
docker run --rm --network none -i --mount "type=bind,src=$root/operator,dst=/operator,readonly" \
  "${REDIS_PASSWORD_TEST_IMAGE:-redis:7.0.15}" sh -eu <<'REDIS_TEST'
redis-server --version
data_password=' space"quote\slash '
sentinel_password=" own'quote\\slash "
REDIS_PASSWORD="$data_password" REDIS_CONFIG_FILE=/tmp/redis.conf SETUP_MODE=standalone \
  /operator agent bootstrap >/dev/null
redis-server /tmp/redis.conf --save '' --appendonly no >/dev/null 2>&1 &
redis-server /tmp/redis.conf --port 6380 --replicaof 127.0.0.1 6379 --save '' --appendonly no >/dev/null 2>&1 &
REDIS_PASSWORD="$sentinel_password" MASTER_PASSWORD="$data_password" IP=127.0.0.1 \
  SENTINEL_CONFIG_FILE=/tmp/sentinel.conf /operator agent bootstrap --sentinel >/dev/null
redis-server /tmp/sentinel.conf --sentinel >/dev/null 2>&1 &
ready=false
for attempt in $(seq 1 100); do
  data="$(REDISCLI_AUTH="$data_password" redis-cli --raw PING 2>/dev/null || true)"
  replica="$(REDISCLI_AUTH="$data_password" redis-cli --raw -p 6380 INFO replication 2>/dev/null | tr -d '\r' || true)"
  discovered="$(REDISCLI_AUTH="$sentinel_password" redis-cli --raw -p 26379 SENTINEL MASTER mymaster 2>/dev/null | awk '$0=="num-slaves" {getline;print}' || true)"
  if [ "$data" = PONG ] && [ "$discovered" = 1 ] && echo "$replica" | grep -qx 'master_link_status:up'; then
    ready=true; break
  fi
  sleep 0.2
done
[ "$ready" = true ] || { echo 'FAIL: escaped credential configuration did not converge'; exit 1; }
wrong="$(REDISCLI_AUTH=wrong redis-cli --raw PING 2>/dev/null)"
[ "$wrong" != PONG ]
echo 'PASS: generated Redis/Sentinel configs authenticate and replicate with spaces, quotes and backslashes'
REDIS_TEST
