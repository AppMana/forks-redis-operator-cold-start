#!/usr/bin/env bash
# End-to-end CLI checks in one disposable, network-isolated Redis container.
# Only the test binary is mounted, read-only; no existing Redis data is used.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
testdir="$(mktemp -d /tmp/redis-data-assert-test.XXXXXX)"
cd "$root/tests/data-assert"
CGO_ENABLED=0 go build -buildvcs=false -o "$testdir/data-assert" .
timeout 60s docker run --rm --network none -i \
  --mount "type=bind,src=$testdir/data-assert,dst=/data-assert,readonly" \
  redis:7.0.15 sh -eu <<'REDIS_TEST'
redis-server --cluster-enabled yes --cluster-config-file /tmp/nodes.conf \
  --cluster-announce-ip 127.0.0.1 --save '' --appendonly no >/dev/null 2>&1 &
ready=false
for attempt in $(seq 1 100); do
  if [ "$(redis-cli --raw PING 2>/dev/null || true)" = PONG ]; then ready=true; break; fi
  sleep 0.1
done
[ "$ready" = true ]
[ "$(redis-cli --raw CLUSTER ADDSLOTSRANGE 0 16383)" = OK ]
ready=false
for attempt in $(seq 1 100); do
  if redis-cli --raw CLUSTER INFO | tr -d '\r' | grep -qx cluster_state:ok; then ready=true; break; fi
  sleep 0.1
done
[ "$ready" = true ]
run() { /data-assert "$1" --mode cluster --host 127.0.0.1:6379; }
run gen-redis-data
run chk-redis-data
[ "$(redis-cli --raw SET key-5 private-corrupted-value)" = OK ]
if output="$(run chk-redis-data 2>&1)"; then echo 'FAIL: corrupted value accepted'; exit 1; fi
case "$output" in *'data value mismatch for key key-5'*) ;; *) echo 'FAIL: missing mismatch diagnostic'; exit 1;; esac
case "$output" in *private-corrupted-value*|*'[OK]'*) echo 'FAIL: corrupt check exposed data or success marker'; exit 1;; esac
[ "$(redis-cli --raw SET key-5 value-5)" = OK ]
run chk-redis-data
[ "$(redis-cli --raw CONFIG SET requirepass synthetic-required-password)" = OK ]
if output="$(/data-assert gen-redis-data --mode cluster --host 127.0.0.1:6379 --password synthetic-wrong-password 2>&1)"; then
  echo 'FAIL: failed authentication reported generation success'; exit 1
fi
case "$output" in *WRONGPASS*) ;; *) echo 'FAIL: expected authentication rejection'; exit 1;; esac
case "$output" in *synthetic-wrong-password*|*'[OK]'*) echo 'FAIL: failed generation exposed credentials or success marker'; exit 1;; esac
echo 'PASS: real Redis data generation, value checking, corruption rejection and authentication failure exit status'
REDIS_TEST
