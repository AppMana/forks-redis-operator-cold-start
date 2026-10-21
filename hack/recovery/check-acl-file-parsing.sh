#!/usr/bin/env bash
# Only synthetic ACL fixtures are mounted, read-only. No ports or data mounts.
set -euo pipefail
fixtures="$(cd "$(dirname "$0")/fixtures" && pwd)"
docker run --rm --network none -i --mount "type=bind,src=$fixtures,dst=/fixtures,readonly" \
  "${REDIS_ACL_TEST_IMAGE:-redis:7.0.15}" sh -eu <<'REDIS_TEST'
redis-server --version
redis-server --port 0 --unixsocket /tmp/acl-literal.sock --save '' --appendonly no \
  --aclfile /fixtures/acl-literal-quotes.acl >/dev/null 2>&1 &
server=$!
ready=false
for attempt in $(seq 1 50); do
  if [ -S /tmp/acl-literal.sock ]; then ready=true; break; fi
  kill -0 "$server"
  sleep 0.1
done
[ "$ready" = true ]
for password in "sec'ret" 'sec"ret'; do
  result="$(REDISCLI_AUTH="$password" redis-cli --raw -s /tmp/acl-literal.sock PING 2>/dev/null)"
  [ "$result" = PONG ]
done
result="$(REDISCLI_AUTH=wrong redis-cli --raw -s /tmp/acl-literal.sock PING 2>/dev/null)"
[ "$result" != PONG ]
# Quotes do not group ACL-file arguments; this file must abort startup.
status=0
timeout 3 redis-server --port 0 --unixsocket /tmp/acl-invalid.sock --save '' --appendonly no \
  --aclfile /fixtures/acl-shell-quoted.acl >/dev/null 2>&1 || status=$?
[ "$status" = 1 ] || { echo 'FAIL: expected immediate ACL startup rejection'; exit 1; }
echo 'PASS: ACL-file quotes are literal; shell-style quoting cannot bypass validation'
REDIS_TEST
