#!/usr/bin/env bash
# Isolated Redis semantics check: synthetic credentials, no ports or host mounts.
set -euo pipefail
docker run --rm --network none -i "${REDIS_ACL_TEST_IMAGE:-redis:7.0.15}" sh -eu <<'REDIS_TEST'
redis-server --version
redis-server --port 0 --unixsocket /tmp/acl-order.sock --save '' --appendonly no --daemonize yes
cli() { redis-cli --raw -s /tmp/acl-order.sock "$@"; }
check() {
  expected="$1"; shift
  result="$(cli ACL SETUSER rotation-test reset "$@")"
  [ "$result" = OK ] || { echo 'FAIL: ACL rule rejected'; exit 1; }
  # Failed AUTH leaves the connection on the open default user. Check
  # identity rather than inferring authentication from a successful PING.
  result="$(REDISCLI_AUTH=secret redis-cli --raw -s /tmp/acl-order.sock --user rotation-test ACL WHOAMI 2>/dev/null)"
  if [ "$expected" = accept ]; then
    [ "$result" = rotation-test ] || { echo 'FAIL: expected credential accepted'; exit 1; }
  else
    [ "$result" != rotation-test ] || { echo 'FAIL: expected credential rejected'; exit 1; }
  fi
}
check accept reset on '>secret' '~*' '&*' '+@all'
check accept on '>old' resetpass '>secret' '+@all'
check accept on '>old' '>secret' '<old' '+@all'
check accept on '>secret' '<secret' '>secret' '+@all'
check accept on '>old' '>secret' '!cba06b5736faf67e54b07b561eae94395e774c517a7d910a54369e1263ccfbd4' '+@all'
check accept off on '>secret' '+@all'
check accept RESET ON '>secret' '+@all'
check accept on nopass '>secret' '+@all'
check reject on '>secret' off '+@all'
check reject on '>secret' resetpass '+@all'
check reject on '>secret' '!2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b' '+@all'
[ "$(cli ACL SETUSER rotation-test reset on '>secret' '<absent')" != OK ]
echo 'PASS: ordered ACL resets, password retirement, enable/disable and hash equivalence'
REDIS_TEST
