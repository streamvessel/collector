#!/usr/bin/env bash
# Kill-mid-batch and sv-be-outage test: nothing the agent read may be lost.
#
#   1. start MiniStack, fake sv-be and the agent; start writing N lines
#   2. stop sv-be; the agent uploads to offline slots; start sv-be again
#   3. SIGKILL the agent mid-batch, start it again on the same state volume
#   4. stop sv-be again: the restarted agent must still have offline slots,
#      restored from file_storage (sv-be would not mint it new ones)
#   5. wait until every line 1..N is in the bucket (duplicates allowed)
set -euo pipefail
cd "$(dirname "$0")"

N=${N:-20000}
RATE=${RATE:-400}
DEADLINE=${DEADLINE:-180}
dc() { docker compose "$@"; }

cleanup() {
  if [[ ${KEEP:-} != 1 ]]; then dc --profile tools down -v --remove-orphans >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT
fail() {
  echo "FAIL: $*" >&2
  dc logs --no-color --tail=200 agent fakesvbe >&2 || true
  exit 1
}

dc --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
[[ ${SKIP_BUILD:-} == 1 ]] || dc build
dc up -d ministack fakesvbe agent
dc --profile tools run -d --name sv-e2e-gen --rm gen -n "$N" -rate "$RATE" -rotate 3000 >/dev/null

sleep 8
echo "--- stopping sv-be; the agent should use offline slots"
dc kill fakesvbe
sleep 12
dc start fakesvbe

sleep 4
echo "--- killing the agent mid-batch"
docker inspect sv-e2e-gen >/dev/null 2>&1 || fail "generator already finished: the kill would not be mid-batch"
dc kill -s SIGKILL agent
sleep 3
restarted=$(date -u +%Y-%m-%dT%H:%M:%SZ)
dc start agent

sleep 5
echo "--- stopping sv-be again; the restarted agent should use its saved slots"
dc kill fakesvbe
sleep 12
dc start fakesvbe

echo "--- waiting for the generator"
while docker inspect sv-e2e-gen >/dev/null 2>&1; do sleep 1; done

echo "--- waiting for every line to reach the bucket"
end=$((SECONDS + DEADLINE))
until dc --profile tools run --rm verify -n "$N" -want-lease; do
  (( SECONDS < end )) || fail "not every line arrived within ${DEADLINE}s"
  sleep 5
done

# Capture first: grep -q closing a pipe early would trip pipefail.
all=$(dc logs --no-color agent)
grep -q "uploading to an offline slot" <<<"$all" || fail "agent never used an offline slot"
after=$(dc logs --no-color --since "$restarted" agent)
grep -q "Restored offline upload slots" <<<"$after" || fail "restarted agent did not restore its slots"
grep -q "uploading to an offline slot" <<<"$after" || fail "restarted agent did not use a saved slot"
echo "PASS: all $N lines delivered across two sv-be outages and a SIGKILL in between"
