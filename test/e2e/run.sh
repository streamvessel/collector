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

down() { dc --profile tools down -v --remove-orphans >/dev/null 2>&1 || true; }
cleanup() { [[ ${KEEP:-} == 1 ]] || down; }
trap cleanup EXIT
fail() {
  echo "FAIL: $*" >&2
  dc logs --no-color --tail=200 agent fakesvbe >&2 || true
  exit 1
}
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
# wait_log SERVICE SINCE PATTERN: wait up to 60s for PATTERN in SERVICE's log.
# The log is captured first: grep -q closing a pipe early would trip pipefail.
wait_log() {
  local end=$((SECONDS + 60)) out
  until out=$(dc logs --no-color --since "$2" "$1") && grep -q "$3" <<<"$out"; do
    (( SECONDS < end )) || fail "no \"$3\" from $1 within 60s"
    sleep 1
  done
}
# outage: stop sv-be until the agent has uploaded to an offline slot.
outage() {
  local since; since=$(now)
  dc kill fakesvbe
  wait_log agent "$since" "uploading to an offline slot"
  dc start fakesvbe
}

down
[[ ${SKIP_BUILD:-} == 1 ]] || dc build
started=$(now)
dc up -d ministack fakesvbe agent
dc --profile tools run -d --name sv-e2e-gen --rm gen -n "$N" -rate "$RATE" -rotate 3000 >/dev/null
wait_log fakesvbe "$started" "POST /be/ingest/leases 200"

echo "--- stopping sv-be; the agent should use offline slots"
outage

echo "--- killing the agent mid-batch"
docker inspect sv-e2e-gen >/dev/null 2>&1 || fail "generator already finished: the kill would not be mid-batch"
dc kill -s SIGKILL agent
restarted=$(now)
dc start agent
wait_log agent "$restarted" "Restored offline upload slots"

echo "--- stopping sv-be again; the restarted agent should use its saved slots"
outage

echo "--- waiting for the generator"
while docker inspect sv-e2e-gen >/dev/null 2>&1; do sleep 1; done

echo "--- waiting for every line to reach the bucket"
end=$((SECONDS + DEADLINE))
until dc --profile tools run --rm verify -n "$N" -want-lease; do
  (( SECONDS < end )) || fail "not every line arrived within ${DEADLINE}s"
  sleep 5
done
echo "PASS: all $N lines delivered across two sv-be outages and a SIGKILL in between"
