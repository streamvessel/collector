#!/usr/bin/env bash
# Kill-mid-batch and sv-be-outage test: nothing the agent read may be lost.
#
#   1. start MiniStack, fake sv-be and the agent; start writing N lines
#   2. SIGKILL the agent mid-batch, start it again on the same state volume
#   3. stop sv-be; the agent uploads to offline slots; start sv-be again
#   4. wait until every line 1..N is in the bucket (duplicates allowed)
set -euo pipefail
cd "$(dirname "$0")"

N=${N:-12000}
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
echo "--- killing the agent mid-batch"
dc kill -s SIGKILL agent
sleep 3
dc start agent

sleep 6
echo "--- stopping sv-be; the agent should use offline slots"
dc stop fakesvbe
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

dc logs --no-color agent | grep -q "uploading to an offline slot" || fail "agent never used an offline slot"
echo "PASS: all $N lines delivered across a SIGKILL and an sv-be outage"
