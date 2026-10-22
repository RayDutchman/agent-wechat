#!/usr/bin/env bash
# Smoke test a built agent-server image.
#
# The script starts the same server binary that the image entrypoint starts
# (/opt/agent-server/agent-server) but replaces the entrypoint, so WeChat is
# never launched. The container is isolated from the host and from the network:
#
#   - no network access at all (--network none)
#   - no published host ports
#   - no account, session or production volume mounted
#   - a fresh agent database in a temporary in-memory filesystem
#   - a bearer token generated for this run only
#
# It then verifies the observable contract of the running server:
#
#   - GET /health             -> 200 without a token (health is auth exempt)
#   - GET /api/sessions       -> 401 without a token
#   - GET /api/sessions       -> 200 with the generated token
#   - no WeChat process inside the container
#
# Usage: scripts/smoke-image-startup.sh <image-reference>
#
# Environment overrides:
#   SMOKE_HEALTH_TIMEOUT_SECONDS  seconds to wait for /health (default: 120)

set -euo pipefail

IMAGE_REF="${1:-}"
if [ -z "$IMAGE_REF" ]; then
  echo "usage: $(basename "$0") <image-reference>" >&2
  exit 2
fi

CONTAINER_NAME="aw-image-smoke-$$"
SERVER_PORT=6174
HEALTH_TIMEOUT_SECONDS="${SMOKE_HEALTH_TIMEOUT_SECONDS:-120}"
REQUEST_TIMEOUT_SECONDS=15
TOKEN="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"

log() {
  printf '[smoke] %s\n' "$*"
}

fail() {
  log "FAILED: $*"
  exit 1
}

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    log "container logs:"
    docker logs "$CONTAINER_NAME" 2>&1 | tail -n 200 || true
    log "container state:"
    docker inspect --format 'status={{.State.Status}} exit={{.State.ExitCode}} error={{.State.Error}}' \
      "$CONTAINER_NAME" 2>&1 || true
  fi
  docker rm --force "$CONTAINER_NAME" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT
# An interrupted or terminated run must still remove the container.
trap 'exit 130' INT
trap 'exit 143' TERM

if ! docker image inspect "$IMAGE_REF" >/dev/null 2>&1; then
  log "pulling $IMAGE_REF"
  docker pull "$IMAGE_REF"
fi

log "image entrypoint: $(docker image inspect --format '{{json .Config.Entrypoint}}' "$IMAGE_REF")"
log "starting $IMAGE_REF as $CONTAINER_NAME (no network, no host ports, temporary database)"
if ! docker run --detach \
  --name "$CONTAINER_NAME" \
  --network none \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --tmpfs /data:rw,size=128m \
  --tmpfs /tmp:rw,size=64m \
  --env AGENT_WECHAT_TOKEN="$TOKEN" \
  --env AGENT_PORT="$SERVER_PORT" \
  --env AGENT_DB_PATH=/data/agent.db \
  --entrypoint /opt/agent-server/agent-server \
  "$IMAGE_REF" >/dev/null; then
  fail "the container did not start"
fi

# Query the server from inside the container: the container has no network
# interface besides loopback and no published host port.
http_status() {
  local path="$1"
  shift
  docker exec "$CONTAINER_NAME" curl --silent --show-error --output /dev/null \
    --write-out '%{http_code}' --max-time "$REQUEST_TIMEOUT_SECONDS" "$@" \
    "http://127.0.0.1:${SERVER_PORT}${path}" 2>/dev/null || true
}

deadline=$(( SECONDS + HEALTH_TIMEOUT_SECONDS ))
health_status=""
while :; do
  if ! docker inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null | grep -q '^true$'; then
    fail "the container stopped before the server answered"
  fi
  health_status="$(http_status /health)"
  if [ "$health_status" = "200" ]; then
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    fail "the server did not answer /health within ${HEALTH_TIMEOUT_SECONDS}s (last status: ${health_status:-none})"
  fi
  sleep 2
done
log "GET /health -> 200"

unauthenticated_status="$(http_status /api/sessions)"
[ "$unauthenticated_status" = "401" ] ||
  fail "GET /api/sessions without a token returned '${unauthenticated_status:-none}', expected 401"
log "GET /api/sessions without a token -> 401"

authenticated_status="$(http_status /api/sessions --header "Authorization: Bearer ${TOKEN}")"
[ "$authenticated_status" = "200" ] ||
  fail "GET /api/sessions with a valid token returned '${authenticated_status:-none}', expected 200"
log "GET /api/sessions with a valid token -> 200"

docker exec "$CONTAINER_NAME" test -f /data/agent.db ||
  fail "the agent database was not created in the temporary filesystem"
log "the agent database was created in the temporary filesystem"

process_list="$(
  docker exec "$CONTAINER_NAME" sh -c '
    for entry in /proc/[0-9]*; do
      [ -r "$entry/cmdline" ] || continue
      tr "\0" " " < "$entry/cmdline"
      printf "\n"
    done
  '
)" || fail "could not read the process list of the smoke container"
wechat_count="$(printf '%s\n' "$process_list" | grep -c "/usr/bin/[w]echat" || true)"
[ "$wechat_count" = "0" ] ||
  fail "WeChat was started inside the smoke container"
log "no WeChat process was started inside the smoke container"

if ! docker inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null | grep -q '^true$'; then
  fail "the smoke container stopped during the checks"
fi
log "the smoke container is still running"

log "image smoke test passed for $IMAGE_REF"
