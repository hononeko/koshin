#!/usr/bin/env bash
# Runs an image with NATS and Valkey unreachable for a while and fails if any line it prints isn't a JSON object,
# or if the NATS client's errors didn't reach the log at all.
#
# Waits until the NATS client logs its first error (or WAIT_SECONDS, 20, pass), then SETTLE_SECONDS (3) more so the
# apps' 2s connection retries are logged too.
set -euo pipefail

image=$1
name="json-logs-$$"
docker run -d --name "$name" -e NATS_URL=nats://127.0.0.1:4999 -e VALKEY_URL=redis://127.0.0.1:6999 "$image" >/dev/null
trap 'docker rm -f "$name" >/dev/null' EXIT

nats_lines() {
  jq -R -c 'fromjson? | select(.logger_name == "dev.hononeko.koshin.nats")' | grep -c '' || true
}

deadline=$((SECONDS + ${WAIT_SECONDS:-20}))
while [ "$SECONDS" -lt "$deadline" ]; do
  if [ "$(docker logs "$name" 2>&1 | nats_lines)" -gt 0 ]; then
    sleep "${SETTLE_SECONDS:-3}"
    break
  fi
  # An exited container logs nothing more
  [ "$(docker inspect -f '{{.State.Running}}' "$name")" = "true" ] || break
  sleep 1
done

logs=$(docker logs "$name" 2>&1)
bad=0
while IFS= read -r line; do
  if ! jq -e 'type == "object"' <<<"$line" >/dev/null 2>&1; then
    echo "::error title=Not JSON::$line"
    bad=1
  fi
done <<<"$logs"

total=$(printf '%s' "$logs" | grep -c '' || true)
nats=$(nats_lines <<<"$logs")
echo "$total lines, $nats from the NATS client"
if [ "$nats" -eq 0 ]; then
  echo "::error::No NATS client errors were logged; is NATS really unreachable?"
  bad=1
fi
exit "$bad"
