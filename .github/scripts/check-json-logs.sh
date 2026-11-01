#!/usr/bin/env bash
# Runs an image with NATS and Valkey unreachable for a while and fails if any line it prints isn't a JSON object,
# or if the NATS client's errors didn't reach the log at all.
set -euo pipefail

image=$1
name="json-logs-$$"
docker run -d --name "$name" -e NATS_URL=nats://127.0.0.1:4999 -e VALKEY_URL=redis://127.0.0.1:6999 "$image" >/dev/null
trap 'docker rm -f "$name" >/dev/null' EXIT
sleep "${WAIT_SECONDS:-20}"

logs=$(docker logs "$name" 2>&1)
bad=0
while IFS= read -r line; do
  if ! jq -e 'type == "object"' <<<"$line" >/dev/null 2>&1; then
    echo "::error title=Not JSON::$line"
    bad=1
  fi
done <<<"$logs"

total=$(wc -l <<<"$logs")
nats=$(jq -c 'select(.logger_name == "dev.hononeko.koshin.nats")' <<<"$logs" 2>/dev/null | wc -l)
echo "$total lines, $nats from the NATS client"
if [ "$nats" -eq 0 ]; then
  echo "::error::No NATS client errors were logged; is NATS really unreachable?"
  bad=1
fi
exit "$bad"
