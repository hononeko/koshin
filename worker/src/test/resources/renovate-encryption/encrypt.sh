#!/usr/bin/env bash
# Encrypts a secret the way docs/registry-credentials.md tells tenants to:
# encrypt.sh <key name> <org> <repo, or "" for the whole org> <secret> prints the value for Renovate's `encrypted`
# config.
set -euo pipefail
key="$1" org="$2" repo="$3" secret="$4"
GNUPGHOME="$(mktemp -d)"
export GNUPGHOME
if [ -n "$repo" ]; then
  jq -cn --arg o "$org" --arg r "$repo" --arg v "$secret" '{o: $o, r: $r, v: $v}'
else
  jq -cn --arg o "$org" --arg v "$secret" '{o: $o, v: $v}'
fi | gpg --batch --encrypt --recipient-file "/tmp/keys/$key-public.asc" | base64 | tr -d '\n'
