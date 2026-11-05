#!/usr/bin/env bash
# Makes a koshin OpenPGP keypair the way docs/registry-credentials.md tells operators to: generate-key.sh <name> writes
# /tmp/keys/<name>-private.asc and /tmp/keys/<name>-public.asc.
set -euo pipefail
name="$1"
GNUPGHOME="$(mktemp -d)"
export GNUPGHOME
gpg --batch --passphrase '' --quick-generate-key "koshin $name <koshin-$name@example.test>" rsa3072 encrypt never
mkdir -p /tmp/keys
gpg --armor --export-secret-keys "koshin-$name@example.test" > "/tmp/keys/$name-private.asc"
gpg --armor --export "koshin-$name@example.test" > "/tmp/keys/$name-public.asc"
