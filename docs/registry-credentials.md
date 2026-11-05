# Encrypted registry credentials

Installation tokens can't read GitHub Packages (`maven.pkg.github.com`, `nuget.pkg.github.com`, an organization's
`npm.pkg.github.com` and `ghcr.io` packages), and koshin keeps no secrets of its own for tenants. Instead, tenants
encrypt their private registry credentials against koshin's OpenPGP public key with Renovate's
[`encrypted`](https://docs.renovatebot.com/configuration-options/#encrypted) config. Renovate decrypts a secret only
for the repositories it's scoped to (`o`: organization, `r`: optionally one of its repositories), so a secret copied
into another tenant's config doesn't decrypt. koshin doesn't add `hostRules` from installation tokens, and doesn't set
`RENOVATE_X_GITHUB_HOST_RULES`.

## Operators

Make a keypair without a passphrase (Renovate can't use one) and store it in a Secret:

```bash
export GNUPGHOME="$(mktemp -d)"
gpg --batch --passphrase '' --quick-generate-key 'koshin <renovate@koshin.example.com>' rsa3072 encrypt never
gpg --armor --export-secret-keys renovate@koshin.example.com > private.asc
gpg --armor --export renovate@koshin.example.com > public.asc
kubectl -n koshin create secret generic koshin-renovate-encryption --from-file=private.asc --from-file=public.asc
```

Then set `renovateEncryption.existingSecret=koshin-renovate-encryption` in the Helm chart, or add the
[`k8s/components/renovate-encryption`](../k8s/components/renovate-encryption/kustomization.yaml) component to a
Kustomize overlay. Either way:

* The private key is mounted into the worker pod's `renovate` container only, and the worker passes
  `RENOVATE_PRIVATE_KEY_PATH` on to Renovate. A raw `RENOVATE_PRIVATE_KEY` or `RENOVATE_PRIVATE_KEY_OLD` is never
  passed on, so the key itself isn't in the environment of Renovate or anything it starts.
* The public key is mounted into the proxy only (`RENOVATE_PUBLIC_KEY_PATH`, checked at startup). The proxy serves it,
  unauthenticated, at `GET /api/v1/encryption-key` (`application/pgp-keys`; `404` without a key). The chart's ingress
  and HTTPRoute route that path too.
* **No repository code next to the key:** the worker pins `RENOVATE_ALLOW_SCRIPTS=false`,
  `RENOVATE_ALLOWED_UNSAFE_EXECUTIONS=[]`, `RENOVATE_ALLOW_PLUGINS=false` and
  `RENOVATE_ALLOW_CUSTOM_CRATE_GIT_REGISTRIES=false`, and ignores other values (with a warning). With
  `RENOVATE_PRIVATE_KEY_PATH` set it also pins `RENOVATE_ALLOWED_COMMANDS=[]` and
  `RENOVATE_ALLOW_SHELL_EXECUTOR_FOR_POST_UPGRADE_COMMANDS=false`, so `postUpgradeTasks` run nothing: whatever they ran
  could read the key and decrypt every tenant's secrets.

### Rotating the key

To rotate without breaking tenants' configs:

1. Make a new keypair. Put it in the Secret as `private.asc` and `public.asc`, and the current private key as
   `private-old.asc`. Set `renovateEncryption.privateKeyOldKey=private-old.asc` (Kustomize: mount it too, and set
   `RENOVATE_PRIVATE_KEY_PATH_OLD` on the worker).
2. Renovate tries the new key, then the old one. The proxy serves the new public key once the Secret update reaches its
   pod, without a restart.
3. Once tenants have encrypted their secrets again, remove the old key.

## Tenants

Encrypt a secret with `gpg` and `jq`. `--recipient-file` uses the downloaded key without importing it:

```bash
curl -fsSo koshin.asc https://koshin.example.com/api/v1/encryption-key
# For one repository, acme/app:
jq -cn --arg o acme --arg r app --arg v "$TOKEN" '{o: $o, r: $r, v: $v}' \
  | gpg --batch --encrypt --recipient-file koshin.asc | base64 | tr -d '\n'
# For every repository of acme:
jq -cn --arg o acme --arg v "$TOKEN" '{o: $o, v: $v}' \
  | gpg --batch --encrypt --recipient-file koshin.asc | base64 | tr -d '\n'
```

The output goes into an `encrypted` block, in `renovate.json` for one repository:

```json
{
  "hostRules": [
    {
      "matchHost": "maven.pkg.github.com",
      "username": "acme-bot",
      "encrypted": { "password": "<encrypted for acme/app>" }
    }
  ]
}
```

To cover every repository of an organization, put it in a preset, e.g. `default.json` in `acme/renovate-config`, and
have each repository `"extends": ["local>acme/renovate-config"]`. Renovate decrypts a preset's secrets for the
repository it's running, so they work in every `acme` repository and in no other organization's, even one that extends
the preset:

```json
{
  "hostRules": [
    { "matchHost": "npm.pkg.github.com", "hostType": "npm", "encrypted": { "token": "<encrypted for acme>" } },
    { "matchHost": "registry.example.com", "encrypted": { "token": "<encrypted for acme>" } }
  ]
}
```

For GitHub Packages the secret is a classic PAT with `read:packages`; for `ghcr.io`, set `"hostType": "docker"` and the
PAT's owner as `username`.
