# Caching

## Repository cache

Renovate's repository cache (`RENOVATE_REPOSITORY_CACHE=enabled`) speeds up runs a lot, but each job lands on a random,
short-lived pod. `REPO_CACHE_PROVIDER` picks where the cache lives between runs:

* **`nats` (default):** before a run the worker downloads `<platform>/<scope>/<repo>.json` from the JetStream Object
  Store bucket `REPO_CACHE_BUCKET` (`koshin-repo-cache`, `NATS_STREAM_REPLICAS` replicas) into Renovate's local cache
  file (`$RENOVATE_CACHE_DIR/renovate/repository/<platform>/<repo>.json`, `RENOVATE_CACHE_DIR` defaulting to
  `$RENOVATE_BASE_DIR/cache`). It uploads the file again after a **successful** run only, never after a failure,
  timeout, rate limit or lost lock. Both happen under the repo lock.
  * A missing object or a NATS error only means a cold run: the local file is dropped first, so a run never starts from
    what an unpersisted run left behind.
  * `REPO_CACHE_MAX_BYTES` caps the bucket (once it's full, uploads are skipped until old caches expire) and
    `REPO_CACHE_TTL_DAYS` drops caches of repos without a successful run for that long. The bucket shares JetStream
    storage with the job streams, so size it well below the server's `max_file_store`.
  * Objects are kept per credential scope, the one whose token the run used (`github/inst-<id>/…`, `github/pat/…`,
    `gitlab/pat/…`), so a repository name that passes to another account (a rename, a transfer) never gets the previous
    owner's cache. Objects under the earlier `<platform>/<repo>.json` names aren't read anymore and expire with
    `REPO_CACHE_TTL_DAYS`.
* **`s3`:** Renovate reads and writes the cache in S3 itself (e.g. SeaweedFS on the LAN); koshin does no sync. Set
  `RENOVATE_REPOSITORY_CACHE_TYPE=s3://<bucket>/<prefix>/`, `RENOVATE_S3_ENDPOINT` (or the legacy
  `RENOVATE_X_S3_ENDPOINT`) and the usual `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`.
  `RENOVATE_S3_PATH_STYLE` defaults to `true`. Renovate holds the bucket's credentials, so `s3` is single-tenant: the
  worker refuses to start with it and the GitHub App.
* **`local`:** the cache stays in the pod's `emptyDir`, so it only helps when a pod runs the same repo again.

The cache is checked against the repo's commit SHAs on every run, so it never needs invalidating. Restores and uploads
are logged with `cache` (`hit` / `miss` / `persisted` / `error`), `bytes` and `durationMs` fields.

## Package cache

Renovate's package cache lives in Valkey (`RENOVATE_REDIS_URL`), on a separate instance from coordination (see
[Valkey requirements](deployment.md#valkey-requirements)).

## Tenant isolation

A worker pod runs one tenant's job after another, so after every run (once a successful one's cache is uploaded) the
worker empties `RENOVATE_CACHE_DIR`: the repository cache file, cached HTTP bodies of private presets and the file
package cache never reach the next run.

Renovate's package cache is shared by all runs, so the worker pins `RENOVATE_CACHE_PRIVATE_PACKAGES=false` and
`RENOVATE_PRESET_CACHE_PERSISTENCE=false` and prefixes its keys per credential scope
(`RENOVATE_REDIS_PREFIX=koshin:<scope>:`, e.g. `koshin:github:inst-42:`). Setting `RENOVATE_REDIS_PREFIX` yourself
shares public package lookups across scopes (the worker warns at startup).
