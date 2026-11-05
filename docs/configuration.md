# Configuration

The proxy and the worker are configured through environment variables. The Helm chart sets them from its values (see
the [chart README](../charts/koshin/README.md#values)), and secrets come from existing Secrets.

| Variable | Component | Default | Description |
| :--- | :--- | :--- | :--- |
| `PORT` | Both | `8080` | HTTP port (`/healthz`, `/readyz`, webhooks, `/api/v1/encryption-key`) |
| `HTTP2_ENABLED` | Both | `true` | HTTP/2 on Netty (`enableHttp2` + `enableH2c`; `h2` with TLS) |
| `LOG_LEVEL` | Both | `info` | koshin's log level (`dev.hononeko.koshin` loggers) |
| `ROOT_LOG_LEVEL` | Both | `info` | Log level of the libraries (NATS, Valkey, Ktor, ...) |
| `RENOVATE_LOG_LEVEL` | Worker | `info` | Renovate's own log level; `LOG_LEVEL` isn't passed on to Renovate |
| `PLATFORMS` | Both | `github` | Enabled platforms (`github`, `gitlab`) |
| `NATS_URL` | Both | `nats://koshin-nats:4222` | NATS connection URL |
| `NATS_STREAM_NAME` | Both | `RENOVATE_JOBS` | WorkQueue stream name |
| `NATS_STREAM_REPLICAS` | Both | `1` | Stream / Object Store replicas (`3` in HA) |
| `NATS_ACK_WAIT_SECONDS` | Both | `60` | Must be > `LOCK_TTL_SECONDS` |
| `NATS_MAX_DELIVER` | Both | `5` | Redelivery cap before DLQ |
| `NATS_EXHAUSTED_SWEEP_SECONDS` | Proxy | `300` | How often to move exhausted jobs whose MaxDeliver advisory was missed to the DLQ and tidy the per-scope consumers; `0` turns it off |
| `NATS_CONSUMER_IDLE_TTL_DAYS` | Proxy | `7` | Delete a credential scope's consumer once it's had no job for this long; `0` keeps them all |
| `GITHUB_WEBHOOK_SECRET` | Proxy | — | HMAC secret for `X-Hub-Signature-256` (GitHub App webhook secret) |
| `GITLAB_WEBHOOK_SECRET` | Proxy | — | Shared token for `X-Gitlab-Token` (constant-time compare) |
| `INTERNAL_TRIGGER_TOKEN` | Proxy | — | Bearer token for `POST /api/v1/trigger` |
| `WORKER_DISPATCHER_ENABLED` | Proxy | `false` | Dispatch burst worker `Job`s from the worker `PodTemplate`; `true` in Helm with `worker.autoscaling.enabled` and in Kustomize |
| `WORKER_POD_TEMPLATE_NAME` | Proxy | `koshin-worker` | `v1/PodTemplate` the burst `Job`s embed (`<fullname>-worker` in Helm) |
| `POD_NAMESPACE` | Proxy | the ServiceAccount's namespace | Namespace of the `PodTemplate` and `Job`s (downward API) |
| `WORKER_MAX_REPLICAS` | Proxy | `MAX_CONCURRENT_RUNS_GLOBAL` | Cap on warm + burst workers (`worker.autoscaling.maxReplicas`) |
| `MIN_WARM_WORKERS` | Proxy | `0` | Warm workers assumed when their Valkey count can't be read (`worker.autoscaling.minReplicas`) |
| `WORKER_DISPATCH_INTERVAL_SECONDS` | Proxy | `5` | Reconciliation interval without publishes; the leader lease lasts 3 × it |
| `WORKER_JOB_ACTIVE_DEADLINE_SECONDS` | Proxy | `21600` | `activeDeadlineSeconds` of burst `Job`s; keep it above `WORKER_MAX_LIFETIME_SECONDS` |
| `WORKER_JOB_TTL_SECONDS` | Proxy | `60` | `ttlSecondsAfterFinished` of burst `Job`s |
| `SWEEP_REPOSITORIES_FILE` | Proxy | — | Static repo list for discovery, added to the GitHub App's repositories (e.g. GitLab projects) |
| `VALKEY_URL` | Both | `redis://koshin-valkey:6379` | Coordination Valkey (`redis://` or `redis-sentinel://`); the proxy coalesces ingest with it and fails open while it's down |
| `VALKEY_SENTINEL_PASSWORD` | Both | — | Sentinel's own password (the URL's password only applies to the Valkey nodes) |
| `VALKEY_SENTINEL_USERNAME` | Both | — | Sentinel's ACL user (`sentinel` with the official Valkey chart, which turns the default user off) |
| `RENOVATE_REDIS_URL` | Worker | `$VALKEY_URL` | Renovate package cache Valkey (`RENOVATE_REDIS_PREFIX` defaults to `koshin:<scope>:`; private package/preset caching pinned off) |
| `RENOVATE_REDIS_PREFIX` | Worker | `koshin:<scope>:` | Renovate package cache key prefix per credential scope; setting it shares the cache across scopes |
| `RENOVATE_CACHE_PRIVATE_PACKAGES` / `RENOVATE_PRESET_CACHE_PERSISTENCE` | Worker | pinned `false` / `false` | Nothing private in the shared package cache; other values are ignored with a warning |
| `LOCK_TTL_SECONDS` | Worker | `45` | Per-repo lock TTL |
| `LOCK_REPLICA_ACK_TIMEOUT_MS` | Worker | `0` (off) | After taking a repo lock, wait this long for a replica to acknowledge it (`WAIT 1 <ms>`); `ha` only, see [Valkey requirements](deployment.md#valkey-requirements) |
| `HEARTBEAT_INTERVAL_SECONDS` | Worker | `15` | Heartbeat / lock / run-slot renewal interval |
| `DIRTY_TTL_SECONDS` | Both | `3600` | Must be > `CLI_TIMEOUT_MINUTES` |
| `QUEUED_TTL_SECONDS` | Both | `86400` | How long a repo stays marked queued; bounds how long a lost job blocks its webhooks |
| `COALESCE_TIMEOUT_MS` | Proxy | `500` | How long ingest waits for Valkey before publishing without coalescing (fail-open) |
| `LOCK_COLLISION_RETRY_SECONDS` | Worker | `30` | NAK delay for a redelivered job that hits a held lock |
| `WORKER_IDLE_TIMEOUT_SECONDS` | Worker | `0` | `0`: run until stopped and keep a warm-worker lease; `> 0`: drain runnable jobs, then exit `0` once idle this long |
| `WORKER_MAX_LIFETIME_SECONDS` | Worker | `0` (unlimited) | Past it, finish the job in flight and exit `0` instead of fetching another |
| `CLI_TIMEOUT_MINUTES` | Worker | `45` | Hard cap per `renovate-cli` run (`+ 5` must stay < 60 for GitHub App tokens) |
| `RUNNER_SOCKET` | Worker | — | Unix socket of the Renovate runner (`/run/koshin/runner.sock` in the chart); unset: Renovate runs in the worker container |
| `RENOVATE_BASE_DIR` | Worker | `/tmp/renovate` | Workspace (`emptyDir`; `repos/` and `cache/` deleted after every run) |
| `RENOVATE_BINARYSOURCE` | Worker | `global` | Tools come from the `full` image |
| `REPO_CACHE_PROVIDER` | Worker | `nats` | `nats` (Object Store sync, scoped as `<platform>/<scope>/<repo>.json`), `s3` (native Renovate, single-tenant PAT mode only: refused with the GitHub App), `local` |
| `REPO_CACHE_BUCKET` | Worker | `koshin-repo-cache` | Object Store bucket |
| `REPO_CACHE_MAX_BYTES` | Worker | — (unlimited) | Bucket size cap; uploads are skipped while it's full |
| `REPO_CACHE_TTL_DAYS` | Worker | — (forever) | Drops caches of repos without a successful run for this long |
| `RENOVATE_CACHE_DIR` | Worker | `$RENOVATE_BASE_DIR/cache` | Renovate's cache dir; wiped after every run once `persist` completes |
| `RENOVATE_REPOSITORY_CACHE_TYPE` / `RENOVATE_S3_ENDPOINT` / `RENOVATE_S3_PATH_STYLE` | Worker | — / — / `true` | Used when `REPO_CACHE_PROVIDER=s3` (`s3://<bucket>/<prefix>/`); the legacy `RENOVATE_X_S3_*` names are accepted |
| `MAX_CONCURRENT_RUNS_GLOBAL` | Both | `6` | Global run-slot cap across all credentials (the dispatcher caps its demand with it too) |
| `MAX_CONCURRENT_RUNS_PER_CREDENTIAL` | Both | `2` | Per-credential (`github:inst-<id>`, `github:pat`, `gitlab:pat`) run-slot cap (`MAX_CONCURRENT_RUNS_PER_PLATFORM` accepted as legacy alias) |
| `RENOVATE_MAX_REQUESTS_PER_SECOND` | Worker | — | Injected into Renovate `hostRules` |
| `RENOVATE_CONCURRENT_REQUEST_LIMIT` | Worker | — | Injected into Renovate `hostRules` |
| `RATE_LIMIT_BACKOFF_BASE_SECONDS` | Worker | `60` | Cooldown base |
| `RATE_LIMIT_BACKOFF_MAX_SECONDS` | Worker | `3600` | Cooldown cap |
| `MAX_RATE_LIMIT_REQUEUES` | Worker | `5` | Rate-limit requeues per job before it's dead-lettered |
| `RATE_LIMIT_LOG_PATTERN` | Worker | `^Rate limit exceeded - aborting$` | Regex for the Renovate log message of a run aborted by the platform's rate limit |
| `GITHUB_APP_ID` / `GITHUB_APP_PRIVATE_KEY_PATH` | Both | — | GitHub App auth (preferred) |
| `GITHUB_APP_ALLOWED_ACCOUNTS` | Both | — | Comma-separated GitHub user/org IDs or logins allowed to use a public App (unset = allow all with startup warning) |
| `GITHUB_APP_DISALLOWED_ACTION` | Both | `ignore` | Action for non-allowed installations: `ignore`, `suspend`, or `uninstall` |
| `GITHUB_TOKEN` | Worker | — | PAT fallback when the App isn't configured |
| `GITHUB_API_URL` | Both | `https://api.github.com` | GHES support |
| `GITLAB_TOKEN` / `GITLAB_ENDPOINT` | Worker | — | GitLab credentials |
| `GITHUB_COM_TOKEN` / `RENOVATE_GITHUB_COM_TOKEN` | Worker | — | Forwarded only on non-`github.com` runs (GitLab / GHES) for release-note lookups; stripped on `github.com` runs |
| `RENOVATE_PUBLIC_KEY_PATH` | Proxy | — | Armored OpenPGP public key served at `GET /api/v1/encryption-key` |
| `RENOVATE_PRIVATE_KEY_PATH` / `RENOVATE_PRIVATE_KEY_PATH_OLD` | Worker | — | OpenPGP private key (and previous key during rotation) for Renovate `encrypted` secrets, passed on to Renovate, which reads it in the `renovate` container; raw `RENOVATE_PRIVATE_KEY` / `RENOVATE_PRIVATE_KEY_OLD` are never passed on |
| `RENOVATE_ALLOW_SCRIPTS` / `RENOVATE_ALLOWED_UNSAFE_EXECUTIONS` / `RENOVATE_ALLOW_PLUGINS` / `RENOVATE_ALLOW_CUSTOM_CRATE_GIT_REGISTRIES` | Worker | pinned `false` / `[]` / `false` / `false` | Untrusted-code guardrails; other values are ignored with a warning |
| `RENOVATE_ALLOWED_COMMANDS` / `RENOVATE_ALLOW_SHELL_EXECUTOR_FOR_POST_UPGRADE_COMMANDS` | Worker | — | Passed on to Renovate; pinned to `[]` / `false` (with a warning) when `RENOVATE_PRIVATE_KEY_PATH` is set |
