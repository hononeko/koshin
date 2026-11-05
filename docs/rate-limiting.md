# Rate limiting and sizing

Git provider API limits cap throughput long before NATS or Valkey do (GitHub: 5,000 requests/hour per token, plus
secondary limits on concurrency).

* **Credential scopes:** every job runs with one credential, its scope: a GitHub App installation (`github:inst-<id>`,
  with its own rate-limit bucket) or the platform's token (`github:pat`, `gitlab:pat`). Jobs are queued per scope, on
  `renovate.jobs.<platform>.<scope>` with the consumer `koshin-workers-<platform>-<scope>`, which the publisher
  creates. The proxy takes a job's installation from the webhook (`installation.id`), from discovery, or for a
  triggered repository from a lookup cached per owner; follow-ups and requeues keep it.
* **Run slots:** at most `MAX_CONCURRENT_RUNS_PER_CREDENTIAL` (default `2`) Renovate runs per scope and
  `MAX_CONCURRENT_RUNS_GLOBAL` (default `6`) in all, **across all pods**. A worker reserves both before fetching a job,
  so waiting jobs stay pending in NATS without spending `NATS_MAX_DELIVER` attempts, and takes the scopes with queued
  jobs round-robin, so one tenant's backlog can't starve another. Leases live in Valkey, are renewed by the job
  heartbeat, and expire if a worker crashes.
* **Per-request limits:** `RENOVATE_MAX_REQUESTS_PER_SECOND` / `RENOVATE_CONCURRENT_REQUEST_LIMIT` become a Renovate
  `hostRules` entry for the platform's API host on every run, appended to any `RENOVATE_HOST_RULES` you set.
* **Cooldown:** when Renovate aborts a run on the platform's rate limit (it still exits 0; the worker reads its JSON
  logs, matching `RATE_LIMIT_LOG_PATTERN`), every worker stops fetching that scope's jobs, and only that scope's, for
  the `Retry-After` / reset time if Renovate logged one, else
  `min(RATE_LIMIT_BACKOFF_BASE_SECONDS × 2^n, RATE_LIMIT_BACKOFF_MAX_SECONDS) ± 20%`. `n` resets after a successful run
  in the scope. The job is requeued up to `MAX_RATE_LIMIT_REQUEUES` times, then dead-lettered.

## Sizing

A scope's API budget ≈ its run slots × requests per second per run. For example, 2 slots × 2 req/s ≈ 14,400
requests/hour, well over a 5,000/hour token, so either lower `RENOVATE_MAX_REQUESTS_PER_SECOND` (2 × 0.6 ≈ 4,300/hour)
or accept occasional cooldowns. Keep `WORKER_MAX_REPLICAS` close to `MAX_CONCURRENT_RUNS_GLOBAL` (the default): extra
pods only wait for a slot. During a cooldown the proxy starts no worker for that scope's jobs.

The [chart README](../charts/koshin/README.md#sizing) lists the resource requests of every component.

## Upgrading from per-platform queues

The proxy moves jobs queued on `renovate.jobs.<platform>` to their scope's subject and deletes the
`koshin-workers-<platform>` consumers, at startup and on every housekeeping pass. A job an older worker is running then
may run once more. `MAX_CONCURRENT_RUNS_PER_PLATFORM` is still read, as the per-credential cap.
