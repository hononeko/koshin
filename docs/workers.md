# Workers

A worker pulls jobs from NATS, takes the repository's lock in Valkey, runs Renovate for it and reports back. No worker
runs while the queue is empty: the proxy starts them on demand (see [Dispatch](#dispatch)).

## Runtime

* **1 job per pod.** The daemon polls the enabled `PLATFORMS` and runs one `renovate <repository>` at a time. Each run
  gets `RENOVATE_PLATFORM`, `RENOVATE_ENDPOINT` and `RENOVATE_TOKEN` for the job's platform
  (`GITHUB_TOKEN`/`GITHUB_API_URL`, `GITLAB_TOKEN`/`GITLAB_ENDPOINT`), plus `LOG_FORMAT=json`, `RENOVATE_BASE_DIR`,
  `RENOVATE_BINARY_SOURCE`, `RENOVATE_REDIS_URL` and the [repository cache](caching.md) settings. Other `RENOVATE_*`
  variables on the worker are passed through; worker secrets (`GITHUB_TOKEN`, `GITLAB_TOKEN`, `VALKEY_URL`, …) are
  not.
* **Timeouts and cancellation** (`CLI_TIMEOUT_MINUTES`, lost lock, shutdown) forcibly kill the whole CLI process tree.
* **Workspaces:** `$RENOVATE_BASE_DIR/repos/<platform>/<repo>` is deleted after every run; everything under
  `$RENOVATE_BASE_DIR` is purged at startup.
* **SIGTERM:** stops fetching, kills the CLI, releases the lock (keeping the dirty flag), NAKs the delivery and releases
  the run slot, without waiting for the run to finish.
* **Drain-then-exit:** with `WORKER_IDLE_TIMEOUT_SECONDS` > 0 the worker runs jobs back to back while it finds any it
  can run, and exits 0 once it found none for that long (jobs of a credential scope in cooldown don't count). Past
  `WORKER_MAX_LIFETIME_SECONDS` it finishes the job in flight and exits 0 instead of fetching another. At `0` (the
  default) it runs until stopped and keeps a warm-worker lease in Valkey (`koshin:{workers}:warm`, renewed every
  heartbeat and dropped on SIGTERM), so the proxy's dispatcher can tell a worker is ready.
* **No Kubernetes API access:** workers mount no ServiceAccount token and need no RBAC.
* **Health:** `GET /healthz` is liveness only; `GET /readyz` checks NATS and Valkey.
* **Dead-lettering (in the proxy):** a job that uses up its `NATS_MAX_DELIVER` deliveries is moved to
  `renovate.dlq.<platform>` when NATS announces it (the MaxDeliver advisory), and its repo's queued mark is cleared.
  That announcement is sent once, so every `NATS_EXHAUSTED_SWEEP_SECONDS` (5 minutes) each proxy replica also moves
  exhausted jobs whose announcement nobody handled, e.g. during a rolling deployment. The proxy does this, so it works
  with no worker running. The same pass tidies the per-scope consumers: it creates any missing for queued jobs, and
  deletes those without a job for `NATS_CONSUMER_IDLE_TTL_DAYS` (7).

## Isolation

Renovate, and any repo code it runs (e.g. Gradle during lockfile updates), runs in the pod's `renovate` container (the
upstream Renovate image), not the worker's.

* The worker hands it each run over a Unix socket (`RUNNER_SOCKET`, in a shared `emptyDir`) with the run's
  environment, and the runner sends back Renovate's output and exit code. Renovate gets the `renovate` container's own
  `PATH`, `HOME` and locale plus the run's variables, never the worker container's.
* The `renovate` container has none of the worker's secrets and no ServiceAccount token, its processes can't see the
  worker's through `/proc`, and it mounts the runner read-only at `/opt/koshin`.
* **Leftover processes:** after every run, normal exit included, whatever it left behind is killed and reaped,
  daemonized (`setsid`) processes too: in the `renovate` container everything but the runner (`dumb-init`, from
  containerbase's entrypoint, reaps them, or the runner when it's PID 1), and without a runner whatever the worker
  adopted as a child subreaper (Linux). That includes Renovate's gpg-agent, which the next run starts again, and Bazel
  servers.
* Without `RUNNER_SOCKET`, the worker runs `renovate` in its own container, which the `koshin-worker` image doesn't
  have.

The [chart README](../charts/koshin/README.md#worker-isolation) shows how the chart sets this up.

## Dispatch

The proxy (`WORKER_DISPATCHER_ENABLED=true`) starts burst worker `Job`s from the worker `PodTemplate` as jobs it can
run queue up, up to `WORKER_MAX_REPLICAS` (run slots × platforms by default). They run jobs back to back and exit once
idle for `WORKER_IDLE_TIMEOUT_SECONDS`. Only one proxy replica, holding a Valkey lease, starts them, and a scope in a
rate-limit cooldown adds no demand. The Helm chart can also keep one warm worker running
(`worker.autoscaling.minReplicas: 1`).

[Architecture § Deployment Profiles](architecture.md#8-deployment-profiles) has the demand formula, and the
[chart README](../charts/koshin/README.md#workers) the values that tune it.
