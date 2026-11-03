# koshin

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.3+-purple.svg)](https://kotlinlang.org)
[![JDK](https://img.shields.io/badge/JDK-25-orange.svg)](https://adoptium.net)
[![Gradle](https://img.shields.io/badge/Gradle-9.4+-green.svg)](https://gradle.org)

Stateless, horizontally scalable Renovate CLI orchestrator built with Kotlin, Ktor, NATS JetStream, Valkey, and KEDA.

---

## Architecture & Modules

`koshin` is structured following Hexagonal Architecture (Ports and Adapters) with clean module boundaries:

* **`:core`**: Pure Kotlin domain models, use cases, and hexagonal ports (zero dependency on infrastructure frameworks).
* **`:adapters:nats`**: NATS JetStream `WorkQueue` publisher/consumer adapter and Object Store repository cache.
* **`:adapters:state-valkey`**: Valkey/Redis adapter for per-repository distributed mutexes and atomic dirty-flag coalescing via Lettuce.
* **`:adapters:github-app`**: GitHub App authentication (JWT signing, installation tokens), bot identity and repository discovery.
* **`:proxy`**: Stateless Ktor HTTP server receiving webhooks (GitHub/GitLab) and internal cron trigger schedules.
* **`:worker`**: Autonomous coroutine daemon that pulls jobs from NATS, acquires repo mutexes, runs Renovate CLI subprocesses, deletes each repo workspace after the run, and handles heartbeats & dirty catch-ups.
* **`charts/koshin`**: Helm chart with the `minimal` and `ha` profiles, the primary install path.

---

## Prerequisites & Tech Stack

* **Java Development Kit (JDK):** JDK 25 (`jvmToolchain(25)`)
* **Build Tool:** Gradle 9.4+ (Wrapper provided)
* **Kotlin:** 2.3+
* **Framework:** Ktor 3.x
* **Static Analysis & Formatting:** Detekt 2.0 with ktlint-wrapper rules
* **Code Coverage:** Kotlinx Kover 0.9+

---

## Build & Test Commands

Build the entire multi-module project:
```bash
./gradlew build
```

Run static analysis (Detekt with type resolution & ktlint formatting rules), test suites, and Kover verification across all modules:
```bash
./gradlew check
```

Run the integration tests (Testcontainers, needs Docker), including the end-to-end suite in `e2e/` that drives the proxy and two workers against real NATS and Valkey:
```bash
./gradlew integrationTest
```

Generate Kover code coverage reports (XML & HTML):
```bash
./gradlew koverHtmlReport koverXmlReport
```
HTML report will be generated at `build/reports/kover/html/index.html`.

Run Detekt directly:
```bash
./gradlew detekt
```

---

## Valkey Coordination Requirements

The coordination Valkey instance (`VALKEY_URL`, supporting both standalone `redis://` and Sentinel `redis-sentinel://` URIs) stores per-repository queued marks (`koshin:{<platform>:<repo>}:queued`), mutexes (`koshin:{<platform>:<repo>}:lock`) and dirty flags (`koshin:{<platform>:<repo>}:dirty`) with explicit TTLs (`EX`).

* **Pre-queue coalescing (KSH-30):** the proxy publishes a webhook or trigger only for a repo that is neither queued nor running: a duplicate for a queued repo is dropped, and one for a running repo sets its dirty flag for a single follow-up run. If Valkey fails or doesn't answer within `COALESCE_TIMEOUT_MS`, the proxy publishes anyway (fail-open) and the workers coalesce the duplicate. A queued mark lasts at most `QUEUED_TTL_SECONDS` (24h), so a job lost without being dead-lettered (a NATS restart on `emptyDir`) blocks its repo's webhooks until then.

* **Minimum Server Version:** `releaseLockAndTakeDirty` uses the `GETDEL` command inside Lua, requiring **Valkey 7.2+** (or **Redis 6.2+**).
* **`maxmemory-policy noeviction` (Required):** You **must** configure `maxmemory-policy noeviction` on the coordination Valkey instance. Because repository locks and dirty flags are TTL-bearing keys, any `volatile-*` eviction policy (`volatile-lru`, `volatile-ttl`, `volatile-random`, `volatile-lfu`) or `allkeys-*` policy under memory pressure would evict active locks or pending dirty flags first, breaking mutual exclusion and coalescing guarantees.
  * Keep Renovate's package cache (`RENOVATE_REDIS_URL`) on a **separate** instance with `allkeys-lru`: with `noeviction`, a full cache makes Valkey reject every write, lock acquisition included. Both the `minimal` profile and `docker-compose.yml` run a dedicated cache Valkey.
  * In the **`ha`** deployment profile, Renovate's package cache runs on a dedicated Valkey instance where `allkeys-lru` is safe.
* **Optional Replica Acknowledgment (`LOCK_REPLICA_ACK_TIMEOUT_MS`):** When set to a positive value (in milliseconds), `ValkeyRepoCoordinationAdapter` issues `WAIT 1 <ms>` after acquiring a repository lock to narrow the replication failover window in primary-replica (`ha`) topologies. Keep this timeout small (e.g., `20`–`100` ms) and **only enable it in the `ha` profile**; on a single-node `minimal` deployment with zero replicas, `WAIT 1 <ms>` blocks the connection for the full timeout on every lock acquisition.

---

## Worker Runtime (KSH-7)

* **1 job per pod.** The daemon polls the enabled `PLATFORMS` and runs one `renovate <repository>` at a time. Each run gets `RENOVATE_PLATFORM`, `RENOVATE_ENDPOINT` and `RENOVATE_TOKEN` for the job's platform (`GITHUB_TOKEN`/`GITHUB_API_URL`, `GITLAB_TOKEN`/`GITLAB_ENDPOINT`), plus `LOG_FORMAT=json`, `RENOVATE_BASE_DIR`, `RENOVATE_BINARY_SOURCE`, `RENOVATE_REDIS_URL` and the repository cache settings below. Other `RENOVATE_*` variables on the worker are passed through; worker secrets (`GITHUB_TOKEN`, `GITLAB_TOKEN`, `VALKEY_URL`, …) are not.
* **Isolation (KSH-21):** Renovate, and any repo code it runs (e.g. Gradle during lockfile updates), runs in the pod's `renovate` container (the upstream Renovate image), not the worker's. The worker hands it each run over a Unix socket (`RUNNER_SOCKET`, in a shared `emptyDir`) with the run's environment, and the runner sends back Renovate's output and exit code. Renovate gets the `renovate` container's own `PATH`, `HOME` and locale plus the run's variables, never the worker container's. The `renovate` container has none of the worker's secrets and no ServiceAccount token, its processes can't see the worker's through `/proc`, and it mounts the runner read-only at `/opt/koshin`. After each run the runner kills every process left in its container. Without `RUNNER_SOCKET`, the worker runs `renovate` in its own container, which the `koshin-worker` image doesn't have.
* **Timeouts and cancellation** (`CLI_TIMEOUT_MINUTES`, lost lock, shutdown) forcibly kill the whole CLI process tree.
* **Workspaces:** `$RENOVATE_BASE_DIR/repos/<platform>/<repo>` is deleted after every run; everything under `$RENOVATE_BASE_DIR` is purged at startup.
* **SIGTERM:** stops fetching, kills the CLI, releases the lock (keeping the dirty flag), NAKs the delivery and releases the run slot, without waiting for the run to finish.
* **Drain-then-exit (KSH-31):** with `WORKER_IDLE_TIMEOUT_SECONDS` > 0 the worker runs jobs back to back while it finds any it can run, and exits 0 once it found none for that long (jobs of a platform in cooldown don't count). Past `WORKER_MAX_LIFETIME_SECONDS` it finishes the job in flight and exits 0 instead of fetching another. At `0` (the default) it runs until stopped and keeps a warm-worker lease in Valkey (`koshin:{workers}:warm`, renewed every heartbeat and dropped on SIGTERM), so the proxy's job dispatcher (KSH-32) can tell a worker is ready.
* **No Kubernetes API access:** workers mount no ServiceAccount token and need no RBAC.
* **Health:** `GET /healthz` is liveness only; `GET /readyz` checks NATS and Valkey.
* **Dead-lettering (in the proxy, KSH-30):** a job that uses up its `NATS_MAX_DELIVER` deliveries is moved to `renovate.dlq.<platform>` when NATS announces it (the MaxDeliver advisory), and its repo's queued mark is cleared. That announcement is sent once, so every `NATS_EXHAUSTED_SWEEP_SECONDS` (5 minutes) each proxy replica also moves exhausted jobs whose announcement nobody handled, e.g. during a rolling deployment. The proxy does this, and creates the consumers, so it works with no worker running.

## GitHub App (KSH-14)

With `GITHUB_APP_ID` and `GITHUB_APP_PRIVATE_KEY_PATH` set on the proxy and the worker, koshin works as a GitHub App instead of with `GITHUB_TOKEN`; [`github-app/`](github-app/README.md) has the manifest and how to register it.

* **Runs** authenticate with installation tokens, cached in memory per installation and handed to a run only while they have at least `CLI_TIMEOUT_MINUTES + 5` minutes left (they last 60), so `CLI_TIMEOUT_MINUTES` must stay below 55. Renovate commits as the App's bot, `<slug>[bot]` (`RENOVATE_USERNAME`, `RENOVATE_GIT_AUTHOR`).
* **Webhooks:** the proxy ignores the bot's own events. Installing the App on repositories runs each of them once.
* **Sweeper:** `discover: true` covers every repository of every installation (archived and disabled ones excluded), plus `SWEEP_REPOSITORIES_FILE`.
* GitHub Enterprise Server works through `GITHUB_API_URL`.
* **Allowed accounts (KSH-17):** a public App can be installed by anyone, so `GITHUB_APP_ALLOWED_ACCOUNTS` (numeric account IDs preferred, or logins) lists the users and organizations koshin serves. Other installations get no runs, tokens or sweeps, and `GITHUB_APP_DISALLOWED_ACTION` (`ignore`, `suspend`, `uninstall`) acts on them. Unset, every installation is served.

## Logging

The proxy and worker print one JSON object per line (logstash-logback-encoder): `message`, `level`, `logger_name`, `thread_name`, `stack_trace`, plus any key-value fields at the top level.

* **Job context:** while the worker handles a job, every line it logs carries `repo`, `deliveryId`, `triggerReason` and `deliveryAttempt`.
* **Renovate output:** each line Renovate prints (`LOG_FORMAT=json`) is logged at its own level, with Renovate's `msg` as the `message` and the whole line under `renovate` (`renovate.level`, `renovate.repository`, `renovate.err`, ...). Lines that aren't JSON are logged as they are.
* **NATS client:** connection events and errors go through the same log (logger `dev.hononeko.koshin.nats`), not `java.util.logging`.
* **Levels:** `LOG_LEVEL` sets koshin's level and `ROOT_LOG_LEVEL` the libraries' (both `info` by default), read at startup, so changing them needs only a restart. Renovate's own level is `RENOVATE_LOG_LEVEL`; `LOG_LEVEL` isn't passed on to Renovate.

## Shared Repository Cache (KSH-10)

Renovate's repository cache (`RENOVATE_REPOSITORY_CACHE=enabled`) speeds up runs a lot, but each job lands on a random, short-lived pod. `REPO_CACHE_PROVIDER` picks where the cache lives between runs:

* **`nats` (default):** before a run the worker downloads `<platform>/<repo>.json` from the JetStream Object Store bucket `REPO_CACHE_BUCKET` (`koshin-repo-cache`, `NATS_STREAM_REPLICAS` replicas) into Renovate's local cache file (`$RENOVATE_CACHE_DIR/renovate/repository/<platform>/<repo>.json`, `RENOVATE_CACHE_DIR` defaulting to `$RENOVATE_BASE_DIR/cache`). It uploads the file again after a **successful** run only, never after a failure, timeout, rate limit or lost lock. Both happen under the repo lock. A missing object or a NATS error only means a cold run: the local file is dropped first, so a run never starts from what an unpersisted run left behind. `REPO_CACHE_MAX_BYTES` caps the bucket (once it's full, uploads are skipped until old caches expire) and `REPO_CACHE_TTL_DAYS` drops caches of repos without a successful run for that long. The bucket shares JetStream storage with the job streams, so size it well below the server's `max_file_store`.
* **`s3`:** Renovate reads and writes the cache in S3 itself (e.g. SeaweedFS on the LAN); koshin does no sync. Set `RENOVATE_REPOSITORY_CACHE_TYPE=s3://<bucket>/<prefix>/`, `RENOVATE_S3_ENDPOINT` (or the legacy `RENOVATE_X_S3_ENDPOINT`) and the usual `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`. `RENOVATE_S3_PATH_STYLE` defaults to `true`.
* **`local`:** the cache stays in the pod's `emptyDir`, so it only helps when a pod runs the same repo again.

The cache is checked against the repo's commit SHAs on every run, so it never needs invalidating. Restores and uploads are logged with `cache` (`hit` / `miss` / `persisted` / `error`), `bytes` and `durationMs` fields.

---

## Rate Limiting & Sizing (KSH-11)

Git provider API limits cap throughput long before NATS or Valkey do (GitHub: 5,000 requests/hour per token, plus secondary limits on concurrency).

* **Run slots:** at most `MAX_CONCURRENT_RUNS_PER_PLATFORM` (default `3`) Renovate runs per platform at once, **across all pods**. A worker reserves a slot before fetching a job, so waiting jobs stay pending in NATS without spending `NATS_MAX_DELIVER` attempts. Leases live in Valkey, are renewed by the job heartbeat, and expire if a worker crashes.
* **Per-request limits:** `RENOVATE_MAX_REQUESTS_PER_SECOND` / `RENOVATE_CONCURRENT_REQUEST_LIMIT` become a Renovate `hostRules` entry for the platform's API host on every run, appended to any `RENOVATE_HOST_RULES` you set.
* **Cooldown:** when Renovate aborts a run on the platform's rate limit (it still exits 0; the worker reads its JSON logs, matching `RATE_LIMIT_LOG_PATTERN`), every worker stops fetching that platform for the `Retry-After` / reset time if Renovate logged one, else `min(RATE_LIMIT_BACKOFF_BASE_SECONDS × 2^n, RATE_LIMIT_BACKOFF_MAX_SECONDS) ± 20%`. `n` resets after a successful run. The job is requeued up to `MAX_RATE_LIMIT_REQUEUES` times, then dead-lettered.

**Sizing:** API budget ≈ run slots × requests per second per run. For example, 3 slots × 2 req/s ≈ 21,600 requests/hour, well over a 5,000/hour token, so either lower `RENOVATE_MAX_REQUESTS_PER_SECOND` (3 × 0.4 ≈ 4,300/hour) or accept occasional cooldowns. Keep the KEDA `maxReplicaCount` close to the total run slots: extra pods only wait for a slot. During a cooldown KEDA still counts the pending jobs and may scale up pods that sit idle until it ends.

---

## Running Locally (Docker Compose)

`docker compose up --build` starts NATS (JetStream), the coordination and cache Valkeys, the proxy (`localhost:8080`) and a worker (`localhost:8081`) with its Renovate runner (`renovate-init`, then the upstream Renovate image), matching the `minimal` profile. Export `GITHUB_TOKEN` (or `GITLAB_TOKEN`) first so runs can authenticate, then trigger one:

```bash
curl -X POST localhost:8080/api/v1/trigger -H "Authorization: Bearer dev-trigger-token" \
  -H "Content-Type: application/json" -d '{"repositories":[{"platform":"github","repository":"org/repo"}]}'
```

---

## Container Images

* **`koshin-proxy`**: `eclipse-temurin:25-jre-noble`, runs as UID `10001`.
* **`koshin-worker`**: `eclipse-temurin:25-jre-noble` with the worker, running as Renovate's UID `12021` (~140 MB compressed). It holds no Renovate: the same image's `koshin-runner-init` copies the Renovate runner (`/opt/koshin/{jre,lib,bin}`, JVM flags in `KOSHIN_RUNNER_JAVA_OPTS`) into a volume for the pod's `renovate` container.
* **Renovate** runs from the upstream `ghcr.io/renovatebot/renovate:<version>-full` image (Renovate plus its whole toolchain, `RENOVATE_BINARY_SOURCE=global`), pinned in the chart (`worker.renovate.image`), Kustomize and Compose and upgradable without a koshin release. Renovate (the bot) bumps it in one weekly, automerged PR.
  * That image is ~10 GB. In-cluster **[Spegel](https://github.com/spegel-org/spegel)** shares image layers between nodes P2P, so a new worker pod usually pulls from a peer instead of the registry; no koshin-specific config is needed.
* JVM flags come from `KOSHIN_JAVA_OPTS` (not `JAVA_OPTS`/`JAVA_TOOL_OPTIONS`, which Renovate's own Java/Gradle runs would inherit).

---

## Kubernetes Deployment (Helm)

The Helm chart in [`charts/koshin`](charts/koshin/README.md) is the primary install path, published as `oci://ghcr.io/hononeko/charts/koshin` with every release. It deploys the proxy, the KEDA-scaled workers and the sweeper, plus NATS (official `nats-io` chart) and Valkey (official `valkey-io` chart), or connects to existing ones (`external*` values). Every env contract variable is a value; secrets come from existing Secrets.

* **`minimal`** (default): one NATS and one coordination Valkey on `emptyDir`, and a separate package-cache Valkey.
* **`ha`** (`-f values-ha.yaml`): a 3-node NATS cluster on PVCs with streams at R3, Valkey primary + 2 replicas with AOF and 3 Sentinels, PodDisruptionBudgets and node spread.

The [chart README](charts/koshin/README.md) covers [choosing a profile](charts/koshin/README.md#choosing-a-profile), [sizing](charts/koshin/README.md#sizing), [backups and recovery](charts/koshin/README.md#backups-and-recovery) and [uninstalling](charts/koshin/README.md#uninstall).

```bash
helm install koshin oci://ghcr.io/hononeko/charts/koshin -n koshin --create-namespace \
  --set proxy.existingSecret=koshin-proxy --set worker.existingSecret=koshin-worker
```

## Kubernetes Deployment (Kustomize reference)

The Kustomize manifests stay as a small, dependency-free reference for the `minimal` profile; `ha` is Helm-only. `k8s/base` holds the proxy (2 replicas, `appProtocol: kubernetes.io/h2c`), the worker (no ServiceAccount token or RBAC), its KEDA `ScaledObject` and the 6-hourly sweeper `CronJob`. `k8s/overlays/minimal` adds the `koshin` namespace plus a single NATS, a coordination Valkey (`noeviction`) and a package-cache Valkey (`allkeys-lru`), all on `emptyDir`. Requires Kubernetes 1.30+ and [KEDA](https://keda.sh) 2.21+.

```bash
kubectl -n koshin create secret generic koshin-proxy-secrets \
  --from-literal=GITHUB_WEBHOOK_SECRET=... --from-literal=INTERNAL_TRIGGER_TOKEN=...
kubectl -n koshin create secret generic koshin-worker-secrets --from-literal=GITHUB_TOKEN=...
kubectl apply -k k8s/overlays/minimal
```

* **Repositories to sweep:** the `koshin-repositories` ConfigMap (`github:org/repo` per line), replaced from your overlay. Empty by default, so the sweeper enqueues nothing.
* **Autoscaling:** one `nats-jetstream` trigger per platform consumer, summed with `scalingModifiers` (`github + gitlab`, target `1`); KEDA counts pending plus in-flight jobs, so workers scale one-to-one with jobs across platforms up to `maxReplicaCount`. The worker creates both platform consumers so neither trigger errors.
* **Scale-in:** KEDA may remove a busy pod; the SIGTERM path NAKs its job for another worker.
* **`minimal` trade-off:** a NATS or Valkey restart loses pending jobs, locks, dirty flags and the package cache; proxy and worker recreate the streams and consumers on reconnect, and the sweeper re-enqueues every repository within 6 hours (after a NATS restart, those still marked queued only once the mark expires: `QUEUED_TTL_SECONDS`). NATS and Valkey are infrastructure, so they're the only stateful pieces; the Helm chart's `ha` profile gives them persistence.
* **Edge HTTP/3:** `k8s/examples/gateway-http3.yaml` is an Envoy Gateway example that terminates TLS + HTTP/3 and exposes only `/webhooks/`.

---

## HTTP/2 & HTTP/3 Transport Architecture (KSH-6 Spike Outcome)

`koshin-proxy` runs on Ktor 3.6.0 with the Netty engine (`io.ktor:ktor-server-netty`) and enables HTTP/2 and cleartext HTTP/2 (`h2c`) by default when `HTTP2_ENABLED=true`:

```kotlin
embeddedServer(
    factory = Netty,
    configure = {
        enableHttp2 = config.http2Enabled
        enableH2c = config.http2Enabled
        connector { port = config.port }
    },
    module = { ... },
)
```

* **HTTP/1.1 & Cleartext HTTP/2 (`h2c`):** With `enableHttp2 = true` and `enableH2c = true`, Netty serves standard HTTP/1.1 requests (used by direct GitHub/GitLab webhook deliveries and Kubernetes probe checks) alongside cleartext HTTP/2 via both HTTP/1.1 `Upgrade: h2c` negotiation (`101 Switching Protocols`) and HTTP/2 prior-knowledge connections.
* **TLS HTTP/2 (`h2`):** When a TLS connector is configured, ALPN negotiates `h2` automatically.
* **HTTP/3 (QUIC) Spike Finding & Chosen Architecture:**
  * Ktor 3.6.0's `NettyApplicationEngine.Configuration` natively exposes `enableHttp2` and `enableH2c`, but does **not** expose Netty's incubator QUIC / HTTP/3 codec pipeline (`netty-incubator-codec-http3` over UDP). Wiring raw UDP QUIC channels would bypass Ktor's Netty channel pipeline initializer and engine lifecycle.
  * **Chosen Option (a) — Edge QUIC Termination + In-Cluster `h2c`:** Terminate HTTP/3 (QUIC) and TLS at the Kubernetes Ingress / Gateway controller (e.g. Traefik or Envoy Gateway) and route in-cluster traffic to the `koshin-proxy` Service over cleartext HTTP/2 (`h2c`, annotated with `appProtocol: kubernetes.io/h2c`).

---

## Licensing & Distribution Notice

* **Source Code:** All original source code across `:core`, `:adapters:*`, `:proxy`, and `:worker` is licensed under the [Apache License, Version 2.0](LICENSE).
* **Container Images:** `koshin-proxy` and `koshin-worker` contain only koshin and a JRE, and are Apache-2.0 too. Renovate runs from the unmodified upstream `ghcr.io/renovatebot/renovate` image, which is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**; koshin doesn't bundle or redistribute it.
