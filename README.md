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
* **`:adapters:github-app`**: GitHub App authentication, JWT signing, and installation access token rotation.
* **`:proxy`**: Stateless Ktor HTTP server receiving webhooks (GitHub/GitLab) and internal cron trigger schedules.
* **`:worker`**: Autonomous coroutine daemon that pulls jobs from NATS, acquires repo mutexes, runs Renovate CLI subprocesses, deletes each repo workspace after the run, and handles heartbeats & dirty catch-ups.

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

The coordination Valkey instance (`VALKEY_URL`, supporting both standalone `redis://` and Sentinel `redis-sentinel://` URIs) stores per-repository mutexes (`koshin:{<platform>:<repo>}:lock`) and dirty flags (`koshin:{<platform>:<repo>}:dirty`) with explicit TTLs (`EX`).

* **Minimum Server Version:** `releaseLockAndTakeDirty` uses the `GETDEL` command inside Lua, requiring **Valkey 7.2+** (or **Redis 6.2+**).
* **`maxmemory-policy noeviction` (Required):** You **must** configure `maxmemory-policy noeviction` on the coordination Valkey instance. Because repository locks and dirty flags are TTL-bearing keys, any `volatile-*` eviction policy (`volatile-lru`, `volatile-ttl`, `volatile-random`, `volatile-lfu`) or `allkeys-*` policy under memory pressure would evict active locks or pending dirty flags first, breaking mutual exclusion and coalescing guarantees.
  * In the **`minimal`** deployment profile, where a single Valkey instance is shared between coordination (`VALKEY_URL`) and Renovate's package cache (`RENOVATE_REDIS_URL`), size `maxmemory` with enough headroom for both coordination keys and Renovate's cache so `noeviction` does not reject lock writes with `OOM`.
  * In the **`ha`** deployment profile, Renovate's package cache runs on a dedicated Valkey instance where `allkeys-lru` is safe.
* **Optional Replica Acknowledgment (`LOCK_REPLICA_ACK_TIMEOUT_MS`):** When set to a positive value (in milliseconds), `ValkeyRepoCoordinationAdapter` issues `WAIT 1 <ms>` after acquiring a repository lock to narrow the replication failover window in primary-replica (`ha`) topologies. Keep this timeout small (e.g., `20`–`100` ms) and **only enable it in the `ha` profile**; on a single-node `minimal` deployment with zero replicas, `WAIT 1 <ms>` blocks the connection for the full timeout on every lock acquisition.

---

## Worker Runtime (KSH-7)

* **1 job per pod.** The daemon polls the enabled `PLATFORMS` and runs one `renovate <repository>` at a time. Each run gets `RENOVATE_PLATFORM`, `RENOVATE_ENDPOINT` and `RENOVATE_TOKEN` for the job's platform (`GITHUB_TOKEN`/`GITHUB_API_URL`, `GITLAB_TOKEN`/`GITLAB_ENDPOINT`), plus `LOG_FORMAT=json`, `RENOVATE_BASE_DIR`, `RENOVATE_BINARY_SOURCE`, `RENOVATE_REDIS_URL` and `RENOVATE_REPOSITORY_CACHE=enabled`. Other `RENOVATE_*` variables on the worker are passed through; worker secrets (`GITHUB_TOKEN`, `GITLAB_TOKEN`, `VALKEY_URL`, …) are not.
* **Timeouts and cancellation** (`CLI_TIMEOUT_MINUTES`, lost lock, shutdown) forcibly kill the whole CLI process tree.
* **Workspaces:** `$RENOVATE_BASE_DIR/repos/<platform>/<repo>` is deleted after every run; everything under `$RENOVATE_BASE_DIR` is purged at startup.
* **SIGTERM:** stops fetching, kills the CLI, releases the lock (keeping the dirty flag), NAKs the delivery and releases the run slot, without waiting for the run to finish.
* **Pod deletion cost (optional):** with `POD_DELETION_COST_ENABLED=true` (plus `POD_NAME` / `POD_NAMESPACE` from the downward API), the worker sets `controller.kubernetes.io/pod-deletion-cost` to `1000` while Renovate runs and back to `0` afterwards. Failed patches are logged and ignored.
* **Health:** `GET /healthz` is liveness only; `GET /readyz` checks NATS and Valkey.
* Run-slot throttling (KSH-11) and the shared repository cache (KSH-10) are no-ops for now.

---

## Running Locally (Docker Compose)

`docker compose up --build` starts NATS (JetStream), Valkey, the proxy (`localhost:8080`) and a worker (`localhost:8081`), matching the `minimal` profile. Export `GITHUB_TOKEN` (or `GITLAB_TOKEN`) first so runs can authenticate, then trigger one:

```bash
curl -X POST localhost:8080/api/v1/trigger -H "Authorization: Bearer dev-trigger-token" \
  -H "Content-Type: application/json" -d '{"repositories":[{"platform":"github","repository":"org/repo"}]}'
```

---

## Container Images

* **`koshin-proxy`**: `eclipse-temurin:25-jre-noble`, runs as UID `10001`.
* **`koshin-worker`**: the pinned `ghcr.io/renovatebot/renovate:<version>-full` image (Renovate plus its whole toolchain, `RENOVATE_BINARY_SOURCE=global`) with a private Temurin 25 JRE under `/opt/koshin/jre`. Runs as Renovate's UID `12021`; `dumb-init` is PID 1 and reaps orphaned processes.
  * The image is ~10 GB. In-cluster **[Spegel](https://github.com/spegel-org/spegel)** shares image layers between nodes P2P, so a new worker pod usually pulls from a peer instead of the registry; no koshin-specific config is needed.
* JVM flags come from `KOSHIN_JAVA_OPTS` (not `JAVA_OPTS`/`JAVA_TOOL_OPTIONS`, which Renovate's own Java/Gradle runs would inherit).

---

## Kubernetes Deployment (Kustomize)

`k8s/base` holds the proxy (2 replicas, `appProtocol: kubernetes.io/h2c`), the worker, its KEDA `ScaledObject`, the 6-hourly sweeper `CronJob`, and the worker's RBAC. `k8s/overlays/minimal` adds the `koshin` namespace plus a single NATS and a single Valkey on `emptyDir`. Requires [KEDA](https://keda.sh) 2.21+.

```bash
kubectl -n koshin create secret generic koshin-proxy-secrets \
  --from-literal=GITHUB_WEBHOOK_SECRET=... --from-literal=INTERNAL_TRIGGER_TOKEN=...
kubectl -n koshin create secret generic koshin-worker-secrets --from-literal=GITHUB_TOKEN=...
kubectl apply -k k8s/overlays/minimal
```

* **Repositories to sweep:** the `koshin-repositories` ConfigMap (`github:org/repo` per line), replaced from your overlay. Empty by default, so the sweeper enqueues nothing.
* **Autoscaling:** one `nats-jetstream` trigger per platform consumer with `lagThreshold: "1"`; KEDA counts pending plus in-flight jobs, so workers scale one-to-one with jobs up to `maxReplicaCount`. The worker creates both platform consumers so neither trigger errors (an erroring metric blocks HPA scale-down).
* **Scale-in:** workers annotate their own pod with `controller.kubernetes.io/pod-deletion-cost` (`1000` while running), so idle pods are removed first. If a busy pod is still picked, the SIGTERM path NAKs its job for another worker.
* **`minimal` trade-off:** a NATS or Valkey restart loses pending jobs, locks, dirty flags and the package cache; the sweeper re-enqueues every repository within 6 hours. NATS and Valkey are infrastructure, so they're the only stateful pieces; the `ha` profile (KSH-12) gives them persistence.
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
* **Distributed Worker Image Notice (AGPL-3.0):** The container image for `koshin-worker` bundles and executes the upstream Mend Renovate CLI (`renovate-cli`), which is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**. When building or distributing container images containing Renovate CLI, the resulting distributed artifacts are governed by the AGPL-3.0 license terms (`LABEL org.opencontainers.image.licenses="AGPL-3.0-only"`).
