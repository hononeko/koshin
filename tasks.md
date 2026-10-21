# Implementation Backlog: `hononeko/koshin` (`KSH`)

**Kaneo Workspace:** `Kerrlab` (`PJFZjT1Bj8roDFQgSMPU9OyVu1YzoHcO`)  
**Kaneo Project:** `Koshin` (`KSH` — ID: `t8377qy5b6xpr8z16zectp8k`)

---

## Build Order Summary

1. **KSH-1:** [EPIC / DOC] System Architecture v3.0 & Global Conventions
2. **KSH-2:** Bootstrap Multi-Module Gradle 9.4+ Project (Kotlin 2.3 / JDK 25)
3. **KSH-3:** Implement `:core` Hexagonal Ports, Domain Models & Use Cases
4. **KSH-4:** Implement `:adapters:nats` (NATS JetStream WorkQueue Adapter & Repo Cache)
5. **KSH-5:** Implement `:adapters:state-valkey` (Valkey Mutex & Dirty-Flag Adapter)
6. **KSH-6:** Implement `:proxy` (Stateless Ktor Webhook Receiver & Scheduled Trigger API)
7. **KSH-7:** Implement `:worker` (Autonomous Coroutine Daemon, LRU Disk Manager & CLI Runner)
8. **KSH-8:** Containerization, Kubernetes Kustomize Manifests & KEDA Autoscaler
9. **KSH-9:** End-to-End Integration Test Suite (Testcontainers: NATS + Valkey)

---

## Detailed Ticket Specifications

### `KSH-1` — [EPIC / DOC] System Architecture v3.0 & Global Conventions
* **Status:** `in-progress`
* **Priority:** `high`
* **Summary:** Master reference ticket containing the `architecture.md` specification, hexagonal module boundaries, environment variable contracts, and Dirty-Flag coalescing state machine for AI coding agents and contributors.

---

### `KSH-2` — Bootstrap Multi-Module Gradle 9.4+ Project (Kotlin 2.3 / JDK 25)
* **Status:** `done`
* **Priority:** `urgent`
* **Dependencies:** None
* **Scope:**
  1. Initialize Gradle 9.4+ wrapper and `settings.gradle.kts` declaring modules:
     * `:core`
     * `:adapters:nats` (JetStream queue + Object Store repo cache; formerly `:adapters:queue-nats`)
     * `:adapters:state-valkey`
     * `:adapters:github-app`
     * `:proxy`
     * `:worker`
  2. Create `gradle/libs.versions.toml` with Kotlin `2.3.x`, `jvmToolchain(25)`, Ktor `3.x`, `kotlinx-coroutines`, `kotlinx-serialization-json`, `io.nats:jnats`, `io.lettuce:lettuce-core`, `logback-classic`, `junit-jupiter`, `mockk`, and `testcontainers`.
  3. Add Apache-2.0 `LICENSE` at the repository root and document the AGPL-3.0 notice for the distributed worker image in `README.md`.
* **Acceptance Criteria:**
  * `./gradlew build` compiles cleanly on JDK 25 with zero warnings across all empty modules.

---

### `KSH-3` — Implement `:core` Hexagonal Ports, Domain Models & Use Cases
* **Status:** `to-do`
* **Priority:** `high`
* **Dependencies:** `KSH-2`
* **Scope:**
  1. Define serializable domain models: `RenovateJobEvent`, `TriggerReason`, `JobExecutionResult`.
  2. Define outbound/inbound ports:
     * `JobQueuePublisher` (`publish(event)`)
     * `JobQueueConsumer` (`fetchNext(timeout)`) & `JobDelivery` (`ack()`, `nack(delay)`, `heartbeat()`)
     * `RepoCoordinationPort` (`tryAcquireLock`, `extendLock`, `markDirty`, `releaseLockAndTakeDirty`)
     * `CliExecutorPort` (`execute(repository, timeout)`)
     * `WorkspaceCleanerPort` (`ensureDiskCapacity()`)
  3. Implement `IngestWebhookUseCase` (validates repository format and publishes `RenovateJobEvent`).
  4. Implement `ProcessRenovateJobUseCase` encapsulating the full worker choreography:
     * Acquire lock -> if locked, `markDirty` + `ack()` and return.
     * Launch concurrent heartbeat coroutine (`delay(heartbeatInterval)` -> `delivery.heartbeat()` + `extendLock()`).
     * Run `WorkspaceCleanerPort.ensureDiskCapacity()` and `CliExecutorPort.execute()`.
     * In `finally`: cancel heartbeat, call `releaseLockAndTakeDirty()`, publish `DIRTY_FOLLOWUP` if `true`, and `delivery.ack()`.
* **Acceptance Criteria:**
  * `:core` has zero dependencies on Ktor, NATS, or Lettuce.
  * 100% unit test coverage of `ProcessRenovateJobUseCase` (including lock collision, dirty follow-up re-queueing, and CLI timeout recovery) using MockK / in-memory fakes.

---

### `KSH-4` — Implement `:adapters:nats` (NATS JetStream WorkQueue Adapter & Repo Cache)
* **Status:** `to-do`
* **Priority:** `high`
* **Dependencies:** `KSH-3`
* **Scope:**
  1. Implement stream bootstrapper (`NatsJetStreamBootstrap`) that idempotently creates or updates the `RENOVATE_JOBS` stream with:
     * Subjects: `renovate.jobs.>`
     * Retention Policy: `RetentionPolicy.WorkQueue`
     * Storage Type: `StorageType.File`
     * Duplicate Window: `2 minutes`
  2. Implement `NatsJobQueuePublisher`:
     * Maps `hononeko/koshin` to safe subject `renovate.jobs.hononeko.koshin`.
     * Sets `Nats-Msg-Id` header (`${event.repository}:${event.deliveryId}`) for broker-level deduplication.
  3. Implement `NatsJobQueueConsumer`:
     * Binds to durable pull consumer `koshin-workers` (`AckPolicy.Explicit`, `AckWait = 60s`, `MaxDeliver = 5`).
     * Maps `Message.ack()`, `Message.nakWithDelay()`, and `Message.inProgress()` to `JobDelivery`.
* **Acceptance Criteria:**
  * Verified via unit/integration tests that `inProgress()` resets the broker `AckWait` timer and `ack()` removes the message from the `WorkQueue` stream.

---

### `KSH-5` — Implement `:adapters:state-valkey` (Valkey Mutex & Dirty-Flag Adapter)
* **Status:** `to-do`
* **Priority:** `high`
* **Dependencies:** `KSH-3`
* **Scope:**
  1. Implement `ValkeyRepoCoordinationAdapter` using Lettuce's `RedisCoroutinesCommands`.
  2. Implement `tryAcquireLock`: `SET koshin:lock:<repo> <workerId> NX EX <ttlSeconds>`.
  3. Implement `extendLock`: Lua script or `EXPIRE koshin:lock:<repo> <ttlSeconds>` verifying lock ownership.
  4. Implement `markDirty`: `SETEX koshin:dirty:<repo> 3600 "1"`.
  5. Implement `releaseLockAndTakeDirty`: Atomic Lua script that deletes `koshin:lock:<repo>` and executes `GETDEL koshin:dirty:<repo>`, returning `true` if the dirty key was present.
* **Acceptance Criteria:**
  * Concurrency test verifying 10 coroutines competing for the same repo result in 1 lock winner, 9 dirty flags coalesced, and exactly 1 dirty follow-up signal on release.

---

### `KSH-6` — Implement `:proxy` (Stateless Ktor Webhook Receiver & Scheduled Trigger API)
* **Status:** `to-do`
* **Priority:** `medium`
* **Dependencies:** `KSH-3`, `KSH-4`
* **Scope:**
  1. Bootstrap Ktor 3.x `CIO` server with `ContentNegotiation` (`kotlinx.serialization`) and structured JSON logging.
  2. Implement constant-time `HmacSignatureValidator` (`HmacSHA256`) verifying GitHub's `X-Hub-Signature-256` header and GitLab's `X-Gitlab-Token`.
  3. Expose routes:
     * `POST /webhooks/github`: Filters relevant events (`push`, `pull_request`, `issues` checkbox edits), extracts `repository.full_name`, invokes `IngestWebhookUseCase`.
     * `POST /webhooks/gitlab`: Extracts `project.path_with_namespace`, invokes `IngestWebhookUseCase`.
     * `POST /api/v1/trigger`: Protected by `Authorization: Bearer <INTERNAL_TRIGGER_TOKEN>`; accepts `{"repositories": ["org/repo1", ...]}` from the scheduled CronJob and fans out `SCHEDULE` events.
     * `GET /healthz`: Checks NATS connection status and returns `200 OK`.
* **Acceptance Criteria:**
  * `testApplication` tests verifying valid HMAC returns `202 Accepted`, invalid HMAC returns `403 Forbidden`, and `/api/v1/trigger` queues batch repositories.

---

### `KSH-7` — Implement `:worker` (Autonomous Coroutine Daemon, LRU Disk Manager & CLI Runner)
* **Status:** `to-do`
* **Priority:** `high`
* **Dependencies:** `KSH-3`, `KSH-4`, `KSH-5`
* **Scope:**
  1. Implement `LruWorkspaceCleaner` (`WorkspaceCleanerPort`):
     * Inspects `FileStore` usage of `RENOVATE_BASE_DIR` (`/tmp/renovate`).
     * When usage `>= DISK_HIGH_WATERMARK_PCT` (85%), walks `$RENOVATE_BASE_DIR/repos` depth-3 (`<platform>/<org>/<repo>`), sorts directories by `Files.getLastModifiedTime` ascending, and deletes oldest repository clones recursively until usage `< DISK_LOW_WATERMARK_PCT` (60%).
  2. Implement `RenovateProcessExecutor` (`CliExecutorPort`):
     * Invokes `renovate <repository>` via `ProcessBuilder`.
     * Passes environment variables: `RENOVATE_BASE_DIR`, `RENOVATE_BINARYSOURCE=install`, `RENOVATE_REDIS_URL`, `RENOVATE_REPOSITORY_CACHE=enabled`, `RENOVATE_TOKEN`.
     * Streams stdout/stderr asynchronously and enforces `withTimeoutOrNull(cliTimeout)` calling `process.destroyForcibly()` on timeout.
  3. Wire graceful shutdown (`Runtime.getRuntime().addShutdownHook` / Ktor `ApplicationStopped`) so in-flight jobs finish or cleanly NACK on `SIGTERM`.
  4. Expose lightweight `GET /healthz` endpoint on port `8080`.
* **Acceptance Criteria:**
  * Unit tests for `LruWorkspaceCleaner` using temporary directories with synthetic file sizes and timestamps.
  * Subprocess timeout test verifying hung processes are forcibly killed and cleaned up.

---

### `KSH-8` — Containerization, Kubernetes Kustomize Manifests & KEDA Autoscaler
* **Status:** `to-do`
* **Priority:** `medium`
* **Dependencies:** `KSH-6`, `KSH-7`
* **Scope:**
  1. `docker/Dockerfile.proxy`: Multi-stage build using `eclipse-temurin:25-jdk-noble` (builder) -> `eclipse-temurin:25-jre-noble` (runtime).
  2. `docker/Dockerfile.worker`: Multi-stage build copying the fat JAR and JDK 25 JRE into `ghcr.io/renovatebot/renovate:full` (with `LABEL org.opencontainers.image.licenses="AGPL-3.0-only"`).
  3. `k8s/base/`:
     * `proxy-deployment.yaml` (2 replicas, stateless) & `proxy-service.yaml`.
     * `worker-deployment.yaml` (min 1 replica, stateless, `emptyDir` mounted at `/tmp/renovate`).
     * `worker-scaledobject.yaml` (KEDA `ScaledObject` with `nats-jetstream` trigger monitoring consumer `koshin-workers` on stream `RENOVATE_JOBS`, `lagThreshold: "3"`, `minReplicaCount: 1`, `maxReplicaCount: 10`).
     * `cronjob-sweeper.yaml` (Runs every 6 hours to trigger scheduled scans).
  4. `docker-compose.yml` for local one-command testing (NATS JetStream + Valkey + Proxy + Worker).
* **Acceptance Criteria:**
  * Both Docker images build cleanly and pass `docker compose up` smoke tests.

---

### `KSH-9` — End-to-End Integration Test Suite (Testcontainers: NATS + Valkey)
* **Status:** `to-do`
* **Priority:** `medium`
* **Dependencies:** `KSH-4`, `KSH-5`, `KSH-6`, `KSH-7`
* **Scope:**
  1. Spin up `nats:2.10-alpine` (with `-js` JetStream enabled) and `valkey/valkey:8-alpine` via Testcontainers.
  2. Run an end-to-end test:
     * Send 5 rapid signed GitHub webhooks for `hononeko/koshin` to the Ktor Proxy while a simulated 2-second Renovate CLI run is executing on Worker 1.
     * Assert that Worker 2 coalesces the 4 mid-flight webhooks via the Valkey dirty flag and that exactly **2** total CLI runs occur (initial run + 1 coalesced follow-up).
  3. Simulate a worker crash (canceling the worker coroutine mid-run without releasing the lock) and verify that after the short TTL/AckWait window, the second worker picks up and completes the job.
* **Acceptance Criteria:**
  * `./gradlew integrationTest` passes reliably in CI and locally.