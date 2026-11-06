# E2E Test Suite Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complement Koshin's existing test coverage with targeted, non-duplicative End-to-End integration tests covering the Trigger API, GitLab webhooks, credential-scope run-slot concurrency limits, and DLQ poison-pill recovery with queued-mark cleanup.

**Architecture:** Extend existing test harness fixtures (`TestProxy`, `E2eCluster`) with GitLab webhook signing, `/api/v1/trigger` client helpers, and configurable NATS delivery thresholds. Add four focused integration test suites in `:e2e` exercising these critical operational paths against real NATS JetStream and Valkey Testcontainers.

**Tech Stack:** Kotlin 2.3, JDK 25, JUnit 5, Ktor Server Netty & Client, NATS JetStream (`jnats`), Valkey (Lettuce), Testcontainers.

**Spec:** [docs/architecture.md](file:///home/vehkiya/projects/koshin/docs/architecture.md)

## Global Constraints

- Never duplicate flows already covered in `EndToEndIntegrationTest.kt`, `RateLimitIntegrationTest.kt`, `RepoCacheIntegrationTest.kt`, or `WorkerLifecycleIntegrationTest.kt`.
- All integration tests in `:e2e` must use Testcontainers and clean up resources (`@AfterEach { cluster.close() }`).
- Maintain Detekt clean code standards and strict compiler warnings (`allWarningsAsErrors = true`).
- Keep test timings small and bounded using monotonic clocks (`awaitUntil`, `awaitValue`, `awaitQuiescent`).

## Review Focus

- Unauthenticated or malformed requests to `/api/v1/trigger` or `/webhooks/gitlab` must be rejected with proper HTTP status codes without affecting broker or coordination state.
- Batch triggers with duplicate repository entries must coalesce in Valkey and only enqueue distinct jobs.
- Mid-run triggers on actively running repositories must mark the repository dirty and produce a single follow-up run with `TriggerReason.SCHEDULE`.
- Run slots must strictly enforce `maxRunsPerCredential` so multiple workers never exceed the configured concurrency cap for any scope.
- MaxDeliver delivery exhaustion must invoke `onDeadLettered` in proxy housekeeping to unblock Valkey's `:queued` key for future webhook deliveries.

---

### Task 1: Extend E2E Test Fixtures (`TestProxy` & `E2eCluster`)

**Files:**
- Modify: `e2e/src/test/kotlin/dev/hononeko/koshin/e2e/TestProxy.kt`
- Modify: `e2e/src/test/kotlin/dev/hononeko/koshin/e2e/E2eCluster.kt`

**Interfaces:**
- Produces:
  - `TestProxy.GITLAB_SECRET: String`
  - `TestProxy.TRIGGER_TOKEN: String`
  - `TestProxy.pushGitLab(repo: RepoRef, ref: String, username: String, token: String): JsonObject`
  - `TestProxy.trigger(repos: List<RepoRef>, token: String): JsonObject`
  - `TestProxy.triggerRaw(body: String, token: String?): HttpResponse<String>`
  - `E2eCluster(..., maxDeliver: Long, exhaustedSweepInterval: Duration)`

- [ ] **Step 1: Write test verifying new TestProxy and E2eCluster fixture capabilities**

Create a temporary check in a scratch test or run existing e2e suite to baseline.

- [ ] **Step 2: Update `TestProxy.kt`**

Add `GITLAB_SECRET = "e2e-gitlab-secret"` and `TRIGGER_TOKEN = "e2e-trigger-token"` constants. Configure `ProxyConfig` with:
- `platforms = platforms` (defaulting to `setOf(Platform.GITHUB, Platform.GITLAB)`)
- `gitlabWebhookSecret = GITLAB_SECRET`
- `internalTriggerToken = TRIGGER_TOKEN`

Implement `pushGitLab`, `trigger`, and `triggerRaw` helper methods.

- [ ] **Step 3: Update `E2eCluster.kt`**

Add `maxDeliver: Long = 5L` and `exhaustedSweepInterval: Duration = 60.seconds` parameters to `E2eCluster` constructor. Forward them to `NatsConfig`. Update `startProxy` to pass `platforms: Set<Platform> = setOf(Platform.GITHUB, Platform.GITLAB)`.

- [ ] **Step 4: Verify existing E2E tests pass**

Run: `./gradlew :e2e:integrationTest`
Expected: BUILD SUCCESSFUL (all 16 existing tests pass).

- [ ] **Step 5: Commit**

```bash
git add e2e/src/test/kotlin/dev/hononeko/koshin/e2e/TestProxy.kt e2e/src/test/kotlin/dev/hononeko/koshin/e2e/E2eCluster.kt
git commit -m "test(e2e): add GitLab and trigger helper methods to E2E fixtures"
```

---

### Task 2: Implement Trigger API E2E Integration Tests (`TriggerApiIntegrationTest`)

**Files:**
- Create: `e2e/src/test/kotlin/dev/hononeko/koshin/e2e/TriggerApiIntegrationTest.kt`

**Interfaces:**
- Consumes: `TestProxy.trigger`, `TestProxy.triggerRaw`, `E2eCluster`, `TestWorker`
- Produces: End-to-end verification of `/api/v1/trigger` sweep flows.

- [ ] **Step 1: Write the failing tests in `TriggerApiIntegrationTest.kt`**

Cover:
1. `unauthorized trigger requests return 401 and enqueue nothing`: invalid/missing Bearer token returns 401, published count is 0.
2. `batch trigger deduplicates within batch and workers execute scheduled jobs`: trigger with `[repoA, repoB, repoA]` reports `enqueued = 2, coalesced = 1`. A worker runs both jobs with `TriggerReason.SCHEDULE`.
3. `triggering a running repository marks it dirty and schedules a single follow-up run`: while worker 1 is running `repoA`, a trigger arrives; it reports `coalesced = 1`, Valkey `:dirty` is set, and upon completing the first run, the worker executes a single follow-up scheduled run.

- [ ] **Step 2: Run test to verify it compiles and runs**

Run: `./gradlew :e2e:integrationTest --tests "*TriggerApiIntegrationTest*"`
Expected: Tests run against Testcontainers and PASS.

- [ ] **Step 3: Commit**

```bash
git add e2e/src/test/kotlin/dev/hononeko/koshin/e2e/TriggerApiIntegrationTest.kt
git commit -m "test(e2e): add end-to-end integration tests for /api/v1/trigger sweep API"
```

---

### Task 3: Implement GitLab Webhook E2E Integration Tests (`GitLabWebhookIntegrationTest`)

**Files:**
- Create: `e2e/src/test/kotlin/dev/hononeko/koshin/e2e/GitLabWebhookIntegrationTest.kt`

**Interfaces:**
- Consumes: `TestProxy.pushGitLab`, `E2eCluster`, `TestWorker`
- Produces: End-to-end verification of `/webhooks/gitlab` pipeline.

- [ ] **Step 1: Write the failing tests in `GitLabWebhookIntegrationTest.kt`**

Cover:
1. `GitLab push webhook validates token and is executed end-to-end by multi-platform worker`: valid `Push Hook` triggers worker configured for `setOf(Platform.GITHUB, Platform.GITLAB)`, executes Renovate CLI for `RepoRef(Platform.GITLAB, "gitlab-org/project")`, stores cache in GitLab Object Store partition, and ACKs.
2. `GitLab push webhook with invalid token returns 403 Forbidden`: invalid `X-Gitlab-Token` returns 403 and nothing is published.
3. `GitLab branch deletion and bot pushes are ignored without publishing`: pushes to non-default branch or by bot user return 202 `enqueued = false`, leaving NATS empty.

- [ ] **Step 2: Run test to verify it passes**

Run: `./gradlew :e2e:integrationTest --tests "*GitLabWebhookIntegrationTest*"`
Expected: Tests run against Testcontainers and PASS.

- [ ] **Step 3: Commit**

```bash
git add e2e/src/test/kotlin/dev/hononeko/koshin/e2e/GitLabWebhookIntegrationTest.kt
git commit -m "test(e2e): add end-to-end integration tests for GitLab webhooks"
```

---

### Task 4: Implement Run-Slot Concurrency E2E Integration Tests (`RunSlotConcurrencyIntegrationTest`)

**Files:**
- Create: `e2e/src/test/kotlin/dev/hononeko/koshin/e2e/RunSlotConcurrencyIntegrationTest.kt`

**Interfaces:**
- Consumes: `PlatformThrottleConfig(maxRunsPerCredential = 1)`, `E2eCluster`, `TestWorker`
- Produces: Verification of run slot capping across concurrent workers.

- [ ] **Step 1: Write the tests in `RunSlotConcurrencyIntegrationTest.kt`**

Cover:
1. `two workers on the same credential scope never run concurrently when maxRunsPerCredential is 1`:
   - Initialize cluster with `throttle = PlatformThrottleConfig(maxRunsPerCredential = 1)`.
   - Publish 2 jobs for `repo1` and `repo2` under the same credential scope.
   - Start `worker1` (with `cli.hold()`) and `worker2`.
   - Await `worker1` to start running `repo1`.
   - Assert `worker2` has NOT started `repo2` (it cannot acquire the run slot).
   - Release `worker1.cli`.
   - Verify `worker2` acquires the slot only after `worker1` finishes, and both jobs complete with no concurrent overlap.
2. `different credential scopes run concurrently`:
   - Publish a job for installation 100 and a job for installation 200.
   - Both workers run their respective jobs simultaneously.

- [ ] **Step 2: Run test to verify it passes**

Run: `./gradlew :e2e:integrationTest --tests "*RunSlotConcurrencyIntegrationTest*"`
Expected: Tests run against Testcontainers and PASS.

- [ ] **Step 3: Commit**

```bash
git add e2e/src/test/kotlin/dev/hononeko/koshin/e2e/RunSlotConcurrencyIntegrationTest.kt
git commit -m "test(e2e): add end-to-end integration tests for run-slot concurrency limiting"
```

---

### Task 5: Implement MaxDeliver Poison-Pill Recovery E2E Integration Tests (`DlqRecoveryIntegrationTest`)

**Files:**
- Create: `e2e/src/test/kotlin/dev/hononeko/koshin/e2e/DlqRecoveryIntegrationTest.kt`

**Interfaces:**
- Consumes: `E2eCluster(maxDeliver = 2L, exhaustedSweepInterval = 100.milliseconds)`, `TestProxy`, `TestWorker`
- Produces: Verification of poison-pill handling and recovery of Valkey `:queued` state.

- [ ] **Step 1: Write the test in `DlqRecoveryIntegrationTest.kt`**

Cover:
1. `an exhausted delivery is moved to DLQ by proxy housekeeping and clears Valkey queued mark so new webhooks can enqueue`:
   - Start cluster with `maxDeliver = 2L` and `exhaustedSweepInterval = 200.milliseconds`.
   - Start proxy.
   - Push webhook for `repo`. Proxy responds `enqueued = true`.
   - Worker 1 picks up job and crashes immediately without settling.
   - Lock expires, job is redelivered.
   - Worker 2 picks up redelivery and crashes immediately.
   - With delivery attempt reaching `maxDeliver` (2), proxy housekeeping moves the message to `E2E_DLQ` and invokes `onDeadLettered`.
   - Verify dead letter message exists in DLQ.
   - Await until `cluster.isQueued(repo)` becomes false.
   - Send another push webhook for `repo`.
   - Assert proxy responds `enqueued = true` (not coalesced/blocked).
   - Start a healthy worker to execute the new job to completion.

- [ ] **Step 2: Run test to verify it passes**

Run: `./gradlew :e2e:integrationTest --tests "*DlqRecoveryIntegrationTest*"`
Expected: Tests run against Testcontainers and PASS.

- [ ] **Step 3: Run full verification across the entire project**

Run: `./gradlew check integrationTest`
Expected: All Detekt checks, unit tests, and integration tests pass cleanly.

- [ ] **Step 4: Commit**

```bash
git add e2e/src/test/kotlin/dev/hononeko/koshin/e2e/DlqRecoveryIntegrationTest.kt
git commit -m "test(e2e): add end-to-end integration test for MaxDeliver DLQ recovery"
```
