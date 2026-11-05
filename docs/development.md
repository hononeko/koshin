# Development

## Tech stack

* **JDK 25** (`jvmToolchain(25)`), **Kotlin 2.3+**, **Gradle 9.4+** (wrapper provided)
* **Ktor 3.x** on Netty, Kotlin Coroutines, `kotlinx.serialization`
* **NATS JetStream** (`jnats`) and **Valkey** (Lettuce)
* **Detekt 2.0** with ktlint-wrapper rules, **Kover 0.9+** coverage
* JUnit 5, MockK and Testcontainers for tests

## Modules

koshin follows Hexagonal Architecture (Ports and Adapters):

* **`:core`**: domain models, use cases and ports, with no dependency on infrastructure frameworks.
* **`:adapters:nats`**: NATS JetStream `WorkQueue` publisher/consumer, DLQ housekeeping and the Object Store repository
  cache.
* **`:adapters:state-valkey`**: per-repository locks, dirty-flag coalescing, run slots, cooldowns and leases in Valkey.
* **`:adapters:github-app`**: GitHub App authentication (JWT signing, installation tokens), bot identity and repository
  discovery.
* **`:proxy`**: Ktor server receiving webhooks (GitHub/GitLab) and triggers, NATS housekeeping and the worker
  dispatcher.
* **`:worker`**: coroutine daemon that pulls jobs from NATS, takes repo locks, hands Renovate runs to the `renovate`
  container and cleans up after each run.
* **`:e2e`**: the proxy and two workers against real NATS and Valkey.

[Architecture](architecture.md) has the ports, protocols and failure handling in detail.

## Build and test

```bash
./gradlew build             # compile, test and check every module
./gradlew check             # Detekt (type resolution, ktlint rules), unit tests and Kover verification
./gradlew integrationTest   # Testcontainers suites (needs Docker), including e2e/
./gradlew koverHtmlReport koverXmlReport   # coverage, HTML at build/reports/kover/html/index.html
./gradlew detekt            # static analysis only
```

The chart README is generated with [helm-docs](https://github.com/norwoodj/helm-docs) from
`charts/koshin/README.md.gotmpl` and the comments in `values.yaml`; CI fails if it's out of date:

```bash
docker run --rm -v "$PWD/charts:/helm-docs" jnorwood/helm-docs:v1.14.2 --chart-search-root /helm-docs
```

## Running locally

`docker compose up --build` starts NATS (JetStream), the coordination and cache Valkeys, the proxy (`localhost:8080`)
and a worker (`localhost:8081`) with its Renovate runner (`renovate-init`, then the upstream Renovate image), matching
the `minimal` profile. Export `GITHUB_TOKEN` (or `GITLAB_TOKEN`) first so runs can authenticate, then trigger one:

```bash
curl -X POST localhost:8080/api/v1/trigger -H "Authorization: Bearer dev-trigger-token" \
  -H "Content-Type: application/json" -d '{"repositories":[{"platform":"github","repository":"org/repo"}]}'
```

The Compose setup doesn't dispatch workers (`WORKER_DISPATCHER_ENABLED=false`): its one worker runs until stopped.
