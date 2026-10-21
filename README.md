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
* **`:worker`**: Autonomous coroutine daemon that pulls jobs from NATS, acquires repo mutexes, enforces LRU workspace cleanup, runs Renovate CLI subprocesses, and handles heartbeats & dirty catch-ups.

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

## Licensing & Distribution Notice

* **Source Code:** All original source code across `:core`, `:adapters:*`, `:proxy`, and `:worker` is licensed under the [Apache License, Version 2.0](LICENSE).
* **Distributed Worker Image Notice (AGPL-3.0):** The container image for `koshin-worker` bundles and executes the upstream Mend Renovate CLI (`renovate-cli`), which is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**. When building or distributing container images containing Renovate CLI, the resulting distributed artifacts are governed by the AGPL-3.0 license terms (`LABEL org.opencontainers.image.licenses="AGPL-3.0-only"`).
