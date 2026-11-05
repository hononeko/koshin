# koshin

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.3+-purple.svg)](https://kotlinlang.org)
[![JDK](https://img.shields.io/badge/JDK-25-orange.svg)](https://adoptium.net)
[![Gradle](https://img.shields.io/badge/Gradle-9.4+-green.svg)](https://gradle.org)

Stateless, scale-to-zero Renovate CLI orchestrator built with Kotlin, Ktor, NATS JetStream and Valkey: a proxy coalesces
webhooks and starts worker Jobs on demand, without an autoscaler.

## How it works

1. **The proxy** receives GitHub and GitLab webhooks and sweeper triggers. It drops duplicates for a repository that's
   already queued, and marks one that's running for a single follow-up run, before publishing to NATS JetStream.
2. **It starts workers on demand** as Kubernetes `Job`s from a `PodTemplate`. With an empty queue no worker runs.
3. **Workers** take a per-repository lock in Valkey, restore Renovate's repository cache and run Renovate in a separate,
   secret-free `renovate` container (the upstream Renovate image). They run jobs back to back and exit once idle.

Along the way koshin:

* serves many tenants from one public **GitHub App**, with an allow-list of accounts;
* caps runs **per credential** (each App installation, or a platform token) and backs off only the scope that hit a
  rate limit;
* keeps **encrypted registry credentials** in tenants' own Renovate configs, decryptable only for their repositories;
* empties caches and kills leftover processes after every run, and scrubs tokens from its logs.

## Quick start

Install the Helm chart (`minimal` profile) after creating the proxy and worker Secrets:

```bash
helm install koshin oci://ghcr.io/hononeko/charts/koshin -n koshin --create-namespace \
  --set proxy.existingSecret=koshin-proxy --set worker.existingSecret=koshin-worker
```

The [chart README](charts/koshin/README.md) covers the Secrets, the `ha` profile and every value. To try koshin
locally, run `docker compose up --build` (see [Development](docs/development.md#running-locally)).

## Documentation

| Guide | Covers |
| :--- | :--- |
| [Deployment](docs/deployment.md) | Helm and Kustomize, Valkey requirements, container images, HTTP/2 and HTTP/3 |
| [Configuration](docs/configuration.md) | Every environment variable of the proxy and the worker |
| [GitHub App](docs/github-app.md) | Registering the App, public Apps and allowed accounts |
| [Encrypted registry credentials](docs/registry-credentials.md) | koshin's OpenPGP key for operators, encrypting secrets for tenants |
| [Workers](docs/workers.md) | Worker runtime, isolation and dispatch |
| [Caching](docs/caching.md) | Repository and package caches, tenant isolation |
| [Rate limiting](docs/rate-limiting.md) | Credential scopes, run slots, cooldowns and sizing |
| [Logging](docs/logging.md) | Log format, token scrubbing and levels |
| [Architecture](docs/architecture.md) | Design, ports, coalescing protocol and failure handling |
| [Development](docs/development.md) | Modules, building, testing and running locally |

## License

koshin's source code is licensed under the [Apache License, Version 2.0](LICENSE), and so are the `koshin-proxy` and
`koshin-worker` images, which contain only koshin and a JRE. Renovate runs from the unmodified upstream
`ghcr.io/renovatebot/renovate` image, licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**; koshin
doesn't bundle or redistribute it.
