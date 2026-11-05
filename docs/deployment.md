# Deployment

koshin runs on vanilla Kubernetes 1.30+ (native sidecar containers) with no operator or autoscaler. NATS and Valkey
are its only stateful pieces.

## Helm (recommended)

The chart in [`charts/koshin`](../charts/koshin/README.md) is the primary install path, published as
`oci://ghcr.io/hononeko/charts/koshin` with every release. It deploys the proxy, the worker `PodTemplate` the proxy
starts burst worker Jobs from (plus one warm worker in `ha`) and the sweeper, plus NATS (official `nats-io` chart) and
Valkey (official `valkey-io` chart), or connects to existing ones (`external*` values). Every
[environment variable](configuration.md) is a value; secrets come from existing Secrets.

* **`minimal`** (default): one NATS and one coordination Valkey on `emptyDir`, a separate package-cache Valkey, and no
  worker while the queue is empty.
* **`ha`** (`-f values-ha.yaml`): a 3-node NATS cluster on PVCs with streams at R3, Valkey primary + 2 replicas with
  AOF and 3 Sentinels, PodDisruptionBudgets, node spread, and one always-warm worker.

```bash
helm install koshin oci://ghcr.io/hononeko/charts/koshin -n koshin --create-namespace \
  --set proxy.existingSecret=koshin-proxy --set worker.existingSecret=koshin-worker
```

The [chart README](../charts/koshin/README.md) covers installing,
[choosing a profile](../charts/koshin/README.md#choosing-a-profile), [workers](../charts/koshin/README.md#workers),
[sizing](../charts/koshin/README.md#sizing), [backups and recovery](../charts/koshin/README.md#backups-and-recovery)
and [uninstalling](../charts/koshin/README.md#uninstall), and lists every value.

## Kustomize (reference)

The Kustomize manifests stay as a small, dependency-free reference for the `minimal` profile; `ha` is Helm-only.
`k8s/base` holds the proxy (2 replicas, `appProtocol: kubernetes.io/h2c`, and a Role to start worker Jobs), the burst
worker `PodTemplate` (no ServiceAccount token or RBAC) and the 6-hourly sweeper `CronJob`. `k8s/overlays/minimal` adds
the `koshin` namespace plus a single NATS, a coordination Valkey (`noeviction`) and a package-cache Valkey
(`allkeys-lru`), all on `emptyDir`.

```bash
kubectl -n koshin create secret generic koshin-proxy-secrets \
  --from-literal=GITHUB_WEBHOOK_SECRET=... --from-literal=INTERNAL_TRIGGER_TOKEN=...
kubectl -n koshin create secret generic koshin-worker-secrets --from-literal=GITHUB_TOKEN=...
kubectl apply -k k8s/overlays/minimal
```

* **Repositories to sweep:** the `koshin-repositories` ConfigMap (`github:org/repo` per line), replaced from your
  overlay. Empty by default, so the sweeper enqueues nothing.
* **Workers:** none run while the queue is empty; the proxy starts burst worker Jobs from the `koshin-worker`
  PodTemplate (see [Workers](workers.md#dispatch)).
* **`minimal` trade-off:** a NATS or Valkey restart loses pending jobs, locks, dirty flags and the package cache; proxy
  and worker recreate the streams on reconnect (and publishers the consumers), and the sweeper re-enqueues every
  repository within 6 hours (after a NATS restart, those still marked queued only once the mark expires:
  `QUEUED_TTL_SECONDS`). The Helm chart's `ha` profile gives NATS and Valkey persistence.
* **Encrypted registry credentials:** the `k8s/components/renovate-encryption` component mounts koshin's OpenPGP
  keypair from the `koshin-renovate-encryption` Secret (see [Encrypted registry credentials](registry-credentials.md)).
* **Edge HTTP/3:** `k8s/examples/gateway-http3.yaml` is an Envoy Gateway example that terminates TLS + HTTP/3 and
  exposes only `/webhooks/` and `/api/v1/encryption-key`.

## Valkey requirements

The coordination Valkey (`VALKEY_URL`, standalone `redis://` or Sentinel `redis-sentinel://`) stores per-repository
queued marks (`koshin:{<platform>:<repo>}:queued`), mutexes (`koshin:{<platform>:<repo>}:lock`) and dirty flags
(`koshin:{<platform>:<repo>}:dirty`) with explicit TTLs (`EX`), plus run slots, cooldowns and leases.

* **Valkey 7.2+ (or Redis 6.2+):** `releaseLockAndTakeDirty` uses `GETDEL` inside Lua.
* **`maxmemory-policy noeviction` (required):** locks and dirty flags are TTL-bearing keys, so any `volatile-*` or
  `allkeys-*` policy under memory pressure would evict active locks or pending dirty flags first, breaking mutual
  exclusion and coalescing.
* **A separate package cache:** keep Renovate's package cache (`RENOVATE_REDIS_URL`) on its own instance with
  `allkeys-lru`. With `noeviction`, a full cache makes Valkey reject every write, lock acquisition included. The Helm
  chart (both profiles), the Kustomize overlay and `docker-compose.yml` all run a dedicated cache Valkey.
* **Replica acknowledgment (`LOCK_REPLICA_ACK_TIMEOUT_MS`, optional):** when positive, the worker issues `WAIT 1 <ms>`
  after acquiring a repository lock, to narrow the failover window in primary-replica (`ha`) topologies. Keep it small
  (e.g. `20`–`100` ms) and **only enable it in `ha`**: with zero replicas, `WAIT 1 <ms>` blocks every lock acquisition
  for the full timeout.
* **Pre-queue coalescing:** the proxy publishes a webhook or trigger only for a repo that is neither queued nor
  running: a duplicate for a queued repo is dropped, and one for a running repo sets its dirty flag for a single
  follow-up run. If Valkey fails or doesn't answer within `COALESCE_TIMEOUT_MS`, the proxy publishes anyway (fail-open)
  and the workers coalesce the duplicate. A queued mark lasts at most `QUEUED_TTL_SECONDS` (24h), so a job lost without
  being dead-lettered (a NATS restart on `emptyDir`) blocks its repo's webhooks until then.

## Container images

* **`koshin-proxy`**: `eclipse-temurin:25-jre-noble`, runs as UID `10001`.
* **`koshin-worker`**: `eclipse-temurin:25-jre-noble` with the worker, running as Renovate's UID `12021` (~140 MB
  compressed). It holds no Renovate: the same image's `koshin-runner-init` copies the Renovate runner
  (`/opt/koshin/{jre,lib,bin}`, JVM flags in `KOSHIN_RUNNER_JAVA_OPTS`) into a volume for the pod's `renovate`
  container.
* **Renovate** runs from the upstream `ghcr.io/renovatebot/renovate:<version>-full` image (Renovate plus its whole
  toolchain, `RENOVATE_BINARY_SOURCE=global`), pinned in the chart (`worker.renovate.image`), Kustomize and Compose and
  upgradable without a koshin release. Renovate (the bot) bumps it in one weekly, automerged PR. That image is ~10 GB:
  in-cluster [Spegel](https://github.com/spegel-org/spegel) shares image layers between nodes P2P, so a new worker pod
  usually pulls from a peer instead of the registry; no koshin-specific config is needed.
* JVM flags come from `KOSHIN_JAVA_OPTS` (not `JAVA_OPTS`/`JAVA_TOOL_OPTIONS`, which Renovate's own Java/Gradle runs
  would inherit).

## HTTP/2 and HTTP/3

The proxy serves HTTP/1.1 (GitHub and GitLab webhook deliveries, probes) and cleartext HTTP/2 (`h2c`, via
`Upgrade: h2c` or prior knowledge) when `HTTP2_ENABLED=true` (the default), and `h2` through ALPN on a TLS connector.
Ktor doesn't expose Netty's QUIC codec, so terminate TLS and HTTP/3 at the ingress or gateway (e.g. Traefik or Envoy
Gateway) and route to the `koshin-proxy` Service over `h2c` (`appProtocol: kubernetes.io/h2c`). Only `/webhooks/` and
`/api/v1/encryption-key` need to be public; `/api/v1/trigger` stays cluster-internal for the sweeper.
