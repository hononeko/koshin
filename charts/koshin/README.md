# koshin

Distributed Renovate orchestrator - a stateless webhook proxy dispatching scale-to-zero workers on NATS JetStream and Valkey

![Version: 0.8.0](https://img.shields.io/badge/Version-0.8.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.8.0](https://img.shields.io/badge/AppVersion-0.8.0-informational?style=flat-square)

This chart is the primary way to install koshin. The Kustomize manifests in `k8s/` stay as a small, dependency-free
reference for the `minimal` profile.

## Prerequisites

* Kubernetes 1.30+ (native sidecar containers)
* No autoscaler: the proxy starts worker `Job`s itself (see [Workers](#workers)).
* Workers run Renovate from the upstream `ghcr.io/renovatebot/renovate:<version>-full` image (`worker.renovate.image`),
  about 10 GB uncompressed with its tools. The koshin images are small Temurin JRE images.

## Install

Create the secrets first. The chart never needs them inline:

```sh
kubectl create namespace koshin
kubectl -n koshin create secret generic koshin-proxy \
  --from-literal=GITHUB_WEBHOOK_SECRET=... \
  --from-literal=INTERNAL_TRIGGER_TOKEN="$(openssl rand -hex 24)"
kubectl -n koshin create secret generic koshin-worker --from-literal=GITHUB_TOKEN=...
```

To run as a GitHub App instead of with a PAT, register one as the
[GitHub App guide](https://github.com/hononeko/koshin/blob/main/docs/github-app.md) shows and set `github.app.id` and
`github.app.existingSecret` (no `worker.existingSecret` needed for GitHub).

### `minimal` profile

One NATS server and one coordination Valkey on `emptyDir`, plus a separate package-cache Valkey. A restart loses
pending jobs, locks and repo caches; the sweeper re-enqueues every repository every 6 hours.

```sh
helm install koshin oci://ghcr.io/hononeko/charts/koshin -n koshin \
  --set proxy.existingSecret=koshin-proxy \
  --set worker.existingSecret=koshin-worker \
  --set 'proxy.sweepRepositories={github:my-org/my-repo}'
```

### `ha` profile

Survives losing one node: a 3-node NATS cluster on PVCs (streams and the repo cache bucket at 3 replicas), Valkey as a
primary with 2 replicas, AOF on PVCs and 3 Sentinels (workers find the primary through Sentinel), PodDisruptionBudgets
and spread across nodes. The package-cache Valkey stays a single, disposable instance. One worker stays warm
(`worker.autoscaling.minReplicas: 1`).

Helm can't switch subchart defaults from a single value, so `ha` comes as a values file, `values-ha.yaml`. It sets
`profile: ha` along with the NATS and Valkey topology, and the chart refuses to render a `profile` that doesn't match
the topology. The official Valkey chart requires a Sentinel password:

```sh
kubectl -n koshin create secret generic koshin-valkey-sentinel --from-literal=sentinel="$(openssl rand -hex 24)"
helm pull oci://ghcr.io/hononeko/charts/koshin --untar
helm install koshin ./koshin -n koshin -f koshin/values-ha.yaml \
  --set proxy.existingSecret=koshin-proxy \
  --set worker.existingSecret=koshin-worker
```

See [Choosing a profile](#choosing-a-profile).

### Bring your own NATS or Valkey

Disable a subchart and point koshin at an existing endpoint:

```yaml
nats:
  enabled: false
externalNats:
  url: nats://nats.messaging:4222
valkey:
  enabled: false
externalValkey:
  url: redis-sentinel://valkey-sentinel.data:26379?sentinelMasterId=mymaster
  # Only if Sentinel uses an ACL user instead of the default one
  sentinelUsername: sentinel
  sentinelPasswordSecret:
    name: valkey-sentinel
    key: password
valkey-cache:
  enabled: false
worker:
  renovateRedisUrl: redis://renovate-cache.data:6379
```

The coordination Valkey must use `maxmemory-policy noeviction`. The package cache should be a separate instance with
`allkeys-lru`.

## Choosing a profile

| | `minimal` | `ha` |
| :--- | :--- | :--- |
| Nodes | any | 3 or more (NATS spreads one pod per node) |
| Storage | `emptyDir` only | a `ReadWriteOnce` storage class: 3 × 10 Gi for NATS, 3 × 1 Gi for Valkey |
| Before installing | the proxy and worker Secrets | also the Sentinel password Secret (`<release>-valkey-sentinel`) |
| NATS restarts | pending jobs and repo caches are lost | nothing is lost: another node leads the streams |
| Valkey restarts | locks, dirty flags and cooldowns are lost | Sentinel promotes a replica if the primary went down; see below for what that can lose |
| A node is drained | same as a restart | PodDisruptionBudgets keep 2 of 3 NATS, Valkey and Sentinel pods up |

`minimal` suits small installs: what it loses is recovered by the sweeper within 6 hours (it needs
`INTERNAL_TRIGGER_TOKEN` and `proxy.sweepRepositories`) or rebuilt by the next run. Pick `ha` when webhooks must not
wait that long after an outage, or when cold repo caches after every restart cost too much API budget.

During a Valkey failover Sentinel promotes a replica within about 15 seconds, and the workers reconnect through
Sentinel. Replication is asynchronous, so a lock or dirty flag written just before the failover can be lost. A lost
lock is caught by the holder's next heartbeat, which kills its Renovate run and NAKs the job for a retry. A lost dirty
flag means a coalesced webhook gets no follow-up run until the next webhook or sweep. A NATS leader election pauses
publishing for a few seconds; the proxy retries, so webhooks are still accepted.

## Secrets

| Secret | Keys | Value |
| :--- | :--- | :--- |
| Proxy | `GITHUB_WEBHOOK_SECRET`, `GITLAB_WEBHOOK_SECRET`, `INTERNAL_TRIGGER_TOKEN` | `proxy.existingSecret` |
| Worker | `GITHUB_TOKEN`, `GITLAB_TOKEN` | `worker.existingSecret` |
| GitHub App | the private key (PEM) under `github.app.privateKeyKey` | `github.app.existingSecret` |
| Encrypted config | koshin's OpenPGP keypair: `private.asc`, `public.asc` (and `private-old.asc` while rotating) | `renovateEncryption.existingSecret` |
| S3 repo cache | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | `worker.repoCache.s3.existingSecret` |
| Sentinel (`ha`) | `sentinel` | `valkey.sentinel.existingSecret` (`<release>-valkey-sentinel`) |

The inline `*.secrets.*` and `github.app.privateKey` values exist for testing only. Without `INTERNAL_TRIGGER_TOKEN`
the sweeper CronJob isn't installed.

## Workers

With `worker.autoscaling.enabled` (the default), no autoscaler is involved: the proxy starts burst workers as Kubernetes
`Job`s, cloned from the `<release>-worker` `PodTemplate` the chart renders, so every pod setting stays in your values.
It reconciles right after it publishes a job and every `autoscaling.dispatchIntervalSeconds`, and only the
proxy replica holding a Valkey lease does, so replicas never double up.

* **Demand:** jobs it can run now, per credential scope (a GitHub App installation, or a platform's token), up to
  `rateLimits.maxConcurrentRunsPerCredential` runs each, `rateLimits.maxConcurrentRunsGlobal` in all and
  `autoscaling.maxReplicas` workers. A scope in a rate-limit cooldown adds none, so one installation's cooldown starts
  no idle workers and holds up no other installation.
* **Burst workers** run jobs back to back and exit 0 once none was runnable for `autoscaling.idleTimeoutSeconds`, or
  after the run in flight once they're `autoscaling.maxLifetimeSeconds` old (below the Job's
  `autoscaling.jobActiveDeadlineSeconds`). Finished Jobs go after `autoscaling.jobTtlSecondsAfterFinished`. The proxy
  never stops a worker, and uninstalling the chart removes the Jobs with their `PodTemplate`.
* **`minReplicas: 0`** (the default) scales to zero: every job waits for a burst worker pod to start, the Renovate image
  pull included. **`minReplicas: 1`** keeps one warm worker in a Deployment (rolled by `helm upgrade`, never scaled in)
  that picks a single job up at once; burst Jobs start only for a second concurrent job, or while the warm worker is
  restarting.
* The proxy gets a Role in the release namespace: `get` on the worker `PodTemplate`, and Jobs. Its token is a projected
  one valid for `proxy.serviceAccount.tokenExpirationSeconds`. Workers get no token.

Without autoscaling, `worker.replicas` static workers run in a Deployment.

## Worker isolation

Renovate can run code from the repositories it updates (e.g. Gradle during lockfile updates), so it doesn't run in the
`worker` container. Each worker pod has a second container, `renovate` (a native sidecar running
`worker.renovate.image`), and the worker hands it every run over a Unix socket in a shared memory `emptyDir`. An init
container, `koshin-runner-init`, first copies the runner from the worker image into a volume that `renovate` mounts
read-only at `/opt/koshin`, so a run can't change it.

* The `renovate` container gets none of the worker's Secrets and no ServiceAccount token, and can't see the worker's
  processes (or their environment) through `/proc`. Renovate gets only the run's environment: the platform token for
  that run and the non-secret settings.
* After each run, every process left in the `renovate` container is killed, so no process a run starts can watch the
  next one.
* No worker container mounts a ServiceAccount token: workers never call the Kubernetes API, and the chart
  installs no RBAC or cluster-scoped resources for them.

## Encrypted registry credentials

Tenants can encrypt their private registry credentials (GitHub Packages PATs included, which installation tokens can't
replace) against koshin's OpenPGP public key, for Renovate's `encrypted` config. Create the keypair Secret as
the [registry credentials guide](https://github.com/hononeko/koshin/blob/main/docs/registry-credentials.md#operators)
shows, then:

```yaml
renovateEncryption:
  existingSecret: koshin-renovate-encryption
```

* The private key is mounted into each worker pod's `renovate` container only, at
  `/etc/renovate-encryption/private.asc`, and the worker passes `RENOVATE_PRIVATE_KEY_PATH` on to Renovate. Renovate
  reads it at the start of every run, so it holds the key while it runs: the worker pins
  `RENOVATE_ALLOW_SCRIPTS=false`, `RENOVATE_ALLOWED_UNSAFE_EXECUTIONS=[]`, `RENOVATE_ALLOW_PLUGINS=false` and
  `RENOVATE_ALLOW_CUSTOM_CRATE_GIT_REGISTRIES=false`, and with the key also `RENOVATE_ALLOWED_COMMANDS=[]` and
  `RENOVATE_ALLOW_SHELL_EXECUTOR_FOR_POST_UPGRADE_COMMANDS=false` (no `postUpgradeTasks`), so no repository runs code
  next to it.
* The proxy gets the public key only, and serves it, unauthenticated, at `GET /api/v1/encryption-key`. The ingress and
  the HTTPRoute route that path (`Exact`) along with `/webhooks/`.
* **Rotating:** put the new keypair in the Secret as `private.asc` / `public.asc`, the previous private key as
  `private-old.asc`, and set `renovateEncryption.privateKeyOldKey: private-old.asc`. Renovate tries both, and the proxy
  serves the new public key once the Secret update reaches its pod. Unset it once tenants have re-encrypted.

## Renovate version

The chart pins Renovate in `worker.renovate.image` (a `-full` tag, which ships the tools Renovate runs). Set `tag` or
`digest` to run another version, without upgrading koshin:

```yaml
worker:
  renovate:
    image:
      tag: 44.180.0-full
```

koshin relies on Renovate's repository cache layout and its rate-limit log message (`worker.rateLimits.logPattern`),
both checked against the pinned version, so try a new major before rolling it out.

## Repository cache

`worker.repoCache.provider` picks where Renovate's repository cache lives between runs: `nats` (default, an Object Store
bucket in the release's NATS, capped by `maxBytes` since it shares JetStream storage with the job streams), `s3`
(Renovate reads and writes it itself, e.g. SeaweedFS) or `local` (pod only). `s3` hands Renovate the bucket's
credentials, so it's single-tenant: the chart refuses it with the GitHub App.

```yaml
worker:
  repoCache:
    provider: s3
    s3:
      url: s3://renovate/repo-cache/
      endpoint: http://seaweedfs-s3.storage:8333
      existingSecret: renovate-s3
```

## Exposing webhooks over HTTP/3

Only `/webhooks/` should be public; `/api/v1/trigger` stays cluster-internal for the sweeper. With Envoy Gateway, TLS
and HTTP/3 terminate at the edge and the gateway talks h2c to the proxy (the Service sets
`appProtocol: kubernetes.io/h2c`):

```yaml
proxy:
  httpRoute:
    enabled: true
    parentRefs:
      - name: koshin
    hostnames:
      - koshin.example.com
extraObjects:
  - apiVersion: gateway.envoyproxy.io/v1alpha1
    kind: ClientTrafficPolicy
    metadata:
      name: "{{ .Release.Name }}-http3"
    spec:
      targetRefs:
        - group: gateway.networking.k8s.io
          kind: Gateway
          name: koshin
      http3: {}
```

## Sizing

Resource requests with the defaults:

| Component | Pods (`minimal` / `ha`) | Requests per pod | Limits per pod | Storage per pod |
| :--- | :--- | :--- | :--- | :--- |
| Proxy | 2 / 3 | 100m CPU, 256 Mi | 512 Mi | — |
| Worker: `renovate` container | `autoscaling.minReplicas` (0 / 1) to `autoscaling.maxReplicas` | 500m CPU, 1 Gi | 4 Gi | 10 Gi `emptyDir`, plus the ~10 GB Renovate image on each node |
| Worker: `worker` container | (same pods) | 100m CPU, 256 Mi | 768 Mi | — |
| NATS | 1 / 3 | 50m CPU, 64 Mi | 256 Mi | 2 Gi `emptyDir` / 10 Gi PVC |
| Valkey (coordination) | 1 / 3 | 50m CPU, 32 Mi | 128 Mi | `emptyDir` / 1 Gi PVC |
| Sentinel | — / 3 | unset (`valkey.sentinel.resources`) | unset | memory-backed `emptyDir` |
| Valkey (package cache) | 1 / 1 | 50m CPU, 128 Mi | 512 Mi | `emptyDir` |
| Sweeper (CronJob) | 1 every 6 hours | 10m CPU, 16 Mi | 64 Mi | — |

Workers dominate: each runs one Renovate process in its `renovate` container (`worker.renovate.resources`), and large
repositories (monorepos, many package managers) may need more than 4 Gi. Keep the Renovate image on nodes that run workers (e.g. with an image cache or Spegel), since pulling
10 GB delays every scale-up.

JetStream storage is mostly the repo cache: jobs are small and leave the stream once acknowledged. Keep
`worker.repoCache.maxBytes` well below `nats.config.jetstream.fileStore.maxSize` (1 Gi in `minimal`, 8 Gi in `ha`) so
the job streams always have room. With `ha` you can raise it to a few Gi. When you raise `fileStore.maxSize`, raise
`fileStore.pvc.size` with it: the PVC needs about 20% headroom for indexes and consumer state.

A credential scope's API budget is roughly its run slots × requests per second per run. Keep
`worker.autoscaling.maxReplicas` near `worker.rateLimits.maxConcurrentRunsGlobal` (the default): extra workers only wait
for a slot. During a rate-limit cooldown the proxy starts no worker for that scope's jobs.

## Backups and recovery

koshin keeps no state that has to be backed up; after a total loss, reinstall and the sweeper re-enqueues every
repository.

* **Coordination Valkey:** locks and run slots expire within 45 seconds, dirty flags and cooldowns within an hour, and
  the rate-limit backoff counter resets after the next successful run. In `ha` the AOF (`appendfsync everysec`) on the PVCs carries
  them across restarts. Don't restore an old copy: stale locks and cooldowns would only delay runs.
* **NATS:** pending jobs are worth keeping across restarts (`ha` replicates them 3 ways), but not in a backup: by the
  time you restore, the sweeper has re-enqueued them. The repo cache bucket is a cache that rebuilds on the next
  successful run of each repository. Back up the dead-letter stream if you want to keep failed jobs for inspection, and
  restore it into a NATS without that stream:

  ```sh
  nats backup stream RENOVATE_DLQ ./koshin-dlq
  nats backup restore stream ./koshin-dlq
  ```

  Prefer this to volume snapshots: snapshots of the 3 `ha` nodes taken independently don't make a consistent cluster.
* **Package-cache Valkey:** a cache; nothing to back up.
* **Secrets:** keep the webhook secrets, the trigger token, the platform tokens or GitHub App key, the S3 credentials (if
  any) and the Sentinel password in your secret manager.

## Uninstall

`helm uninstall` leaves the PersistentVolumeClaims of the NATS and Valkey StatefulSets behind (`ha` only; `minimal` has
none), so a reinstall finds its data again. To remove them as well (here for a release named `koshin`):

```sh
helm uninstall koshin -n koshin
kubectl -n koshin delete pvc -l app.kubernetes.io/instance=koshin
```

With a `Retain` storage class, the PersistentVolumes stay until you delete them too. The Secrets you created for the
chart are yours to delete.

## Requirements

Kubernetes: `>=1.30.0-0`

| Repository | Name | Version |
|------------|------|---------|
| https://nats-io.github.io/k8s/helm/charts/ | nats | 2.15.2 |
| https://valkey.io/valkey-helm/ | valkey | 1.0.0-rc.1 |
| https://valkey.io/valkey-helm/ | valkey-cache(valkey) | 1.0.0-rc.1 |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| clusterDomain | string | `"cluster.local"` | Kubernetes cluster domain, used by `tests.jobProcessing` to reach NATS monitoring |
| commonLabels | object | `{}` | Labels added to every koshin resource |
| externalNats | object | `{"url":""}` | NATS server used when `nats.enabled` is false |
| externalNats.url | string | `""` | NATS URL, e.g. `nats://nats.messaging:4222` |
| externalValkey | object | `{"sentinelPasswordSecret":{"key":"","name":""},"sentinelUsername":"","url":""}` | Coordination Valkey used when `valkey.enabled` is false |
| externalValkey.sentinelPasswordSecret | object | `{"key":"","name":""}` | Secret with Sentinel's own password (`VALKEY_SENTINEL_PASSWORD`), if Sentinel requires one |
| externalValkey.sentinelUsername | string | `""` | `VALKEY_SENTINEL_USERNAME`: Sentinel's ACL user, if it has one (empty: the default user) |
| externalValkey.url | string | `""` | `VALKEY_URL`: `redis://host:6379` or `redis-sentinel://host:26379?sentinelMasterId=mymaster` |
| extraObjects | list | `[]` | Extra manifests rendered with the release (templated), e.g. an Envoy Gateway `ClientTrafficPolicy` for HTTP/3 |
| fullnameOverride | string | `""` | Overrides the full resource name prefix (default: the release name, plus `-koshin` unless it contains it) |
| github.apiUrl | string | `""` | `GITHUB_API_URL` for GitHub Enterprise Server (default `https://api.github.com`) |
| github.app.allowedAccounts | list | `[]` | `GITHUB_APP_ALLOWED_ACCOUNTS`: the users and organizations whose installations koshin serves, as numeric IDs (preferred: `gh api users/<login> --jq .id`) or logins. Empty serves every installation, which is fine for a private App; set it before making the App public. |
| github.app.disallowedAction | string | `"ignore"` | `GITHUB_APP_DISALLOWED_ACTION` for installations by other accounts: `ignore`, `suspend` or `uninstall` |
| github.app.existingSecret | string | `""` | Existing Secret holding the App's private key (PEM), mounted for both the proxy and the worker |
| github.app.id | string | `""` | `GITHUB_APP_ID` |
| github.app.privateKey | string | `""` | Inline private key, for testing only; ignored when `existingSecret` is set |
| github.app.privateKeyKey | string | `"private-key.pem"` | Key of the private key in `existingSecret` |
| github.botLogin | string | `""` | `GITHUB_BOT_LOGIN`: webhooks sent by this login are ignored, so Renovate's own pushes don't re-trigger it. With the GitHub App, its bot (`<slug>[bot]`) is ignored without this. |
| gitlab.botLogin | string | `""` | `GITLAB_BOT_LOGIN`: webhooks sent by this login are ignored |
| gitlab.endpoint | string | `""` | `GITLAB_ENDPOINT`, e.g. `https://gitlab.example.com/api/v4/` |
| http2Enabled | bool | `true` | `HTTP2_ENABLED`: cleartext HTTP/2 (h2c) next to HTTP/1.1 on the proxy and worker |
| image.pullPolicy | string | `"IfNotPresent"` | Pull policy of the koshin images |
| image.registry | string | `"ghcr.io"` | Registry of the koshin images |
| imagePullSecrets | list | `[]` | Image pull secrets for koshin's own pods |
| logging.level | string | `"info"` | `LOG_LEVEL`: koshin's own log level (`trace`, `debug`, `info`, `warn`, `error`) |
| logging.renovateLevel | string | `"info"` | `RENOVATE_LOG_LEVEL`: Renovate's own log level (`trace`, `debug`, `info`, `warn`, `error`, `fatal`) |
| logging.rootLevel | string | `"info"` | `ROOT_LOG_LEVEL`: the level of the libraries koshin uses (NATS, Valkey, Ktor, ...) |
| nameOverride | string | `""` | Overrides the chart name in resource names |
| nats.config.cluster.enabled | bool | `false` | Run a NATS cluster (`ha`) |
| nats.config.cluster.replicas | int | `3` | Cluster size |
| nats.config.jetstream.enabled | bool | `true` |  |
| nats.config.jetstream.fileStore.dir | string | `"/data"` |  |
| nats.config.jetstream.fileStore.enabled | bool | `true` |  |
| nats.config.jetstream.fileStore.maxSize | string | `"1Gi"` | JetStream file storage limit (streams and the repo cache bucket) |
| nats.config.jetstream.fileStore.pvc.enabled | bool | `false` | Keep JetStream on PVCs (`ha`); `minimal` uses an emptyDir |
| nats.config.jetstream.fileStore.pvc.size | string | `"10Gi"` |  |
| nats.config.jetstream.memoryStore.enabled | bool | `true` |  |
| nats.config.jetstream.memoryStore.maxSize | string | `"64Mi"` |  |
| nats.config.monitor.enabled | bool | `true` |  |
| nats.config.monitor.port | int | `8222` |  |
| nats.config.nats.port | int | `4222` |  |
| nats.container.merge.resources.limits.memory | string | `"256Mi"` |  |
| nats.container.merge.resources.requests.cpu | string | `"50m"` |  |
| nats.container.merge.resources.requests.memory | string | `"64Mi"` |  |
| nats.container.merge.securityContext.allowPrivilegeEscalation | bool | `false` |  |
| nats.container.merge.securityContext.capabilities.drop[0] | string | `"ALL"` |  |
| nats.container.merge.securityContext.readOnlyRootFilesystem | bool | `true` |  |
| nats.container.patch[0].op | string | `"add"` |  |
| nats.container.patch[0].path | string | `"/volumeMounts/-"` |  |
| nats.container.patch[0].value.mountPath | string | `"/data"` |  |
| nats.container.patch[0].value.name | string | `"js-data"` |  |
| nats.enabled | bool | `true` | Deploy NATS with the release; set false and configure `externalNats` to bring your own |
| nats.natsBox.enabled | bool | `false` |  |
| nats.podTemplate.merge.spec.securityContext.fsGroup | int | `1000` |  |
| nats.podTemplate.merge.spec.securityContext.runAsGroup | int | `1000` |  |
| nats.podTemplate.merge.spec.securityContext.runAsNonRoot | bool | `true` |  |
| nats.podTemplate.merge.spec.securityContext.runAsUser | int | `1000` |  |
| nats.podTemplate.merge.spec.securityContext.seccompProfile.type | string | `"RuntimeDefault"` |  |
| nats.podTemplate.patch[0].op | string | `"add"` |  |
| nats.podTemplate.patch[0].path | string | `"/spec/volumes/-"` |  |
| nats.podTemplate.patch[0].value.emptyDir.sizeLimit | string | `"2Gi"` |  |
| nats.podTemplate.patch[0].value.name | string | `"js-data"` |  |
| nats.reloader.merge.securityContext.allowPrivilegeEscalation | bool | `false` |  |
| nats.reloader.merge.securityContext.capabilities.drop[0] | string | `"ALL"` |  |
| nats.reloader.merge.securityContext.readOnlyRootFilesystem | bool | `true` |  |
| natsSettings.ackWaitSeconds | int | `60` | `NATS_ACK_WAIT_SECONDS`; must be greater than `worker.config.lockTtlSeconds` |
| natsSettings.consumerIdleTtlDays | int | `7` | `NATS_CONSUMER_IDLE_TTL_DAYS`: a credential scope's consumer is deleted once it's had no job for this long (e.g. an uninstalled GitHub App installation); 0 keeps them all |
| natsSettings.exhaustedSweepSeconds | int | `300` | `NATS_EXHAUSTED_SWEEP_SECONDS`: how often the proxy moves exhausted jobs whose MaxDeliver advisory was missed to the DLQ and tidies the per-scope consumers; 0 turns it off |
| natsSettings.maxDeliver | int | `5` | `NATS_MAX_DELIVER`: deliveries of a job before it's dead-lettered |
| natsSettings.streamName | string | `"RENOVATE_JOBS"` | `NATS_STREAM_NAME`: the job stream (WorkQueue) |
| natsSettings.streamReplicas | string | `nil` | `NATS_STREAM_REPLICAS` for the job and DLQ streams and the repo cache bucket. Default: the NATS cluster size (`nats.config.cluster.replicas` when clustered), else 1. |
| networkPolicy.enabled | bool | `false` | Create NetworkPolicies: the proxy accepts webhooks from anywhere, and NATS/Valkey accept only this release's pods |
| networkPolicy.proxyFrom | list | `[]` | Sources allowed to reach the proxy (default: anywhere) |
| platforms | list | `["github"]` | Enabled platforms (`github`, `gitlab`). Each gets its own NATS consumer and run slots. |
| profile | string | `"minimal"` | Deployment profile: `minimal` (single NATS and Valkey on `emptyDir`) or `ha` (3-node NATS on PVCs, Valkey with Sentinel). Install `ha` with `-f values-ha.yaml`: Helm can't switch subchart defaults from a single value, so the file sets both this and the NATS/Valkey topology, and the chart refuses to render a profile that doesn't match them. |
| proxy.affinity | object | `{}` | Proxy affinity |
| proxy.existingSecret | string | `""` | Existing Secret with any of `GITHUB_WEBHOOK_SECRET`, `GITLAB_WEBHOOK_SECRET` and `INTERNAL_TRIGGER_TOKEN`. With it set, the sweeper (and `tests.jobProcessing`) expect `INTERNAL_TRIGGER_TOKEN` in it. |
| proxy.extraEnv | list | `[]` | Extra environment variables for the proxy |
| proxy.httpRoute.annotations | object | `{}` | HTTPRoute annotations |
| proxy.httpRoute.enabled | bool | `false` | Expose the webhook endpoints through a Gateway API HTTPRoute |
| proxy.httpRoute.hostnames | list | `[]` | Hostnames of the route |
| proxy.httpRoute.parentRefs | list | `[]` | Gateways to attach to |
| proxy.httpRoute.pathPrefixes | list | `["/webhooks/"]` | Path prefixes routed to the proxy (and `/api/v1/encryption-key` with `renovateEncryption.existingSecret`) |
| proxy.image.digest | string | `""` | Proxy image digest; takes precedence over the tag |
| proxy.image.repository | string | `"hononeko/koshin-proxy"` | Proxy image repository |
| proxy.image.tag | string | `""` | Proxy image tag (default: the chart's `appVersion`) |
| proxy.ingress.annotations | object | `{}` | Ingress annotations |
| proxy.ingress.className | string | `""` | Ingress class |
| proxy.ingress.enabled | bool | `false` | Expose the webhook endpoints through an Ingress |
| proxy.ingress.hosts | list | `[{"host":"koshin.example.com","paths":[{"path":"/webhooks/","pathType":"Prefix"}]}]` | Hosts and paths. Only `/webhooks/` is exposed by default (and `/api/v1/encryption-key` with `renovateEncryption.existingSecret`): `/api/v1/trigger` stays cluster-internal. |
| proxy.ingress.tls | list | `[]` | Ingress TLS |
| proxy.nodeSelector | object | `{}` | Proxy node selector |
| proxy.podAnnotations | object | `{}` | Proxy pod annotations |
| proxy.podDisruptionBudget.enabled | bool | `true` | Create a PodDisruptionBudget for the proxy |
| proxy.podDisruptionBudget.minAvailable | int | `1` | Minimum available proxy pods |
| proxy.podLabels | object | `{}` | Proxy pod labels |
| proxy.podSecurityContext | object | `{"runAsGroup":10001,"runAsNonRoot":true,"runAsUser":10001,"seccompProfile":{"type":"RuntimeDefault"}}` | Proxy pod security context |
| proxy.replicas | int | `2` | Proxy replicas |
| proxy.resources | object | `{"limits":{"memory":"512Mi"},"requests":{"cpu":"100m","memory":"256Mi"}}` | Proxy resources |
| proxy.secrets.githubWebhookSecret | string | `""` | `GITHUB_WEBHOOK_SECRET` |
| proxy.secrets.gitlabWebhookSecret | string | `""` | `GITLAB_WEBHOOK_SECRET` |
| proxy.secrets.internalTriggerToken | string | `""` | `INTERNAL_TRIGGER_TOKEN`, the bearer token of `POST /api/v1/trigger` (required by the sweeper) |
| proxy.securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true}` | Proxy container security context |
| proxy.service.annotations | object | `{}` | Proxy Service annotations |
| proxy.service.port | int | `80` | Proxy Service port |
| proxy.service.type | string | `"ClusterIP"` | Proxy Service type |
| proxy.serviceAccount.annotations | object | `{}` | Proxy ServiceAccount annotations |
| proxy.serviceAccount.tokenExpirationSeconds | int | `600` | Lifetime in seconds (at least 600) of the token the proxy's worker dispatcher uses for the Kubernetes API; the kubelet renews it before it expires. Mounted only with `worker.autoscaling.enabled`. |
| proxy.sweepRepositories | list | `[]` | Repositories the sweeper schedules (`github:org/repo` or `gitlab:group/project`), served to the proxy as `SWEEP_REPOSITORIES_FILE` |
| proxy.tolerations | list | `[]` | Proxy tolerations |
| proxy.topologySpreadConstraints | list | `[]` | Proxy topology spread constraints (default: spread across nodes) |
| renovateBotLogins | list | `[]` | `RENOVATE_BOT_LOGINS`: more bot logins whose webhooks are ignored |
| renovateEncryption.existingSecret | string | `""` | Existing Secret holding koshin's armored OpenPGP keypair; empty disables encrypted config |
| renovateEncryption.privateKeyKey | string | `"private.asc"` | Key of the private key in `existingSecret`, mounted into the worker's `renovate` container only (`RENOVATE_PRIVATE_KEY_PATH`) |
| renovateEncryption.privateKeyOldKey | string | `""` | Key of the previous private key while rotating keys (`RENOVATE_PRIVATE_KEY_PATH_OLD`); empty: none |
| renovateEncryption.publicKeyKey | string | `"public.asc"` | Key of the public key in `existingSecret`, mounted into the proxy only (`RENOVATE_PUBLIC_KEY_PATH`). The ingress and the HTTPRoute route `/api/v1/encryption-key` to it. |
| sweeper.enabled | bool | `true` | Schedule a run of every discovered repository on `schedule`. Needs `INTERNAL_TRIGGER_TOKEN`; without one the CronJob isn't rendered. |
| sweeper.image.repository | string | `"docker.io/curlimages/curl"` | Sweeper image (any image with curl) |
| sweeper.image.tag | string | `"8.23.0"` | Sweeper image tag |
| sweeper.resources | object | `{"limits":{"memory":"64Mi"},"requests":{"cpu":"10m","memory":"16Mi"}}` | Sweeper resources |
| sweeper.schedule | string | `"0 */6 * * *"` | Cron schedule |
| tests.jobProcessing.enabled | bool | `false` | `helm test` also triggers a Renovate run of `repository` and waits until a worker acknowledges it, and with `worker.autoscaling` checks the burst workers: one starts and exits again with `minReplicas: 0`, none starts with `minReplicas: 1`. Off by default because it starts a real run; needs `INTERNAL_TRIGGER_TOKEN` and the `nats` subchart. |
| tests.jobProcessing.platform | string | `"github"` | Platform of the test repository |
| tests.jobProcessing.repository | string | `"hononeko/koshin"` | Repository the test triggers |
| tests.jobProcessing.timeoutSeconds | int | `600` | How long to wait for the run to finish |
| valkey-cache.enabled | bool | `true` | Deploy the package-cache Valkey with the release |
| valkey-cache.extraConfig | string | `"maxmemory 384mb\nmaxmemory-policy allkeys-lru\nsave \"\"\nappendonly no"` | valkey.conf additions |
| valkey-cache.resources.limits.memory | string | `"512Mi"` |  |
| valkey-cache.resources.requests.cpu | string | `"50m"` |  |
| valkey-cache.resources.requests.memory | string | `"128Mi"` |  |
| valkey-cache.service.port | int | `6379` |  |
| valkey.enabled | bool | `true` | Deploy the coordination Valkey with the release; set false and configure `externalValkey` to bring your own |
| valkey.extraConfig | string | `"maxmemory 64mb\nmaxmemory-policy noeviction\nsave \"\"\nappendonly no"` | valkey.conf additions |
| valkey.resources.limits.memory | string | `"128Mi"` |  |
| valkey.resources.requests.cpu | string | `"50m"` |  |
| valkey.resources.requests.memory | string | `"32Mi"` |  |
| valkey.sentinel.enabled | bool | `false` | Sentinel failover (`ha`) |
| valkey.sentinel.existingSecret | string | `""` | Secret with Sentinel's password (templated), required by the chart with Sentinel |
| valkey.sentinel.masterSet | string | `"mymaster"` |  |
| valkey.sentinel.passwordKey | string | `"sentinel"` | Key of the password in `existingSecret` |
| valkey.sentinel.port | int | `26379` |  |
| valkey.sentinel.service.port | int | `26379` |  |
| valkey.service.port | int | `6379` |  |
| valkeySettings.coalesceTimeoutMs | int | `500` | `COALESCE_TIMEOUT_MS`: how long the proxy waits for Valkey before publishing a webhook without coalescing |
| valkeySettings.commandTimeoutSeconds | int | `5` | `VALKEY_COMMAND_TIMEOUT_SECONDS` |
| valkeySettings.keyPrefix | string | `"koshin"` | `VALKEY_KEY_PREFIX` for koshin's coordination keys |
| valkeySettings.lockReplicaAckTimeoutMs | int | `0` | `LOCK_REPLICA_ACK_TIMEOUT_MS`: after acquiring a lock, wait this long for a replica to acknowledge it (`WAIT 1`). 0 disables it. |
| valkeySettings.queuedTtlSeconds | int | `86400` | `QUEUED_TTL_SECONDS`: how long a repo stays marked queued, so its webhooks are coalesced instead of published. Bounds how long a lost job (e.g. NATS restarting on `emptyDir`) blocks the repo's webhooks. |
| worker.affinity | object | `{}` | Worker affinity |
| worker.autoscaling.dispatchIntervalSeconds | int | `5` | How often the proxy reconciles burst workers when it publishes nothing (it also does after every publish) |
| worker.autoscaling.enabled | bool | `true` | The proxy starts burst worker `Job`s from the worker `PodTemplate` as jobs queue up, and they exit once the queue is drained. No KEDA: the proxy gets a namespaced Role on Jobs. Off: `worker.replicas` static workers. |
| worker.autoscaling.idleTimeoutSeconds | int | `10` | `WORKER_IDLE_TIMEOUT_SECONDS` of burst workers: exit once no job was runnable for this long |
| worker.autoscaling.jobActiveDeadlineSeconds | int | `21600` | `activeDeadlineSeconds` of burst worker Jobs |
| worker.autoscaling.jobTtlSecondsAfterFinished | int | `60` | `ttlSecondsAfterFinished` of burst worker Jobs |
| worker.autoscaling.maxLifetimeSeconds | int | `18000` | `WORKER_MAX_LIFETIME_SECONDS` of burst workers: past this, finish the job in flight and exit instead of fetching another. Keep it a run (`config.cliTimeoutMinutes`) below `jobActiveDeadlineSeconds`. |
| worker.autoscaling.maxReplicas | string | `nil` | Maximum workers, warm and burst (default: `rateLimits.maxConcurrentRunsGlobal`; more would only wait for a slot) |
| worker.autoscaling.minReplicas | int | `0` | Always-warm workers: `0` scales to zero (every job waits for a burst `Job` to start), `1` keeps one warm Deployment pod that picks a single job up at once. |
| worker.config.cliTimeoutMinutes | int | `45` | `CLI_TIMEOUT_MINUTES`: hard cap per Renovate run (`+ 5` must stay below 60 for GitHub App tokens) |
| worker.config.dirtyTtlSeconds | int | `3600` | `DIRTY_TTL_SECONDS` (worker and proxy); must exceed `cliTimeoutMinutes` |
| worker.config.heartbeatIntervalSeconds | int | `15` | `HEARTBEAT_INTERVAL_SECONDS`: heartbeat, lock and run-slot renewal interval |
| worker.config.lockCollisionRetrySeconds | int | `30` | `LOCK_COLLISION_RETRY_SECONDS`: NAK delay for a redelivered job that hits a held lock |
| worker.config.lockTtlSeconds | int | `45` | `LOCK_TTL_SECONDS`: per-repo lock TTL |
| worker.config.renovateBinarySource | string | `"global"` | `RENOVATE_BINARY_SOURCE` (tools come from the `full` Renovate image) |
| worker.existingSecret | string | `""` | Existing Secret with `GITHUB_TOKEN` and/or `GITLAB_TOKEN` (PAT fallback when the GitHub App isn't configured) |
| worker.extraEnv | list | `[]` | Extra environment variables for the worker (passed on to Renovate unless they're koshin secrets) |
| worker.image.digest | string | `""` | Worker image digest; takes precedence over the tag |
| worker.image.repository | string | `"hononeko/koshin-worker"` | Worker image repository. It holds koshin only: Renovate runs from `worker.renovate.image`. |
| worker.image.tag | string | `""` | Worker image tag (default: the chart's `appVersion`) |
| worker.nodeSelector | object | `{}` | Worker node selector |
| worker.podAnnotations | object | `{}` | Worker pod annotations |
| worker.podDisruptionBudget.enabled | string | `nil` | Create a PodDisruptionBudget for the workers (`maxUnavailable: 1`, for node drains). Default: on in `ha`. |
| worker.podLabels | object | `{}` | Worker pod labels |
| worker.podSecurityContext | object | `{"runAsGroup":0,"runAsNonRoot":true,"runAsUser":12021,"seccompProfile":{"type":"RuntimeDefault"}}` | Worker pod security context (both images run as Renovate's uid 12021) |
| worker.rateLimits.backoffBaseSeconds | int | `60` | `RATE_LIMIT_BACKOFF_BASE_SECONDS`: a credential scope's cooldown base |
| worker.rateLimits.backoffMaxSeconds | int | `3600` | `RATE_LIMIT_BACKOFF_MAX_SECONDS`: a credential scope's cooldown cap |
| worker.rateLimits.concurrentRequestLimit | string | `nil` | `RENOVATE_CONCURRENT_REQUEST_LIMIT`, injected into Renovate's `hostRules` for the platform API |
| worker.rateLimits.logPattern | string | `""` | `RATE_LIMIT_LOG_PATTERN`: regex for Renovate's rate-limit abort message (default `^Rate limit exceeded - aborting$`) |
| worker.rateLimits.maxConcurrentRunsGlobal | int | `6` | `MAX_CONCURRENT_RUNS_GLOBAL`: Renovate runs across all scopes and workers |
| worker.rateLimits.maxConcurrentRunsPerCredential | int | `2` | `MAX_CONCURRENT_RUNS_PER_CREDENTIAL`: Renovate runs per credential scope (a GitHub App installation, or a platform's token) across all workers |
| worker.rateLimits.maxRequestsPerSecond | string | `nil` | `RENOVATE_MAX_REQUESTS_PER_SECOND`, injected into Renovate's `hostRules` for the platform API |
| worker.rateLimits.maxRequeues | int | `5` | `MAX_RATE_LIMIT_REQUEUES` before a rate-limited job is dead-lettered |
| worker.renovate.image.digest | string | `""` | Renovate image digest; takes precedence over the tag |
| worker.renovate.image.pullPolicy | string | `""` | Pull policy of the Renovate image (default: `image.pullPolicy`) |
| worker.renovate.image.registry | string | `"ghcr.io"` | Registry of the Renovate image (empty: Docker Hub, e.g. with `repository: renovate/renovate`) |
| worker.renovate.image.repository | string | `"renovatebot/renovate"` | Renovate image repository. Use a `-full` image: it ships the tools Renovate runs (`binarySource=global`). |
| worker.renovate.image.tag | string | `"44.178.3-full"` | Renovate version, upgradable without a koshin release |
| worker.renovate.resources | object | `{"limits":{"memory":"4Gi"},"requests":{"cpu":"500m","memory":"1Gi"}}` | Resources of the `renovate` container, where Renovate runs |
| worker.renovateRedisUrl | string | `""` | `RENOVATE_REDIS_URL`: Renovate's package cache. Default: the `valkey-cache` subchart, else the coordination Valkey when it's a plain `redis://` URL. |
| worker.replicas | int | `1` | Worker replicas when `autoscaling.enabled` is false |
| worker.repoCache.bucket | string | `"koshin-repo-cache"` | `REPO_CACHE_BUCKET`: Object Store bucket |
| worker.repoCache.maxBytes | int | `536870912` | `REPO_CACHE_MAX_BYTES`: bucket size cap; it shares JetStream storage with the job streams |
| worker.repoCache.provider | string | `"nats"` | `REPO_CACHE_PROVIDER`: `nats` (Object Store sync), `s3` (native Renovate S3; single-tenant, so not with the GitHub App) or `local` (pod only) |
| worker.repoCache.s3.endpoint | string | `""` | `RENOVATE_S3_ENDPOINT`, e.g. `http://seaweedfs-s3:8333` |
| worker.repoCache.s3.existingSecret | string | `""` | Existing Secret with `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (and optionally `AWS_REGION`) |
| worker.repoCache.s3.pathStyle | bool | `true` | `RENOVATE_S3_PATH_STYLE` |
| worker.repoCache.s3.url | string | `""` | `RENOVATE_REPOSITORY_CACHE_TYPE`, e.g. `s3://renovate/repo-cache/` |
| worker.repoCache.ttlDays | int | `30` | `REPO_CACHE_TTL_DAYS`: drops caches of repos without a successful run for this long |
| worker.resources | object | `{"limits":{"memory":"768Mi"},"requests":{"cpu":"100m","memory":"256Mi"}}` | Resources of the `worker` container (koshin itself) |
| worker.secrets.githubToken | string | `""` | `GITHUB_TOKEN` |
| worker.secrets.gitlabToken | string | `""` | `GITLAB_TOKEN` |
| worker.securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}}` | Security context of the `worker` and `renovate` containers |
| worker.serviceAccount.annotations | object | `{}` | Worker ServiceAccount annotations |
| worker.terminationGracePeriodSeconds | int | `30` | Grace period covering the SIGTERM path (kill the CLI, release the lock, NAK), not a full run |
| worker.tolerations | list | `[]` | Worker tolerations |
| worker.topologySpreadConstraints | list | `[]` | Worker topology spread constraints |
| worker.workspaceSizeLimit | string | `"10Gi"` | Size of the `emptyDir` holding per-run workspaces and Renovate's caches |
