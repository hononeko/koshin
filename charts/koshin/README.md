# koshin

Distributed Renovate orchestrator - a stateless webhook proxy and KEDA-scaled workers on NATS JetStream and Valkey

![Version: 0.8.0](https://img.shields.io/badge/Version-0.8.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.8.0](https://img.shields.io/badge/AppVersion-0.8.0-informational?style=flat-square)

This chart is the primary way to install koshin. The Kustomize manifests in `k8s/` stay as a small, dependency-free
reference for the `minimal` profile.

## Prerequisites

* Kubernetes 1.29+
* [KEDA](https://keda.sh) 2.21+ to scale workers with the job backlog. Without it the chart runs a fixed
  `worker.replicas` (the `ScaledObject` is rendered only when the `keda.sh/v1alpha1` API exists, so pass
  `--api-versions keda.sh/v1alpha1/ScaledObject` to `helm template`).
* The worker image is about 10 GB uncompressed: it bundles a pinned Renovate (`renovate:<version>-full`) and its tools.

## Install

Create the secrets first. The chart never needs them inline:

```sh
kubectl create namespace koshin
kubectl -n koshin create secret generic koshin-proxy \
  --from-literal=GITHUB_WEBHOOK_SECRET=... \
  --from-literal=INTERNAL_TRIGGER_TOKEN="$(openssl rand -hex 24)"
kubectl -n koshin create secret generic koshin-worker --from-literal=GITHUB_TOKEN=...
```

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
and spread across nodes. The package-cache Valkey stays a single, disposable instance.

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
  # For KEDA; for a cluster, the headless Service so KEDA can reach each node
  monitoringEndpoint: nats-headless.messaging.svc.cluster.local:8222
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
| A NATS or Valkey pod restarts | pending jobs, locks, dirty flags and repo caches are lost; the sweeper re-enqueues every repository within 6 hours | nothing is lost: NATS elects a new stream leader, Sentinel promotes a Valkey replica |
| A node is drained | same as a restart | PodDisruptionBudgets keep 2 of 3 NATS, Valkey and Sentinel pods up |

`minimal` suits small installs: everything it loses is either recovered by the sweeper or rebuilt by the next run.
Pick `ha` when webhooks must not wait up to 6 hours after an outage, or when cold repo caches after every restart cost
too much API budget.

During a Valkey failover Sentinel promotes a replica within about 15 seconds, and the workers reconnect through
Sentinel. Replication is asynchronous, so a lock taken just before the failover can be lost: the holder's next
heartbeat finds the lock gone or taken, kills its Renovate run and NAKs the job for a retry. A NATS leader election pauses
publishing for a few seconds; the proxy retries, so webhooks are still accepted.

## Secrets

| Secret | Keys | Value |
| :--- | :--- | :--- |
| Proxy | `GITHUB_WEBHOOK_SECRET`, `GITLAB_WEBHOOK_SECRET`, `INTERNAL_TRIGGER_TOKEN` | `proxy.existingSecret` |
| Worker | `GITHUB_TOKEN`, `GITLAB_TOKEN` | `worker.existingSecret` |
| GitHub App | the private key (PEM) under `github.app.privateKeyKey` | `github.app.existingSecret` |
| S3 repo cache | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | `worker.repoCache.s3.existingSecret` |
| Sentinel (`ha`) | `sentinel` | `valkey.sentinel.existingSecret` (`<release>-valkey-sentinel`) |

The inline `*.secrets.*` and `github.app.privateKey` values exist for testing only. Without `INTERNAL_TRIGGER_TOKEN`
the sweeper CronJob isn't installed.

## Repository cache

`worker.repoCache.provider` picks where Renovate's repository cache lives between runs: `nats` (default, an Object Store
bucket in the release's NATS, capped by `maxBytes` since it shares JetStream storage with the job streams), `s3`
(Renovate reads and writes it itself, e.g. SeaweedFS) or `local` (pod only):

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
| Worker | 1 to `autoscaling.maxReplicas` | 500m CPU, 1 Gi | 4 Gi | 10 Gi `emptyDir`, plus the ~10 GB image on each node |
| NATS | 1 / 3 | 50m CPU, 64 Mi | 256 Mi | 2 Gi `emptyDir` / 10 Gi PVC |
| Valkey (coordination) | 1 / 3 | 50m CPU, 32 Mi | 128 Mi | `emptyDir` / 1 Gi PVC |
| Sentinel | — / 3 | unset (`valkey.sentinel.resources`) | unset | memory-backed `emptyDir` |
| Valkey (package cache) | 1 / 1 | 50m CPU, 128 Mi | 512 Mi | `emptyDir` |

Workers dominate: each runs one Renovate process, and large repositories (monorepos, many package managers) may need
more than 4 Gi. Keep the worker image on nodes that run workers (e.g. with an image cache or Spegel), since pulling
10 GB delays every scale-up.

JetStream storage is mostly the repo cache: jobs are small and leave the stream once acknowledged. Keep
`worker.repoCache.maxBytes` well below `nats.config.jetstream.fileStore.maxSize` (1 Gi in `minimal`, 8 Gi in `ha`) so
the job streams always have room. With `ha` you can raise it to a few Gi.

The API budget is roughly run slots × requests per second per run. Keep `worker.autoscaling.maxReplicas` near
`worker.rateLimits.maxConcurrentRunsPerPlatform` × platforms (the default): extra workers only wait for a slot. During
a rate-limit cooldown KEDA still counts the pending jobs and may scale up workers that sit idle until it ends.

## Backups and recovery

koshin keeps no state that has to be backed up; after a total loss, reinstall and the sweeper re-enqueues every
repository.

* **Coordination Valkey:** locks, dirty flags, run slots and cooldowns expire within an hour, and the rate-limit
  backoff counter resets after the next successful run. In `ha` the AOF (`appendfsync everysec`) on the PVCs carries
  them across restarts. Don't restore an old copy: stale locks and cooldowns would only delay runs.
* **NATS:** pending jobs are worth keeping across restarts (`ha` replicates them 3 ways), but not in a backup: by the
  time you restore, the sweeper has re-enqueued them. The repo cache bucket is a cache that rebuilds on the next
  successful run of each repository. Back up the dead-letter stream if you want to keep failed jobs for inspection:

  ```sh
  nats backup stream RENOVATE_DLQ ./koshin-dlq
  nats backup restore stream ./koshin-dlq
  ```

  Crash-consistent volume snapshots of the JetStream PVCs work too.
* **Package-cache Valkey:** a cache; nothing to back up.
* **Secrets:** keep the webhook secrets, the trigger token, the platform tokens and the Sentinel password in your secret
  manager.

## Uninstall

`helm uninstall` leaves the PersistentVolumeClaims of the NATS and Valkey StatefulSets behind (`ha` only; `minimal` has
none), so a reinstall finds its data again. To remove them as well:

```sh
helm uninstall koshin -n koshin
kubectl -n koshin delete pvc -l app.kubernetes.io/instance=koshin
```

With a `Retain` storage class, the PersistentVolumes stay until you delete them too. The Secrets you created for the
chart are yours to delete.

## Requirements

Kubernetes: `>=1.29.0-0`

| Repository | Name | Version |
|------------|------|---------|
| https://nats-io.github.io/k8s/helm/charts/ | nats | 2.15.2 |
| https://valkey.io/valkey-helm/ | valkey | 1.0.0-rc.1 |
| https://valkey.io/valkey-helm/ | valkey-cache(valkey) | 1.0.0-rc.1 |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| clusterDomain | string | `"cluster.local"` | Kubernetes cluster domain, used for the KEDA monitoring endpoint |
| commonLabels | object | `{}` | Labels added to every koshin resource |
| externalNats | object | `{"monitoringEndpoint":"","url":""}` | NATS server used when `nats.enabled` is false |
| externalNats.monitoringEndpoint | string | `""` | NATS monitoring endpoint (`host:8222`) for the KEDA scaler; for a cluster, the headless Service so KEDA can reach each node, e.g. `nats-headless.messaging.svc.cluster.local:8222` |
| externalNats.url | string | `""` | NATS URL, e.g. `nats://nats.messaging:4222` |
| externalValkey | object | `{"sentinelPasswordSecret":{"key":"","name":""},"sentinelUsername":"","url":""}` | Coordination Valkey used when `valkey.enabled` is false |
| externalValkey.sentinelPasswordSecret | object | `{"key":"","name":""}` | Secret with Sentinel's own password (`VALKEY_SENTINEL_PASSWORD`), if Sentinel requires one |
| externalValkey.sentinelUsername | string | `""` | `VALKEY_SENTINEL_USERNAME`: Sentinel's ACL user, if it has one (empty: the default user) |
| externalValkey.url | string | `""` | `VALKEY_URL`: `redis://host:6379` or `redis-sentinel://host:26379?sentinelMasterId=mymaster` |
| extraObjects | list | `[]` | Extra manifests rendered with the release (templated), e.g. an Envoy Gateway `ClientTrafficPolicy` for HTTP/3 |
| fullnameOverride | string | `""` | Overrides the full resource name prefix (default: the release name, plus `-koshin` unless it contains it) |
| github.apiUrl | string | `""` | `GITHUB_API_URL` for GitHub Enterprise Server (default `https://api.github.com`) |
| github.app.existingSecret | string | `""` | Existing Secret holding the App's private key (PEM), mounted for both the proxy and the worker |
| github.app.id | string | `""` | `GITHUB_APP_ID` |
| github.app.privateKey | string | `""` | Inline private key, for testing only; ignored when `existingSecret` is set |
| github.app.privateKeyKey | string | `"private-key.pem"` | Key of the private key in `existingSecret` |
| github.botLogin | string | `""` | `GITHUB_BOT_LOGIN`: webhooks sent by this login are ignored, so Renovate's own pushes don't re-trigger it |
| gitlab.botLogin | string | `""` | `GITLAB_BOT_LOGIN`: webhooks sent by this login are ignored |
| gitlab.endpoint | string | `""` | `GITLAB_ENDPOINT`, e.g. `https://gitlab.example.com/api/v4/` |
| http2Enabled | bool | `true` | `HTTP2_ENABLED`: cleartext HTTP/2 (h2c) next to HTTP/1.1 on the proxy and worker |
| image.pullPolicy | string | `"IfNotPresent"` | Pull policy of the koshin images |
| image.registry | string | `"ghcr.io"` | Registry of the koshin images |
| imagePullSecrets | list | `[]` | Image pull secrets for koshin's own pods |
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
| natsSettings.maxDeliver | int | `5` | `NATS_MAX_DELIVER`: deliveries of a job before it's dead-lettered |
| natsSettings.streamName | string | `"RENOVATE_JOBS"` | `NATS_STREAM_NAME`: the job stream (WorkQueue) |
| natsSettings.streamReplicas | string | `nil` | `NATS_STREAM_REPLICAS` for the job and DLQ streams and the repo cache bucket. Default: the NATS cluster size (`nats.config.cluster.replicas` when clustered), else 1. |
| networkPolicy.enabled | bool | `false` | Create NetworkPolicies: the proxy accepts webhooks from anywhere, and NATS/Valkey accept only this release's pods (plus KEDA on the NATS monitoring port) |
| networkPolicy.kedaNamespaceSelector | object | `{"matchLabels":{"kubernetes.io/metadata.name":"keda"}}` | Namespace selector of the KEDA operator, allowed to reach NATS monitoring |
| networkPolicy.proxyFrom | list | `[]` | Sources allowed to reach the proxy (default: anywhere) |
| platforms | list | `["github"]` | Enabled platforms (`github`, `gitlab`). Each gets a KEDA trigger and its own NATS consumer. |
| profile | string | `"minimal"` | Deployment profile: `minimal` (single NATS and Valkey on `emptyDir`) or `ha` (3-node NATS on PVCs, Valkey with Sentinel). Install `ha` with `-f values-ha.yaml`: Helm can't switch subchart defaults from a single value, so the file sets both this and the NATS/Valkey topology, and the chart refuses to render a profile that doesn't match them. |
| proxy.affinity | object | `{}` | Proxy affinity |
| proxy.existingSecret | string | `""` | Existing Secret with any of `GITHUB_WEBHOOK_SECRET`, `GITLAB_WEBHOOK_SECRET` and `INTERNAL_TRIGGER_TOKEN`. With it set, the sweeper (and `tests.jobProcessing`) expect `INTERNAL_TRIGGER_TOKEN` in it. |
| proxy.extraEnv | list | `[]` | Extra environment variables for the proxy |
| proxy.httpRoute.annotations | object | `{}` | HTTPRoute annotations |
| proxy.httpRoute.enabled | bool | `false` | Expose the webhook endpoints through a Gateway API HTTPRoute |
| proxy.httpRoute.hostnames | list | `[]` | Hostnames of the route |
| proxy.httpRoute.parentRefs | list | `[]` | Gateways to attach to |
| proxy.httpRoute.pathPrefixes | list | `["/webhooks/"]` | Path prefixes routed to the proxy |
| proxy.image.digest | string | `""` | Proxy image digest; takes precedence over the tag |
| proxy.image.repository | string | `"hononeko/koshin-proxy"` | Proxy image repository |
| proxy.image.tag | string | `""` | Proxy image tag (default: the chart's `appVersion`) |
| proxy.ingress.annotations | object | `{}` | Ingress annotations |
| proxy.ingress.className | string | `""` | Ingress class |
| proxy.ingress.enabled | bool | `false` | Expose the webhook endpoints through an Ingress |
| proxy.ingress.hosts | list | `[{"host":"koshin.example.com","paths":[{"path":"/webhooks/","pathType":"Prefix"}]}]` | Hosts and paths. Only `/webhooks/` is exposed by default: `/api/v1/trigger` stays cluster-internal. |
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
| proxy.sweepRepositories | list | `[]` | Repositories the sweeper schedules (`github:org/repo` or `gitlab:group/project`), served to the proxy as `SWEEP_REPOSITORIES_FILE` |
| proxy.tolerations | list | `[]` | Proxy tolerations |
| proxy.topologySpreadConstraints | list | `[]` | Proxy topology spread constraints (default: spread across nodes) |
| renovateBotLogins | list | `[]` | `RENOVATE_BOT_LOGINS`: more bot logins whose webhooks are ignored |
| sweeper.enabled | bool | `true` | Schedule a run of every discovered repository on `schedule`. Needs `INTERNAL_TRIGGER_TOKEN`; without one the CronJob isn't rendered. |
| sweeper.image.repository | string | `"docker.io/curlimages/curl"` | Sweeper image (any image with curl) |
| sweeper.image.tag | string | `"8.23.0"` | Sweeper image tag |
| sweeper.resources | object | `{"limits":{"memory":"64Mi"},"requests":{"cpu":"10m","memory":"16Mi"}}` | Sweeper resources |
| sweeper.schedule | string | `"0 */6 * * *"` | Cron schedule |
| tests.jobProcessing.enabled | bool | `false` | `helm test` also triggers a Renovate run of `repository` and waits until a worker acknowledges it. Off by default because it starts a real run; needs `INTERNAL_TRIGGER_TOKEN`. |
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
| valkeySettings.commandTimeoutSeconds | int | `5` | `VALKEY_COMMAND_TIMEOUT_SECONDS` |
| valkeySettings.keyPrefix | string | `"koshin"` | `VALKEY_KEY_PREFIX` for koshin's coordination keys |
| valkeySettings.lockReplicaAckTimeoutMs | int | `0` | `LOCK_REPLICA_ACK_TIMEOUT_MS`: after acquiring a lock, wait this long for a replica to acknowledge it (`WAIT 1`). 0 disables it. |
| worker.affinity | object | `{}` | Worker affinity |
| worker.autoscaling.enabled | bool | `true` | Scale workers with KEDA (one per active or pending job). Rendered only when the `keda.sh/v1alpha1` API exists. |
| worker.autoscaling.maxReplicas | string | `nil` | Maximum workers (default: `maxConcurrentRunsPerPlatform` × enabled platforms; more would only wait for a slot) |
| worker.autoscaling.minReplicas | int | `1` | Minimum workers |
| worker.autoscaling.natsAccount | string | `"$G"` | KEDA trigger `account` (`$G` without NATS accounts) |
| worker.autoscaling.pollingInterval | int | `15` | KEDA polling interval (seconds) |
| worker.autoscaling.scaleDownStabilizationSeconds | int | `60` | Scale-down stabilization window (seconds) |
| worker.config.cliTimeoutMinutes | int | `45` | `CLI_TIMEOUT_MINUTES`: hard cap per Renovate run (`+ 5` must stay below 60 for GitHub App tokens) |
| worker.config.dirtyTtlSeconds | int | `3600` | `DIRTY_TTL_SECONDS`; must exceed `cliTimeoutMinutes` |
| worker.config.heartbeatIntervalSeconds | int | `15` | `HEARTBEAT_INTERVAL_SECONDS`: heartbeat, lock and run-slot renewal interval |
| worker.config.lockCollisionRetrySeconds | int | `30` | `LOCK_COLLISION_RETRY_SECONDS`: NAK delay for a redelivered job that hits a held lock |
| worker.config.lockTtlSeconds | int | `45` | `LOCK_TTL_SECONDS`: per-repo lock TTL |
| worker.config.renovateBinarySource | string | `"global"` | `RENOVATE_BINARY_SOURCE` (tools come from the `full` image) |
| worker.existingSecret | string | `""` | Existing Secret with `GITHUB_TOKEN` and/or `GITLAB_TOKEN` (PAT fallback when the GitHub App isn't configured) |
| worker.extraEnv | list | `[]` | Extra environment variables for the worker (passed on to Renovate unless they're koshin secrets) |
| worker.image.digest | string | `""` | Worker image digest; takes precedence over the tag |
| worker.image.repository | string | `"hononeko/koshin-worker"` | Worker image repository. The image bundles a pinned Renovate (`renovate:<version>-full`). |
| worker.image.tag | string | `""` | Worker image tag (default: the chart's `appVersion`) |
| worker.nodeSelector | object | `{}` | Worker node selector |
| worker.podAnnotations | object | `{}` | Worker pod annotations |
| worker.podDeletionCost.enabled | bool | `true` | `POD_DELETION_COST_ENABLED`: mark busy workers expensive to delete, so KEDA scale-in removes idle ones |
| worker.podDisruptionBudget.enabled | string | `nil` | Create a PodDisruptionBudget for the workers (`maxUnavailable: 1`, for node drains). Default: on in `ha`. |
| worker.podLabels | object | `{}` | Worker pod labels |
| worker.podSecurityContext | object | `{"runAsGroup":0,"runAsNonRoot":true,"runAsUser":12021,"seccompProfile":{"type":"RuntimeDefault"}}` | Worker pod security context (Renovate's `full` image runs as uid 12021) |
| worker.rateLimits.backoffBaseSeconds | int | `60` | `RATE_LIMIT_BACKOFF_BASE_SECONDS`: platform cooldown base |
| worker.rateLimits.backoffMaxSeconds | int | `3600` | `RATE_LIMIT_BACKOFF_MAX_SECONDS`: platform cooldown cap |
| worker.rateLimits.concurrentRequestLimit | string | `nil` | `RENOVATE_CONCURRENT_REQUEST_LIMIT`, injected into Renovate's `hostRules` for the platform API |
| worker.rateLimits.logPattern | string | `""` | `RATE_LIMIT_LOG_PATTERN`: regex for Renovate's rate-limit abort message (default `^Rate limit exceeded - aborting$`) |
| worker.rateLimits.maxConcurrentRunsPerPlatform | int | `3` | `MAX_CONCURRENT_RUNS_PER_PLATFORM`: Renovate runs per platform across all workers |
| worker.rateLimits.maxRequestsPerSecond | string | `nil` | `RENOVATE_MAX_REQUESTS_PER_SECOND`, injected into Renovate's `hostRules` for the platform API |
| worker.rateLimits.maxRequeues | int | `5` | `MAX_RATE_LIMIT_REQUEUES` before a rate-limited job is dead-lettered |
| worker.renovateRedisUrl | string | `""` | `RENOVATE_REDIS_URL`: Renovate's package cache. Default: the `valkey-cache` subchart, else the coordination Valkey when it's a plain `redis://` URL. |
| worker.replicas | int | `1` | Worker replicas when KEDA isn't installed or `autoscaling.enabled` is false |
| worker.repoCache.bucket | string | `"koshin-repo-cache"` | `REPO_CACHE_BUCKET`: Object Store bucket |
| worker.repoCache.maxBytes | int | `536870912` | `REPO_CACHE_MAX_BYTES`: bucket size cap; it shares JetStream storage with the job streams |
| worker.repoCache.provider | string | `"nats"` | `REPO_CACHE_PROVIDER`: `nats` (Object Store sync), `s3` (native Renovate S3) or `local` (pod only) |
| worker.repoCache.s3.endpoint | string | `""` | `RENOVATE_S3_ENDPOINT`, e.g. `http://seaweedfs-s3:8333` |
| worker.repoCache.s3.existingSecret | string | `""` | Existing Secret with `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (and optionally `AWS_REGION`) |
| worker.repoCache.s3.pathStyle | bool | `true` | `RENOVATE_S3_PATH_STYLE` |
| worker.repoCache.s3.url | string | `""` | `RENOVATE_REPOSITORY_CACHE_TYPE`, e.g. `s3://renovate/repo-cache/` |
| worker.repoCache.ttlDays | int | `30` | `REPO_CACHE_TTL_DAYS`: drops caches of repos without a successful run for this long |
| worker.resources | object | `{"limits":{"memory":"4Gi"},"requests":{"cpu":"500m","memory":"1Gi"}}` | Worker resources |
| worker.secrets.githubToken | string | `""` | `GITHUB_TOKEN` |
| worker.secrets.gitlabToken | string | `""` | `GITLAB_TOKEN` |
| worker.securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}}` | Worker container security context |
| worker.serviceAccount.annotations | object | `{}` | Worker ServiceAccount annotations |
| worker.terminationGracePeriodSeconds | int | `30` | Grace period covering the SIGTERM path (kill the CLI, release the lock, NAK), not a full run |
| worker.tolerations | list | `[]` | Worker tolerations |
| worker.topologySpreadConstraints | list | `[]` | Worker topology spread constraints |
| worker.workspaceSizeLimit | string | `"10Gi"` | Size of the `emptyDir` holding per-run workspaces and Renovate's caches |
