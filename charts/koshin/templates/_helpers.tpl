{{/* Chart name */}}
{{- define "koshin.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Resource name prefix: the release name, plus the chart name unless it already contains it */}}
{{- define "koshin.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "koshin.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Labels; pass (dict "ctx" $ "component" "proxy") */}}
{{- define "koshin.labels" -}}
helm.sh/chart: {{ include "koshin.chart" .ctx }}
{{ include "koshin.selectorLabels" . }}
app.kubernetes.io/version: {{ .ctx.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .ctx.Release.Service }}
app.kubernetes.io/part-of: koshin
{{- with .ctx.Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "koshin.selectorLabels" -}}
app.kubernetes.io/name: {{ include "koshin.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
{{- with .component }}
app.kubernetes.io/component: {{ . }}
{{- end }}
{{- end }}

{{/* Image reference; pass (dict "ctx" $ "image" .Values.proxy.image) */}}
{{- define "koshin.image" -}}
{{- $repo := printf "%s/%s" .ctx.Values.image.registry .image.repository | trimPrefix "/" }}
{{- if .image.digest }}
{{- printf "%s@%s" $repo .image.digest }}
{{- else }}
{{- printf "%s:%s" $repo (.image.tag | default .ctx.Chart.AppVersion) }}
{{- end }}
{{- end }}

{{/* Full name of a subchart (mirrors the nats and valkey charts); pass (list $ "valkey-cache") */}}
{{- define "koshin.subchartFullname" -}}
{{- $ctx := index . 0 }}
{{- $chart := index . 1 }}
{{- $values := index $ctx.Values $chart | default dict }}
{{- if $values.fullnameOverride }}
{{- $values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default $chart $values.nameOverride }}
{{- if contains $name $ctx.Release.Name }}
{{- $ctx.Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" $ctx.Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "koshin.natsUrl" -}}
{{- if .Values.nats.enabled }}
{{- printf "nats://%s:%v" (include "koshin.subchartFullname" (list . "nats")) .Values.nats.config.nats.port }}
{{- else }}
{{- required "externalNats.url is required when nats.enabled is false" .Values.externalNats.url }}
{{- end }}
{{- end }}

{{/* KEDA resolves the endpoint from its own namespace, and in a cluster it reaches each node as <pod>.<endpoint> */}}
{{- define "koshin.natsMonitoringEndpoint" -}}
{{- if .Values.nats.enabled }}
{{- $headless := dig "headlessService" "name" "" .Values.nats | default (printf "%s-headless" (include "koshin.subchartFullname" (list . "nats"))) }}
{{- printf "%s.%s.svc.%s:%v" $headless .Release.Namespace .Values.clusterDomain .Values.nats.config.monitor.port }}
{{- else }}
{{- required "externalNats.monitoringEndpoint is required for KEDA when nats.enabled is false" .Values.externalNats.monitoringEndpoint }}
{{- end }}
{{- end }}

{{- define "koshin.natsStreamReplicas" -}}
{{- if not (kindIs "invalid" .Values.natsSettings.streamReplicas) }}
{{- .Values.natsSettings.streamReplicas }}
{{- else if and .Values.nats.enabled .Values.nats.config.cluster.enabled }}
{{- .Values.nats.config.cluster.replicas }}
{{- else if eq .Values.profile "ha" }}
{{- 3 }}
{{- else }}
{{- 1 }}
{{- end }}
{{- end }}

{{- define "koshin.valkeySentinelEnabled" -}}
{{- if and .Values.valkey.enabled .Values.valkey.sentinel.enabled }}true{{ end }}
{{- end }}

{{- define "koshin.valkeyUrl" -}}
{{- if .Values.valkey.enabled }}
{{- $fullname := include "koshin.subchartFullname" (list . "valkey") }}
{{- if .Values.valkey.sentinel.enabled }}
{{- $sentinel := printf "%s-sentinel" ($fullname | trunc 43 | trimSuffix "-") }}
{{- printf "redis-sentinel://%s:%v?sentinelMasterId=%s" $sentinel .Values.valkey.sentinel.service.port .Values.valkey.sentinel.masterSet }}
{{- else }}
{{- printf "redis://%s:%v" $fullname .Values.valkey.service.port }}
{{- end }}
{{- else }}
{{- required "externalValkey.url is required when valkey.enabled is false" .Values.externalValkey.url }}
{{- end }}
{{- end }}

{{/* RENOVATE_REDIS_URL, or empty to let the worker fall back on a plain redis:// VALKEY_URL */}}
{{- define "koshin.renovateRedisUrl" -}}
{{- if .Values.worker.renovateRedisUrl }}
{{- .Values.worker.renovateRedisUrl }}
{{- else if (index .Values "valkey-cache").enabled }}
{{- printf "redis://%s:%v" (include "koshin.subchartFullname" (list . "valkey-cache")) (index .Values "valkey-cache").service.port }}
{{- end }}
{{- end }}

{{/* VALKEY_SENTINEL_PASSWORD env entry, if Sentinel needs one */}}
{{- define "koshin.sentinelPasswordEnv" -}}
{{- if include "koshin.valkeySentinelEnabled" . }}
{{- $sentinel := .Values.valkey.sentinel }}
- name: VALKEY_SENTINEL_PASSWORD
  valueFrom:
    secretKeyRef:
      {{- if $sentinel.existingSecret }}
      name: {{ tpl $sentinel.existingSecret . }}
      key: {{ $sentinel.passwordKey }}
      {{- else }}
      name: {{ include "koshin.subchartFullname" (list . "valkey") }}-auth
      key: sentinel-password
      {{- end }}
{{- else if and (not .Values.valkey.enabled) .Values.externalValkey.sentinelPasswordSecret.name }}
- name: VALKEY_SENTINEL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.externalValkey.sentinelPasswordSecret.name }}
      key: {{ required "externalValkey.sentinelPasswordSecret.key is required" .Values.externalValkey.sentinelPasswordSecret.key }}
{{- end }}
{{- end }}

{{/* Name of the proxy Secret, existing or created from inline values; empty if neither */}}
{{- define "koshin.proxySecretName" -}}
{{- if .Values.proxy.existingSecret }}
{{- .Values.proxy.existingSecret }}
{{- else if include "koshin.proxyInlineSecrets" . }}
{{- printf "%s-proxy" (include "koshin.fullname" .) }}
{{- end }}
{{- end }}

{{- define "koshin.proxyInlineSecrets" -}}
{{- with .Values.proxy.secrets }}
{{- if or .githubWebhookSecret .gitlabWebhookSecret .internalTriggerToken }}true{{ end }}
{{- end }}
{{- end }}

{{- define "koshin.workerSecretName" -}}
{{- if .Values.worker.existingSecret }}
{{- .Values.worker.existingSecret }}
{{- else if include "koshin.workerInlineSecrets" . }}
{{- printf "%s-worker" (include "koshin.fullname" .) }}
{{- end }}
{{- end }}

{{- define "koshin.workerInlineSecrets" -}}
{{- with .Values.worker.secrets }}
{{- if or .githubToken .gitlabToken }}true{{ end }}
{{- end }}
{{- end }}

{{/* Whether the sweeper has a trigger token to use */}}
{{- define "koshin.sweeperEnabled" -}}
{{- if and .Values.sweeper.enabled (or .Values.proxy.existingSecret .Values.proxy.secrets.internalTriggerToken) }}true{{ end }}
{{- end }}

{{- define "koshin.githubAppSecretName" -}}
{{- if .Values.github.app.existingSecret }}
{{- .Values.github.app.existingSecret }}
{{- else if .Values.github.app.privateKey }}
{{- printf "%s-github-app" (include "koshin.fullname" .) }}
{{- end }}
{{- end }}

{{- define "koshin.githubAppKeyPath" -}}
/etc/koshin/github-app/private-key.pem
{{- end }}

{{/* GitHub App private key volume and mount, shared by the proxy and the worker */}}
{{- define "koshin.githubAppVolume" -}}
{{- with include "koshin.githubAppSecretName" . }}
- name: github-app
  secret:
    secretName: {{ . }}
    items:
      - key: {{ if $.Values.github.app.existingSecret }}{{ $.Values.github.app.privateKeyKey }}{{ else }}private-key.pem{{ end }}
        path: private-key.pem
{{- end }}
{{- end }}

{{- define "koshin.githubAppVolumeMount" -}}
{{- if include "koshin.githubAppSecretName" . }}
- name: github-app
  mountPath: /etc/koshin/github-app
  readOnly: true
{{- end }}
{{- end }}

{{- define "koshin.kedaEnabled" -}}
{{- if and .Values.worker.autoscaling.enabled (.Capabilities.APIVersions.Has "keda.sh/v1alpha1/ScaledObject") }}true{{ end }}
{{- end }}

{{- define "koshin.workerMaxReplicas" -}}
{{- .Values.worker.autoscaling.maxReplicas | default (mul .Values.worker.rateLimits.maxConcurrentRunsPerPlatform (len .Values.platforms)) }}
{{- end }}
