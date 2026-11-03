{{/*
A worker pod (`metadata` and `spec`), shared by the burst worker PodTemplate and the warm or static Deployment (KSH-32).
Pass (dict "ctx" $ "mode" "burst|warm|static" "restartPolicy" "Never|Always" "idleTimeoutSeconds" 10
"maxLifetimeSeconds" 18000).
*/}}
{{- define "koshin.workerPod" -}}
{{- $ctx := .ctx -}}
metadata:
  labels:
    {{- include "koshin.labels" (dict "ctx" $ctx "component" "worker") | nindent 4 }}
    koshin.hononeko.dev/worker-mode: {{ .mode }}
    {{- with $ctx.Values.worker.podLabels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  annotations:
    checksum/config: {{ include (print $ctx.Template.BasePath "/configmap.yaml") $ctx | sha256sum }}
    checksum/secrets: {{ include (print $ctx.Template.BasePath "/secrets.yaml") $ctx | sha256sum }}
    {{- with $ctx.Values.worker.podAnnotations }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  restartPolicy: {{ .restartPolicy }}
  serviceAccountName: {{ include "koshin.fullname" $ctx }}-worker
  # Workers never call the Kubernetes API (KSH-31): no container gets a token
  automountServiceAccountToken: false
  terminationGracePeriodSeconds: {{ $ctx.Values.worker.terminationGracePeriodSeconds }}
  {{- with $ctx.Values.imagePullSecrets }}
  imagePullSecrets:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  securityContext:
    {{- toYaml $ctx.Values.worker.podSecurityContext | nindent 4 }}
  {{- with $ctx.Values.worker.topologySpreadConstraints }}
  topologySpreadConstraints:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $ctx.Values.worker.nodeSelector }}
  nodeSelector:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $ctx.Values.worker.tolerations }}
  tolerations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $ctx.Values.worker.affinity }}
  affinity:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  initContainers:
    # Copies the Renovate runner (JRE, koshin's jars, scripts) from the worker image for the `renovate` container
    - name: koshin-runner-init
      image: {{ include "koshin.image" (dict "ctx" $ctx "image" $ctx.Values.worker.image) }}
      imagePullPolicy: {{ $ctx.Values.image.pullPolicy }}
      command: ["/opt/koshin/bin/koshin-runner-init", "/opt/koshin-dist"]
      resources:
        requests:
          cpu: 50m
          memory: 32Mi
        limits:
          memory: 256Mi
      securityContext:
        {{- toYaml $ctx.Values.worker.securityContext | nindent 8 }}
      volumeMounts:
        - name: runner-bin
          mountPath: /opt/koshin-dist
    # Renovate, and any repo code it runs, live in this container: no worker secrets, no ServiceAccount token.
    # The worker hands it each run over the runner socket. A native sidecar: it starts before the worker and
    # stops after it, so the SIGTERM path still kills the run itself.
    - name: renovate
      {{- with $ctx.Values.worker.renovate.image }}
      image: {{ include "koshin.image" (dict "ctx" $ctx "image" .) }}
      imagePullPolicy: {{ .pullPolicy | default $ctx.Values.image.pullPolicy }}
      {{- end }}
      restartPolicy: Always
      # containerbase's entrypoint, without Renovate's, which would take a missing runner for a repository name
      command: ["/usr/local/sbin/docker-entrypoint.sh", "/opt/koshin/bin/koshin-runner"]
      env:
        - name: RUNNER_SOCKET
          value: {{ include "koshin.runnerSocket" $ctx }}
      startupProbe:
        exec:
          command: ["test", "-S", {{ include "koshin.runnerSocket" $ctx | quote }}]
        periodSeconds: 2
        failureThreshold: 60
      resources:
        {{- toYaml $ctx.Values.worker.renovate.resources | nindent 8 }}
      securityContext:
        {{- toYaml $ctx.Values.worker.securityContext | nindent 8 }}
      volumeMounts:
        - name: renovate
          mountPath: /tmp/renovate
        - name: runner
          mountPath: /run/koshin
        # Read-only, so a run can't change the runner for the next one
        - name: runner-bin
          mountPath: /opt/koshin
          readOnly: true
  containers:
    - name: worker
      image: {{ include "koshin.image" (dict "ctx" $ctx "image" $ctx.Values.worker.image) }}
      imagePullPolicy: {{ $ctx.Values.image.pullPolicy }}
      ports:
        - name: http
          containerPort: 8080
      envFrom:
        - configMapRef:
            name: {{ include "koshin.fullname" $ctx }}-config
        {{- with include "koshin.workerSecretName" $ctx }}
        # GITHUB_TOKEN, GITLAB_TOKEN
        - secretRef:
            name: {{ . }}
        {{- end }}
        {{- if and (eq $ctx.Values.worker.repoCache.provider "s3") $ctx.Values.worker.repoCache.s3.existingSecret }}
        # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION for Renovate's S3 repository cache
        - secretRef:
            name: {{ $ctx.Values.worker.repoCache.s3.existingSecret }}
        {{- end }}
      env:
        - name: RENOVATE_BASE_DIR
          value: /tmp/renovate
        - name: RUNNER_SOCKET
          value: {{ include "koshin.runnerSocket" $ctx }}
        # 0: run until stopped (a warm or static worker); > 0: drain the queue, then exit 0 (a burst Job)
        - name: WORKER_IDLE_TIMEOUT_SECONDS
          value: {{ .idleTimeoutSeconds | quote }}
        - name: WORKER_MAX_LIFETIME_SECONDS
          value: {{ .maxLifetimeSeconds | quote }}
        {{- with include "koshin.sentinelPasswordEnv" $ctx }}{{ . | nindent 8 }}{{- end }}
        {{- with $ctx.Values.worker.extraEnv }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      readinessProbe:
        httpGet:
          path: /readyz
          port: http
        periodSeconds: 10
      livenessProbe:
        httpGet:
          path: /healthz
          port: http
        initialDelaySeconds: 10
        periodSeconds: 20
      resources:
        {{- toYaml $ctx.Values.worker.resources | nindent 8 }}
      securityContext:
        {{- toYaml $ctx.Values.worker.securityContext | nindent 8 }}
      volumeMounts:
        - name: renovate
          mountPath: /tmp/renovate
        # Connecting needs no write access
        - name: runner
          mountPath: /run/koshin
          readOnly: true
        {{- with include "koshin.githubAppVolumeMount" $ctx }}{{ . | nindent 8 }}{{- end }}
  volumes:
    # Per-run workspaces, deleted after each run and purged at startup
    - name: renovate
      emptyDir:
        sizeLimit: {{ $ctx.Values.worker.workspaceSizeLimit }}
    - name: runner
      emptyDir:
        medium: Memory
        sizeLimit: 1Mi
    # The runner, filled by koshin-runner-init
    - name: runner-bin
      emptyDir:
        sizeLimit: 512Mi
    {{- with include "koshin.githubAppVolume" $ctx }}{{ . | nindent 4 }}{{- end }}
{{- end }}
