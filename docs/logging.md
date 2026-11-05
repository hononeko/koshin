# Logging

The proxy and worker print one JSON object per line (logstash-logback-encoder): `message`, `level`, `logger_name`,
`thread_name`, `stack_trace`, plus any key-value fields at the top level.

* **Job context:** while the worker handles a job, every line it logs carries `repo`, `deliveryId`, `triggerReason` and
  `deliveryAttempt`.
* **Renovate output:** each line Renovate prints (`LOG_FORMAT=json`) is logged at its own level, with Renovate's `msg`
  as the `message` and the whole line under `renovate` (`renovate.level`, `renovate.repository`, `renovate.err`, ...).
  Lines that aren't JSON are logged as they are.
* **Token scrubbing:** the worker replaces the token of the run in progress, and anything shaped like a GitHub or
  GitLab token (`ghs_…`, `ghp_…`, `github_pat_…`, `glpat-…`, `x-access-token:…`), with `**redacted**` in everything it
  logs: messages, fields, stack traces and Renovate's output, including what child tools (`git`, `npm`, `gpg`) print
  past Renovate's own sanitizer.
* **NATS client:** connection events and errors go through the same log (logger `dev.hononeko.koshin.nats`), not
  `java.util.logging`.
* **Levels:** `LOG_LEVEL` sets koshin's level and `ROOT_LOG_LEVEL` the libraries' (both `info` by default), read at
  startup, so changing them needs only a restart. Renovate's own level is `RENOVATE_LOG_LEVEL`; `LOG_LEVEL` isn't
  passed on to Renovate.
