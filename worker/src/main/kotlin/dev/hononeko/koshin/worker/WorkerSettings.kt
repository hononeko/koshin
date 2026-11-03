package dev.hononeko.koshin.worker

import dev.hononeko.koshin.adapters.valkey.PlatformThrottleConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyConfig
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.usecase.WorkerConfig
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

data class WorkerSettings(
    val port: Int = DEFAULT_PORT,
    val http2Enabled: Boolean = DEFAULT_HTTP2_ENABLED,
    val platforms: Set<Platform> = DEFAULT_PLATFORMS,
    val natsUrl: String = DEFAULT_NATS_URL,
    val renovateBaseDir: Path = Path(DEFAULT_RENOVATE_BASE_DIR),
    val renovateCacheDir: Path = renovateBaseDir.resolve(RENOVATE_CACHE_SUBDIR),
    val renovateBinarySource: String = DEFAULT_RENOVATE_BINARY_SOURCE,
    val renovateRedisUrl: String? = null,
    /** The Renovate runner's socket (`RUNNER_SOCKET`); without one, Renovate runs in this container. */
    val runnerSocket: Path? = null,
    val githubToken: String? = null,
    val githubEndpoint: String? = null,
    val gitlabToken: String? = null,
    val gitlabEndpoint: String? = null,
    /**
     * `WORKER_IDLE_TIMEOUT_SECONDS` (KSH-31): zero runs as a daemon until stopped, keeping a warm-worker lease;
     * positive drains runnable jobs back to back and exits 0 once none was found for this long (a burst `Job` pod).
     */
    val idleTimeout: Duration = Duration.ZERO,
    /** `WORKER_MAX_LIFETIME_SECONDS`: past this, finish the job in flight and exit 0 instead of fetching another. */
    val maxLifetime: Duration = Duration.ZERO,
    val jobConfig: WorkerConfig = WorkerConfig(),
    val throttle: PlatformThrottleConfig = PlatformThrottleConfig(),
    val requestLimits: RenovateRequestLimits = RenovateRequestLimits(),
    val rateLimitLogPattern: String = RateLimitDetector.DEFAULT_ABORT_PATTERN,
    val repoCache: RepoCacheSettings = RepoCacheSettings(),
) {
    init {
        require(port in 0..MAX_PORT) { "port must be in 0..$MAX_PORT: $port" }
        require(platforms.isNotEmpty()) { "platforms cannot be empty" }
        require(natsUrl.isNotBlank()) { "natsUrl cannot be blank" }
        require(renovateBaseDir.isAbsolute) { "RENOVATE_BASE_DIR must be absolute: $renovateBaseDir" }
        require(renovateCacheDir.isAbsolute) { "RENOVATE_CACHE_DIR must be absolute: $renovateCacheDir" }
        require(renovateBinarySource.isNotBlank()) { "renovateBinarySource cannot be blank" }
        require(runnerSocket?.isAbsolute != false) { "RUNNER_SOCKET must be absolute: $runnerSocket" }
        require(!idleTimeout.isNegative() && idleTimeout.isFinite()) {
            "WORKER_IDLE_TIMEOUT_SECONDS must be >= 0: $idleTimeout"
        }
        require(!maxLifetime.isNegative() && maxLifetime.isFinite()) {
            "WORKER_MAX_LIFETIME_SECONDS must be >= 0: $maxLifetime"
        }
        Regex(rateLimitLogPattern)
    }

    override fun toString(): String = "WorkerSettings(port=$port, http2Enabled=$http2Enabled, platforms=$platforms, " +
        "natsUrl=${natsUrl.withoutUserInfo()}, renovateBaseDir=$renovateBaseDir, runnerSocket=$runnerSocket, " +
        "idleTimeout=$idleTimeout, maxLifetime=$maxLifetime, " +
        "githubToken=${githubToken.redacted()}, gitlabToken=${gitlabToken.redacted()}, jobConfig=$jobConfig, " +
        "throttle=$throttle, requestLimits=$requestLimits, repoCache=$repoCache)"

    companion object {
        const val DEFAULT_PORT = 8080
        const val MAX_PORT = 65535
        const val DEFAULT_HTTP2_ENABLED = true
        const val DEFAULT_NATS_URL = "nats://koshin-nats:4222"
        const val DEFAULT_RENOVATE_BASE_DIR = "/tmp/renovate"
        const val DEFAULT_RENOVATE_BINARY_SOURCE = "global"

        /** Renovate's default `cacheDir` under `baseDir`. */
        const val RENOVATE_CACHE_SUBDIR = "cache"
        val DEFAULT_PLATFORMS: Set<Platform> = setOf(Platform.GITHUB)

        fun fromEnv(env: Map<String, String> = System.getenv()): WorkerSettings {
            val baseDir = Path(env.nonBlank("RENOVATE_BASE_DIR") ?: DEFAULT_RENOVATE_BASE_DIR)
            return fromEnv(env, baseDir)
        }

        private fun fromEnv(env: Map<String, String>, baseDir: Path) = WorkerSettings(
            port = env.long("PORT")?.toInt() ?: DEFAULT_PORT,
            http2Enabled = env.boolean("HTTP2_ENABLED") ?: DEFAULT_HTTP2_ENABLED,
            platforms = parsePlatforms(env["PLATFORMS"]),
            natsUrl = env.nonBlank("NATS_URL") ?: DEFAULT_NATS_URL,
            renovateBaseDir = baseDir,
            renovateCacheDir = env.nonBlank("RENOVATE_CACHE_DIR")?.let { Path(it) }
                ?: baseDir.resolve(RENOVATE_CACHE_SUBDIR),
            // Renovate reads RENOVATE_BINARY_SOURCE; RENOVATE_BINARYSOURCE is the older koshin spelling.
            renovateBinarySource = env.nonBlank("RENOVATE_BINARY_SOURCE")
                ?: env.nonBlank("RENOVATE_BINARYSOURCE")
                ?: DEFAULT_RENOVATE_BINARY_SOURCE,
            renovateRedisUrl = env.nonBlank("RENOVATE_REDIS_URL") ?: renovateRedisFallback(env.nonBlank("VALKEY_URL")),
            runnerSocket = env.nonBlank("RUNNER_SOCKET")?.let { Path(it) },
            githubToken = env.nonBlank("GITHUB_TOKEN"),
            githubEndpoint = env.nonBlank("GITHUB_API_URL"),
            gitlabToken = env.nonBlank("GITLAB_TOKEN"),
            gitlabEndpoint = env.nonBlank("GITLAB_ENDPOINT"),
            idleTimeout = env.long("WORKER_IDLE_TIMEOUT_SECONDS")?.seconds ?: Duration.ZERO,
            maxLifetime = env.long("WORKER_MAX_LIFETIME_SECONDS")?.seconds ?: Duration.ZERO,
            jobConfig = jobConfigFromEnv(env),
            throttle = throttleFromEnv(env),
            requestLimits = RenovateRequestLimits(
                maxRequestsPerSecond = env.double("RENOVATE_MAX_REQUESTS_PER_SECOND"),
                concurrentRequestLimit = env.long("RENOVATE_CONCURRENT_REQUEST_LIMIT")?.toInt(),
            ),
            rateLimitLogPattern = env.nonBlank("RATE_LIMIT_LOG_PATTERN") ?: RateLimitDetector.DEFAULT_ABORT_PATTERN,
            repoCache = RepoCacheSettings.fromEnv(env),
        )

        private fun throttleFromEnv(env: Map<String, String>): PlatformThrottleConfig {
            val defaults = PlatformThrottleConfig()
            return PlatformThrottleConfig(
                // The per-platform cap of earlier versions is the per-credential one now (KSH-24)
                maxRunsPerCredential = (
                    env.long("MAX_CONCURRENT_RUNS_PER_CREDENTIAL") ?: env.long("MAX_CONCURRENT_RUNS_PER_PLATFORM")
                    )?.toInt() ?: defaults.maxRunsPerCredential,
                maxRunsGlobal = env.long("MAX_CONCURRENT_RUNS_GLOBAL")?.toInt() ?: defaults.maxRunsGlobal,
                backoffBase = env.long("RATE_LIMIT_BACKOFF_BASE_SECONDS")?.seconds ?: defaults.backoffBase,
                backoffMax = env.long("RATE_LIMIT_BACKOFF_MAX_SECONDS")?.seconds ?: defaults.backoffMax,
            )
        }

        private fun jobConfigFromEnv(env: Map<String, String>): WorkerConfig {
            val defaults = WorkerConfig()
            return WorkerConfig(
                natsAckWait = env.long("NATS_ACK_WAIT_SECONDS")?.seconds ?: defaults.natsAckWait,
                lockTtl = env.long("LOCK_TTL_SECONDS")?.seconds ?: defaults.lockTtl,
                dirtyTtl = env.long("DIRTY_TTL_SECONDS")?.seconds ?: defaults.dirtyTtl,
                queuedTtl = env.long("QUEUED_TTL_SECONDS")?.seconds ?: defaults.queuedTtl,
                cliTimeout = env.long("CLI_TIMEOUT_MINUTES")?.minutes ?: defaults.cliTimeout,
                heartbeatInterval = env.long("HEARTBEAT_INTERVAL_SECONDS")?.seconds ?: defaults.heartbeatInterval,
                lockCollisionRetryDelay = env.long("LOCK_COLLISION_RETRY_SECONDS")?.seconds
                    ?: defaults.lockCollisionRetryDelay,
                maxRateLimitRequeues = env.long("MAX_RATE_LIMIT_REQUEUES")?.toInt() ?: defaults.maxRateLimitRequeues,
            )
        }

        private fun parsePlatforms(raw: String?): Set<Platform> {
            if (raw.isNullOrBlank()) {
                return DEFAULT_PLATFORMS
            }
            val parsed = raw.split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .map { token -> Platform.fromString(token) }
                .toSet()
            require(parsed.isNotEmpty()) { "PLATFORMS must specify at least one platform: '$raw'" }
            return parsed
        }

        private fun String?.redacted(): String = if (this == null) "null" else "***"

        private fun String.withoutUserInfo(): String = replace(Regex("(?<=://)[^@/]+@"), "***@")

        /** Renovate can't use a `redis-sentinel://` URL, so only plain Valkey URLs double as the package cache. */
        private fun renovateRedisFallback(valkeyUrl: String?): String? = when {
            valkeyUrl == null -> ValkeyConfig.DEFAULT_VALKEY_URL
            valkeyUrl.startsWith("redis://") || valkeyUrl.startsWith("rediss://") -> valkeyUrl
            else -> null
        }
    }
}
