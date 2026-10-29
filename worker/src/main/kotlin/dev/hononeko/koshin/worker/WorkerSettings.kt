package dev.hononeko.koshin.worker

import dev.hononeko.koshin.adapters.valkey.PlatformThrottleConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyConfig
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.usecase.WorkerConfig
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

data class WorkerSettings(
    val port: Int = DEFAULT_PORT,
    val http2Enabled: Boolean = DEFAULT_HTTP2_ENABLED,
    val platforms: Set<Platform> = DEFAULT_PLATFORMS,
    val natsUrl: String = DEFAULT_NATS_URL,
    val renovateBaseDir: Path = Path(DEFAULT_RENOVATE_BASE_DIR),
    val renovateBinarySource: String = DEFAULT_RENOVATE_BINARY_SOURCE,
    val renovateRedisUrl: String? = null,
    val githubToken: String? = null,
    val githubEndpoint: String? = null,
    val gitlabToken: String? = null,
    val gitlabEndpoint: String? = null,
    val podDeletionCostEnabled: Boolean = false,
    val podName: String? = null,
    val podNamespace: String? = null,
    val jobConfig: WorkerConfig = WorkerConfig(),
    val throttle: PlatformThrottleConfig = PlatformThrottleConfig(),
    val requestLimits: RenovateRequestLimits = RenovateRequestLimits(),
    val rateLimitLogPattern: String = RateLimitDetector.DEFAULT_ABORT_PATTERN,
) {
    init {
        require(port in 0..MAX_PORT) { "port must be in 0..$MAX_PORT: $port" }
        require(platforms.isNotEmpty()) { "platforms cannot be empty" }
        require(natsUrl.isNotBlank()) { "natsUrl cannot be blank" }
        require(renovateBaseDir.isAbsolute) { "RENOVATE_BASE_DIR must be absolute: $renovateBaseDir" }
        require(renovateBinarySource.isNotBlank()) { "renovateBinarySource cannot be blank" }
        require(!podDeletionCostEnabled || (!podName.isNullOrBlank() && !podNamespace.isNullOrBlank())) {
            "POD_DELETION_COST_ENABLED requires POD_NAME and POD_NAMESPACE"
        }
        Regex(rateLimitLogPattern)
    }

    override fun toString(): String = "WorkerSettings(port=$port, http2Enabled=$http2Enabled, platforms=$platforms, " +
        "natsUrl=${natsUrl.withoutUserInfo()}, renovateBaseDir=$renovateBaseDir, " +
        "githubToken=${githubToken.redacted()}, gitlabToken=${gitlabToken.redacted()}, jobConfig=$jobConfig, " +
        "throttle=$throttle, requestLimits=$requestLimits)"

    companion object {
        const val DEFAULT_PORT = 8080
        const val MAX_PORT = 65535
        const val DEFAULT_HTTP2_ENABLED = true
        const val DEFAULT_NATS_URL = "nats://koshin-nats:4222"
        const val DEFAULT_RENOVATE_BASE_DIR = "/tmp/renovate"
        const val DEFAULT_RENOVATE_BINARY_SOURCE = "global"
        val DEFAULT_PLATFORMS: Set<Platform> = setOf(Platform.GITHUB)

        fun fromEnv(env: Map<String, String> = System.getenv()): WorkerSettings = WorkerSettings(
            port = env.long("PORT")?.toInt() ?: DEFAULT_PORT,
            http2Enabled = env.boolean("HTTP2_ENABLED") ?: DEFAULT_HTTP2_ENABLED,
            platforms = parsePlatforms(env["PLATFORMS"]),
            natsUrl = env.nonBlank("NATS_URL") ?: DEFAULT_NATS_URL,
            renovateBaseDir = Path(env.nonBlank("RENOVATE_BASE_DIR") ?: DEFAULT_RENOVATE_BASE_DIR),
            // Renovate reads RENOVATE_BINARY_SOURCE; RENOVATE_BINARYSOURCE is the older koshin spelling.
            renovateBinarySource = env.nonBlank("RENOVATE_BINARY_SOURCE")
                ?: env.nonBlank("RENOVATE_BINARYSOURCE")
                ?: DEFAULT_RENOVATE_BINARY_SOURCE,
            renovateRedisUrl = env.nonBlank("RENOVATE_REDIS_URL") ?: renovateRedisFallback(env.nonBlank("VALKEY_URL")),
            githubToken = env.nonBlank("GITHUB_TOKEN"),
            githubEndpoint = env.nonBlank("GITHUB_API_URL"),
            gitlabToken = env.nonBlank("GITLAB_TOKEN"),
            gitlabEndpoint = env.nonBlank("GITLAB_ENDPOINT"),
            podDeletionCostEnabled = env.boolean("POD_DELETION_COST_ENABLED") ?: false,
            podName = env.nonBlank("POD_NAME"),
            podNamespace = env.nonBlank("POD_NAMESPACE"),
            jobConfig = jobConfigFromEnv(env),
            throttle = throttleFromEnv(env),
            requestLimits = RenovateRequestLimits(
                maxRequestsPerSecond = env.double("RENOVATE_MAX_REQUESTS_PER_SECOND"),
                concurrentRequestLimit = env.long("RENOVATE_CONCURRENT_REQUEST_LIMIT")?.toInt(),
            ),
            rateLimitLogPattern = env.nonBlank("RATE_LIMIT_LOG_PATTERN") ?: RateLimitDetector.DEFAULT_ABORT_PATTERN,
        )

        private fun throttleFromEnv(env: Map<String, String>): PlatformThrottleConfig {
            val defaults = PlatformThrottleConfig()
            return PlatformThrottleConfig(
                maxConcurrentRuns = env.long("MAX_CONCURRENT_RUNS_PER_PLATFORM")?.toInt() ?: defaults.maxConcurrentRuns,
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
                cliTimeout = env.long("CLI_TIMEOUT_MINUTES")?.minutes ?: defaults.cliTimeout,
                heartbeatInterval = env.long("HEARTBEAT_INTERVAL_SECONDS")?.seconds ?: defaults.heartbeatInterval,
                lockCollisionRetryDelay = env.long("LOCK_COLLISION_RETRY_SECONDS")?.seconds
                    ?: defaults.lockCollisionRetryDelay,
                maxRateLimitRequeues = env.long("MAX_RATE_LIMIT_REQUEUES")?.toInt() ?: defaults.maxRateLimitRequeues,
            )
        }

        private fun Map<String, String>.nonBlank(key: String): String? = this[key]?.trim()?.takeIf { it.isNotEmpty() }

        private fun Map<String, String>.boolean(key: String): Boolean? = nonBlank(key)?.let { raw ->
            requireNotNull(raw.lowercase().toBooleanStrictOrNull()) { "Invalid boolean for $key: '$raw'" }
        }

        private fun Map<String, String>.long(key: String): Long? = nonBlank(key)?.let { raw ->
            requireNotNull(raw.toLongOrNull()) { "Invalid integer for $key: '$raw'" }
        }

        private fun Map<String, String>.double(key: String): Double? = nonBlank(key)?.let { raw ->
            requireNotNull(raw.toDoubleOrNull()) { "Invalid number for $key: '$raw'" }
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
