package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.Platform
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

data class ProxyConfig(
    val port: Int = DEFAULT_PORT,
    val http2Enabled: Boolean = DEFAULT_HTTP2_ENABLED,
    val platforms: Set<Platform> = DEFAULT_PLATFORMS,
    val natsUrl: String = DEFAULT_NATS_URL,
    val githubWebhookSecret: String? = null,
    val gitlabWebhookSecret: String? = null,
    val internalTriggerToken: String? = null,
    val sweepRepositoriesFile: String? = null,
    val botLogins: Set<String> = DEFAULT_BOT_LOGINS,
    val publishRetryBaseDelay: Duration = DEFAULT_PUBLISH_RETRY_BASE_DELAY,
    val publishRetryBudget: Duration = DEFAULT_PUBLISH_RETRY_BUDGET,
    val maxRequestBodyBytes: Int = DEFAULT_MAX_REQUEST_BODY_BYTES,
    val requestReadTimeoutSeconds: Int = DEFAULT_REQUEST_READ_TIMEOUT_SECONDS,
) {
    init {
        require(port in 0..MAX_PORT) { "port must be in 0..$MAX_PORT: $port" }
        require(platforms.isNotEmpty()) { "platforms cannot be empty" }
        require(natsUrl.isNotBlank()) { "natsUrl cannot be blank" }
        require(!publishRetryBaseDelay.isNegative() && publishRetryBaseDelay.isFinite()) {
            "publishRetryBaseDelay must be non-negative and finite: $publishRetryBaseDelay"
        }
        require(!publishRetryBudget.isNegative() && publishRetryBudget.isFinite()) {
            "publishRetryBudget must be non-negative and finite: $publishRetryBudget"
        }
        require(!publishRetryBudget.isPositive() || publishRetryBaseDelay.isPositive()) {
            "publishRetryBaseDelay must be positive when publishRetryBudget > 0"
        }
        require(maxRequestBodyBytes > 0) { "maxRequestBodyBytes must be > 0: $maxRequestBodyBytes" }
        require(requestReadTimeoutSeconds > 0) {
            "requestReadTimeoutSeconds must be > 0: $requestReadTimeoutSeconds"
        }
    }

    fun isPlatformEnabled(platform: Platform): Boolean = platform in platforms

    companion object {
        const val DEFAULT_PORT = 8080
        const val MAX_PORT = 65535
        const val DEFAULT_HTTP2_ENABLED = true
        const val DEFAULT_NATS_URL = "nats://koshin-nats:4222"
        const val DEFAULT_MAX_REQUEST_BODY_BYTES = 10 * 1024 * 1024
        const val DEFAULT_REQUEST_READ_TIMEOUT_SECONDS = 15
        val DEFAULT_PLATFORMS: Set<Platform> = setOf(Platform.GITHUB)
        val DEFAULT_BOT_LOGINS: Set<String> = setOf("renovate[bot]", "renovate", "renovate-bot")
        val DEFAULT_PUBLISH_RETRY_BASE_DELAY: Duration = 100.milliseconds
        val DEFAULT_PUBLISH_RETRY_BUDGET: Duration = 5.seconds

        fun fromEnv(env: Map<String, String> = System.getenv()): ProxyConfig {
            val port = env["PORT"]?.let { raw ->
                requireNotNull(raw.toIntOrNull()) { "Invalid integer for PORT: '$raw'" }
            } ?: DEFAULT_PORT

            val http2Enabled = env["HTTP2_ENABLED"]?.let { raw ->
                requireNotNull(raw.lowercase().toBooleanStrictOrNull()) {
                    "Invalid boolean for HTTP2_ENABLED: '$raw'"
                }
            } ?: DEFAULT_HTTP2_ENABLED

            val platforms = parsePlatforms(env["PLATFORMS"])
            val natsUrl = env["NATS_URL"]?.takeIf { it.isNotBlank() } ?: DEFAULT_NATS_URL

            val botLogins = parseBotLogins(env)

            return ProxyConfig(
                port = port,
                http2Enabled = http2Enabled,
                platforms = platforms,
                natsUrl = natsUrl,
                githubWebhookSecret = env["GITHUB_WEBHOOK_SECRET"]?.takeIf { it.isNotBlank() },
                gitlabWebhookSecret = env["GITLAB_WEBHOOK_SECRET"]?.takeIf { it.isNotBlank() },
                internalTriggerToken = env["INTERNAL_TRIGGER_TOKEN"]?.takeIf { it.isNotBlank() },
                sweepRepositoriesFile = env["SWEEP_REPOSITORIES_FILE"]?.takeIf { it.isNotBlank() },
                botLogins = botLogins,
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

        private fun parseBotLogins(env: Map<String, String>): Set<String> {
            val configured = buildSet {
                addAll(DEFAULT_BOT_LOGINS)
                env["RENOVATE_BOT_LOGINS"]?.split(',')
                    ?.map { it.trim().lowercase() }
                    ?.filter { it.isNotEmpty() }
                    ?.let { addAll(it) }
                env["GITHUB_BOT_LOGIN"]?.trim()?.lowercase()?.takeIf { it.isNotEmpty() }?.let { add(it) }
                env["GITLAB_BOT_LOGIN"]?.trim()?.lowercase()?.takeIf { it.isNotEmpty() }?.let { add(it) }
            }
            return configured
        }
    }
}
