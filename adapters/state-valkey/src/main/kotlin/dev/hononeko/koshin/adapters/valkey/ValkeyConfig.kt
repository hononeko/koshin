package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import io.lettuce.core.RedisURI
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

data class ValkeyConfig(
    val valkeyUrl: String = DEFAULT_VALKEY_URL,
    val keyPrefix: String = DEFAULT_KEY_PREFIX,
    val lockReplicaAckTimeoutMs: Long = DEFAULT_LOCK_REPLICA_ACK_TIMEOUT_MS,
    val commandTimeout: Duration = DEFAULT_COMMAND_TIMEOUT,
    /** Sentinel's own password: Lettuce applies a `redis-sentinel://` URL's password to the data nodes only. */
    val sentinelPassword: String? = null,
) {
    init {
        require(valkeyUrl.isNotBlank()) { "valkeyUrl cannot be blank" }
        require(keyPrefix.isNotBlank()) { "keyPrefix cannot be blank" }
        require(!keyPrefix.contains('{') && !keyPrefix.contains('}')) {
            "keyPrefix cannot contain '{' or '}': '$keyPrefix'"
        }
        require(lockReplicaAckTimeoutMs >= 0) { "lockReplicaAckTimeoutMs must be >= 0" }
        require(commandTimeout.isFinite() && commandTimeout.isPositive()) {
            "commandTimeout must be positive and finite: $commandTimeout"
        }
        require(sentinelPassword == null || sentinelPassword.isNotEmpty()) { "sentinelPassword cannot be empty" }
        toRedisUri()
    }

    fun toRedisUri(): RedisURI {
        val uri = RedisURI.create(valkeyUrl)
        if (!hasExplicitTimeoutQueryParam(valkeyUrl)) {
            uri.timeout = commandTimeout.toJavaDuration()
        }
        sentinelPassword?.let { password -> uri.sentinels.forEach { it.setAuthentication(password) } }
        return uri
    }

    override fun toString(): String = "ValkeyConfig(valkeyUrl=${valkeyUrl.replace(USER_INFO, "***@")}, " +
        "keyPrefix=$keyPrefix, lockReplicaAckTimeoutMs=$lockReplicaAckTimeoutMs, commandTimeout=$commandTimeout, " +
        "sentinelPassword=${if (sentinelPassword == null) "null" else "***"})"

    fun lockKeyFor(repo: RepoRef): String = "$keyPrefix:{${repo.platform.name.lowercase()}:${repo.repository}}:lock"

    fun dirtyKeyFor(repo: RepoRef): String = "$keyPrefix:{${repo.platform.name.lowercase()}:${repo.repository}}:dirty"

    fun throttleKeyFor(platform: Platform, name: String): String =
        "$keyPrefix:{throttle:${platform.name.lowercase()}}:$name"

    companion object {
        const val DEFAULT_VALKEY_URL = "redis://koshin-valkey:6379"
        private val USER_INFO = Regex("(?<=://)[^@/]+@")
        const val DEFAULT_KEY_PREFIX = "koshin"
        const val DEFAULT_LOCK_REPLICA_ACK_TIMEOUT_MS = 0L
        const val DEFAULT_COMMAND_TIMEOUT_SECONDS = 5L
        val DEFAULT_COMMAND_TIMEOUT: Duration = DEFAULT_COMMAND_TIMEOUT_SECONDS.seconds

        fun fromEnv(env: Map<String, String> = System.getenv()): ValkeyConfig = ValkeyConfig(
            valkeyUrl = env["VALKEY_URL"] ?: DEFAULT_VALKEY_URL,
            keyPrefix = env["VALKEY_KEY_PREFIX"] ?: DEFAULT_KEY_PREFIX,
            lockReplicaAckTimeoutMs = parseEnvLong(
                env,
                "LOCK_REPLICA_ACK_TIMEOUT_MS",
                DEFAULT_LOCK_REPLICA_ACK_TIMEOUT_MS,
            ),
            commandTimeout = parseEnvLong(
                env,
                "VALKEY_COMMAND_TIMEOUT_SECONDS",
                DEFAULT_COMMAND_TIMEOUT_SECONDS,
            ).seconds,
            sentinelPassword = env["VALKEY_SENTINEL_PASSWORD"]?.takeIf { it.isNotEmpty() },
        )

        private fun parseEnvLong(env: Map<String, String>, key: String, default: Long): Long = env[key]?.let { raw ->
            requireNotNull(raw.toLongOrNull()) { "Invalid long for $key: '$raw'" }
        } ?: default

        private fun hasExplicitTimeoutQueryParam(url: String): Boolean {
            val query = url.substringAfter('?', "")
            if (query.isEmpty()) return false
            return query.split('&', ';').any { param ->
                param.substringBefore('=').equals("timeout", ignoreCase = true)
            }
        }
    }
}
