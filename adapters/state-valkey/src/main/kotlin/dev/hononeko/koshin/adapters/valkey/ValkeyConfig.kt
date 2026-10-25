package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.RepoRef
import io.lettuce.core.RedisURI

data class ValkeyConfig(
    val valkeyUrl: String = DEFAULT_VALKEY_URL,
    val keyPrefix: String = DEFAULT_KEY_PREFIX,
    val lockReplicaAckTimeoutMs: Long = DEFAULT_LOCK_REPLICA_ACK_TIMEOUT_MS,
) {
    init {
        require(valkeyUrl.isNotBlank()) { "valkeyUrl cannot be blank" }
        require(keyPrefix.isNotBlank()) { "keyPrefix cannot be blank" }
        require(lockReplicaAckTimeoutMs >= 0) { "lockReplicaAckTimeoutMs must be >= 0" }
        RedisURI.create(valkeyUrl)
    }

    fun toRedisUri(): RedisURI = RedisURI.create(valkeyUrl)

    fun lockKeyFor(repo: RepoRef): String = "$keyPrefix:{${repo.platform.name.lowercase()}:${repo.repository}}:lock"

    fun dirtyKeyFor(repo: RepoRef): String = "$keyPrefix:{${repo.platform.name.lowercase()}:${repo.repository}}:dirty"

    companion object {
        const val DEFAULT_VALKEY_URL = "redis://koshin-valkey:6379"
        const val DEFAULT_KEY_PREFIX = "koshin"
        const val DEFAULT_LOCK_REPLICA_ACK_TIMEOUT_MS = 0L

        fun fromEnv(env: Map<String, String> = System.getenv()): ValkeyConfig = ValkeyConfig(
            valkeyUrl = env["VALKEY_URL"] ?: DEFAULT_VALKEY_URL,
            keyPrefix = env["VALKEY_KEY_PREFIX"] ?: DEFAULT_KEY_PREFIX,
            lockReplicaAckTimeoutMs = parseEnvLong(
                env,
                "LOCK_REPLICA_ACK_TIMEOUT_MS",
                DEFAULT_LOCK_REPLICA_ACK_TIMEOUT_MS,
            ),
        )

        private fun parseEnvLong(env: Map<String, String>, key: String, default: Long): Long = env[key]?.let { raw ->
            requireNotNull(raw.toLongOrNull()) { "Invalid long for $key: '$raw'" }
        } ?: default
    }
}
