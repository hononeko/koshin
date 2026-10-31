package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import io.lettuce.core.RedisURI
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import java.time.Duration as JavaDuration

class ValkeyConfigTest {
    @Test
    fun `default config has expected values`() {
        val config = ValkeyConfig()
        assertEquals("redis://koshin-valkey:6379", config.valkeyUrl)
        assertEquals("koshin", config.keyPrefix)
        assertEquals(0L, config.lockReplicaAckTimeoutMs)
        assertEquals(5.seconds, config.commandTimeout)
        assertEquals(JavaDuration.ofSeconds(5), config.toRedisUri().timeout)
    }

    @Test
    fun `fromEnv reads environment configuration`() {
        val env = mapOf(
            "VALKEY_URL" to "redis://localhost:6380",
            "VALKEY_KEY_PREFIX" to "custom-koshin",
            "LOCK_REPLICA_ACK_TIMEOUT_MS" to "250",
            "VALKEY_COMMAND_TIMEOUT_SECONDS" to "10",
        )
        val config = ValkeyConfig.fromEnv(env)
        assertEquals("redis://localhost:6380", config.valkeyUrl)
        assertEquals("custom-koshin", config.keyPrefix)
        assertEquals(250L, config.lockReplicaAckTimeoutMs)
        assertEquals(10.seconds, config.commandTimeout)
        assertEquals(JavaDuration.ofSeconds(10), config.toRedisUri().timeout)
    }

    @Test
    fun `supports both redis and redis-sentinel URIs and respects explicit URI timeout parameter`() {
        val standalone = ValkeyConfig(
            valkeyUrl = "redis://koshin-valkey:6379?timeout=12s",
            commandTimeout = 5.seconds,
        )
        val standaloneUri = standalone.toRedisUri()
        assertEquals("koshin-valkey", standaloneUri.host)
        assertEquals(6379, standaloneUri.port)
        assertEquals(JavaDuration.ofSeconds(12), standaloneUri.timeout)

        val sentinel = ValkeyConfig(
            valkeyUrl = "redis-sentinel://sentinel-0:26379,sentinel-1:26379,sentinel-2:26379?sentinelMasterId=mymaster",
            commandTimeout = 8.seconds,
        )
        val sentinelUri = sentinel.toRedisUri()
        assertEquals("mymaster", sentinelUri.sentinelMasterId)
        assertEquals(3, sentinelUri.sentinels.size)
        assertEquals(JavaDuration.ofSeconds(8), sentinelUri.timeout)
    }

    @Test
    fun `generates cluster-safe hash-tagged lock and dirty keys`() {
        val config = ValkeyConfig()
        val ghRepo = RepoRef(Platform.GITHUB, "hononeko/koshin")
        assertEquals("koshin:{github:hononeko/koshin}:lock", config.lockKeyFor(ghRepo))
        assertEquals("koshin:{github:hononeko/koshin}:dirty", config.dirtyKeyFor(ghRepo))

        val glRepo = RepoRef(Platform.GITLAB, "group/subgroup/project")
        assertEquals("koshin:{gitlab:group/subgroup/project}:lock", config.lockKeyFor(glRepo))
        assertEquals("koshin:{gitlab:group/subgroup/project}:dirty", config.dirtyKeyFor(glRepo))
    }

    @Test
    fun `fromEnv rejects invalid parameters at startup`() {
        assertThrows<IllegalArgumentException> {
            ValkeyConfig.fromEnv(mapOf("LOCK_REPLICA_ACK_TIMEOUT_MS" to "not-a-number"))
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConfig.fromEnv(mapOf("VALKEY_COMMAND_TIMEOUT_SECONDS" to "not-a-number"))
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConfig.fromEnv(mapOf("VALKEY_URL" to "http://not-a-valkey-url"))
        }
    }

    @Test
    fun `rejects invalid parameters`() {
        assertThrows<IllegalArgumentException> {
            ValkeyConfig(valkeyUrl = " ")
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConfig(keyPrefix = " ")
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConfig(keyPrefix = "bad{prefix")
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConfig(keyPrefix = "bad}prefix")
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConfig(lockReplicaAckTimeoutMs = -1L)
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConfig(commandTimeout = 0.seconds)
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConfig(commandTimeout = Duration.INFINITE)
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConfig(valkeyUrl = "invalid://uri")
        }
    }

    @Test
    fun `sentinel password authenticates the sentinel nodes, not the data nodes`() {
        val config = ValkeyConfig.fromEnv(
            mapOf(
                "VALKEY_URL" to "redis-sentinel://s-0:26379,s-1:26379?sentinelMasterId=mymaster",
                "VALKEY_SENTINEL_PASSWORD" to "sentinel-secret",
            ),
        )

        val uri = config.toRedisUri()
        assertEquals(2, uri.sentinels.size)
        uri.sentinels.forEach { assertEquals("sentinel-secret", String(checkNotNull(it.resolvedPassword()))) }
        assertNull(uri.resolvedPassword())
        assertFalse(config.toString().contains("sentinel-secret"))
        assertNull(ValkeyConfig.fromEnv(mapOf("VALKEY_SENTINEL_PASSWORD" to "")).sentinelPassword)
    }

    @Test
    fun `toString hides credentials`() {
        val config = ValkeyConfig(valkeyUrl = "redis://:data-secret@valkey:6379")
        assertFalse(config.toString().contains("data-secret"))
    }

    private fun RedisURI.resolvedPassword(): CharArray? =
        credentialsProviderAsync?.resolveCredentialsAsync()?.toCompletableFuture()?.get()?.password
}
