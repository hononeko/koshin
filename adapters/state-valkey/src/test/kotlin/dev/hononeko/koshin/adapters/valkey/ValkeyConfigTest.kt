package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ValkeyConfigTest {
    @Test
    fun `default config has expected values`() {
        val config = ValkeyConfig()
        assertEquals("redis://koshin-valkey:6379", config.valkeyUrl)
        assertEquals("koshin", config.keyPrefix)
        assertEquals(0L, config.lockReplicaAckTimeoutMs)
    }

    @Test
    fun `fromEnv reads environment configuration`() {
        val env = mapOf(
            "VALKEY_URL" to "redis://localhost:6380",
            "VALKEY_KEY_PREFIX" to "custom-koshin",
            "LOCK_REPLICA_ACK_TIMEOUT_MS" to "250",
        )
        val config = ValkeyConfig.fromEnv(env)
        assertEquals("redis://localhost:6380", config.valkeyUrl)
        assertEquals("custom-koshin", config.keyPrefix)
        assertEquals(250L, config.lockReplicaAckTimeoutMs)
    }

    @Test
    fun `supports both redis and redis-sentinel URIs`() {
        val standalone = ValkeyConfig(valkeyUrl = "redis://koshin-valkey:6379")
        val standaloneUri = standalone.toRedisUri()
        assertEquals("koshin-valkey", standaloneUri.host)
        assertEquals(6379, standaloneUri.port)

        val sentinel = ValkeyConfig(
            valkeyUrl = "redis-sentinel://sentinel-0:26379,sentinel-1:26379,sentinel-2:26379?sentinelMasterId=mymaster",
        )
        val sentinelUri = sentinel.toRedisUri()
        assertEquals("mymaster", sentinelUri.sentinelMasterId)
        assertEquals(3, sentinelUri.sentinels.size)
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
            ValkeyConfig(lockReplicaAckTimeoutMs = -1L)
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConfig(valkeyUrl = "invalid://uri")
        }
    }
}
