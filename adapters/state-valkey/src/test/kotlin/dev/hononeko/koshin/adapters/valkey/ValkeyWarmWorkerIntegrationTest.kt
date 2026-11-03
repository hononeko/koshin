package dev.hononeko.koshin.adapters.valkey

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyWarmWorkerIntegrationTest {
    companion object {
        private lateinit var client: RedisClient
        private lateinit var connection: StatefulRedisConnection<String, String>
        private lateinit var config: ValkeyConfig

        @BeforeAll
        @JvmStatic
        fun setUp() {
            config = ValkeyConfig(valkeyUrl = SharedValkeyContainer.valkeyUrl, keyPrefix = "warm-it")
            client = RedisClient.create(config.toRedisUri())
            connection = client.connect()
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            connection.close()
            client.shutdown()
        }
    }

    private val adapter by lazy { ValkeyWarmWorkerAdapter(connection, config) }

    @BeforeEach
    fun clear() {
        connection.sync().del(config.warmWorkersKey())
    }

    @Test
    fun `counts live leases, drops removed and expired ones`() = runBlocking {
        adapter.preloadScripts()
        adapter.renew("worker-a", 30.seconds)
        adapter.renew("worker-b", 300.milliseconds)
        adapter.renew("worker-a", 30.seconds)
        assertEquals(2, adapter.liveCount())

        delay(400.milliseconds)
        assertEquals(1, adapter.liveCount(), "worker-b's lease expired")

        adapter.remove("worker-a")
        assertEquals(0, adapter.liveCount())
    }

    @Test
    fun `renewing extends the lease and keeps the key alive at least as long`() = runBlocking {
        adapter.renew("worker-a", 300.milliseconds)
        delay(200.milliseconds)
        adapter.renew("worker-a", 300.milliseconds)
        delay(200.milliseconds)
        assertEquals(1, adapter.liveCount())

        val keyTtl = connection.coroutines().pttl(config.warmWorkersKey()) ?: -1L
        assertTrue(keyTtl in 1L..300L, "the ZSET expires with its last lease: $keyTtl")
        assertEquals("warm-it:{workers}:warm", config.warmWorkersKey())
    }

    @Test
    fun `rejects a blank worker ID or a sub-millisecond TTL`() = runBlocking<Unit> {
        assertThrows<IllegalArgumentException> { adapter.renew(" ", 1.seconds) }
        assertThrows<IllegalArgumentException> { adapter.renew("worker-a", 0.milliseconds) }
    }
}
