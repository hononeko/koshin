package dev.hononeko.koshin.adapters.valkey

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
@Suppress("InjectDispatcher")
class ValkeyLeaderLeaseIntegrationTest {
    companion object {
        private lateinit var client: RedisClient
        private lateinit var connection: StatefulRedisConnection<String, String>
        private val config = ValkeyConfig(valkeyUrl = SharedValkeyContainer.valkeyUrl, keyPrefix = "leader-it")

        @BeforeAll
        @JvmStatic
        fun setUp() {
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

    private val lease by lazy { ValkeyLeaderLeaseAdapter(connection, config) }

    @BeforeEach
    fun clear() {
        connection.sync().del(config.dispatcherLeaderKey())
    }

    @Test
    fun `one of many concurrent candidates gets the lease`() = runBlocking(Dispatchers.IO) {
        val winners = (1..10).map { i -> async { lease.acquireOrRenew("proxy-$i", 15.seconds) } }.awaitAll()
        assertEquals(1, winners.count { it })
    }

    @Test
    fun `the holder renews, and others take over only once it expires or is released`() = runBlocking {
        lease.preloadScripts()
        assertTrue(lease.acquireOrRenew("proxy-a", 400.milliseconds))
        assertFalse(lease.acquireOrRenew("proxy-b", 400.milliseconds))

        delay(250.milliseconds)
        assertTrue(lease.acquireOrRenew("proxy-a", 400.milliseconds), "renewed")
        delay(250.milliseconds)
        assertFalse(lease.acquireOrRenew("proxy-b", 400.milliseconds), "the renewal kept it")

        delay(500.milliseconds)
        assertTrue(lease.acquireOrRenew("proxy-b", 400.milliseconds), "expired: handed over")
        assertFalse(lease.acquireOrRenew("proxy-a", 400.milliseconds))

        lease.release("proxy-a")
        assertNotNull(connection.sync().get(config.dispatcherLeaderKey()), "only the holder can release it")
        lease.release("proxy-b")
        assertNull(connection.sync().get(config.dispatcherLeaderKey()))
        assertTrue(lease.acquireOrRenew("proxy-a", 400.milliseconds))
    }

    @Test
    fun `the lease key shares no hash tag with repo or throttle keys`() {
        assertEquals("leader-it:{dispatcher}:leader", config.dispatcherLeaderKey())
    }
}
