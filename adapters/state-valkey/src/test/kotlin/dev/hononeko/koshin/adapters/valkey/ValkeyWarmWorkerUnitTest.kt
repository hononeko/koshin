package dev.hononeko.koshin.adapters.valkey

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyWarmWorkerUnitTest {
    private val commands = mockk<RedisCoroutinesCommands<String, String>>()
    private val adapter = ValkeyWarmWorkerAdapter(commands)
    private val key = arrayOf("koshin:{workers}:warm")

    @Test
    fun `renews, counts and removes leases through the scripts`() = runTest {
        coEvery { commands.scriptLoad(ValkeyWarmWorkerAdapter.RENEW_SCRIPT) } returns "sha-renew"
        coEvery { commands.scriptLoad(ValkeyWarmWorkerAdapter.COUNT_SCRIPT) } returns "sha-count"
        coEvery { commands.evalsha<Long>("sha-renew", ScriptOutputType.INTEGER, key, "w-1", "45000") } returns 1L
        coEvery { commands.evalsha<Long>("sha-count", ScriptOutputType.INTEGER, key) } returns 2L
        coEvery { commands.zrem(key.single(), "w-1") } returns 1L

        adapter.preloadScripts()
        adapter.renew("w-1", 45.seconds)
        assertEquals(2, adapter.liveCount())
        adapter.remove("w-1")

        coVerify(exactly = 1) { commands.evalsha<Long>("sha-renew", ScriptOutputType.INTEGER, key, "w-1", "45000") }
        coVerify(exactly = 1) { commands.zrem(key.single(), "w-1") }
    }

    @Test
    fun `rejects a blank worker ID or a non-positive TTL`() = runTest {
        assertThrows<IllegalArgumentException> { adapter.renew(" ", 45.seconds) }
        assertThrows<IllegalArgumentException> { adapter.renew("w-1", Duration.ZERO) }
        assertThrows<IllegalArgumentException> { adapter.renew("w-1", Duration.INFINITE) }
    }

    @Test
    fun `can be built from a connection`() {
        val connection = mockk<StatefulRedisConnection<String, String>>(relaxed = true)
        assertNotNull(ValkeyWarmWorkerAdapter(connection))
    }
}
