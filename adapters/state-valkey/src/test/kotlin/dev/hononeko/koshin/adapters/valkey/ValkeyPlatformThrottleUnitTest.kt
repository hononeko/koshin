package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.Platform
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyPlatformThrottleUnitTest {
    private val commands = mockk<RedisCoroutinesCommands<String, String>>(relaxed = true)
    private val slotsKey = "koshin:{throttle:github}:slots"
    private val cooldownKey = "koshin:{throttle:github}:cooldown"
    private val backoffKey = "koshin:{throttle:github}:backoff_n"

    /** nextDouble() == 0, the lowest jitter: a factor of exactly 0.8. */
    private val lowestJitter = object : Random() {
        override fun nextBits(bitCount: Int): Int = 0
    }

    private val adapter = ValkeyPlatformThrottleAdapter(
        commands,
        ValkeyConfig(),
        PlatformThrottleConfig(maxConcurrentRuns = 3, backoffBase = 60.seconds, backoffMax = 3600.seconds),
        lowestJitter,
    )

    init {
        coEvery { commands.scriptLoad(any<String>()) } answers { "sha-${firstArg<String>().hashCode()}" }
    }

    private fun stubScript(vararg args: String, result: Long) {
        coEvery { commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, any<Array<String>>(), *args) } returns result
    }

    @Test
    fun `slots are leased with the ttl and cap, and no client timestamp`() = runTest {
        stubScript("tok", "45000", "3", result = 1L)
        assertTrue(adapter.tryAcquireRunSlot(Platform.GITHUB, "tok", 45.seconds))
        stubScript("tok", "45000", "3", result = 0L)
        assertFalse(adapter.tryAcquireRunSlot(Platform.GITHUB, "tok", 45.seconds))

        stubScript("tok", "45000", result = 1L)
        assertTrue(adapter.extendRunSlot(Platform.GITHUB, "tok", 45.seconds))
        stubScript("tok", "45000", result = 0L)
        assertFalse(adapter.extendRunSlot(Platform.GITHUB, "tok", 45.seconds))

        coVerify { commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, arrayOf(slotsKey), "tok", "45000", "3") }
    }

    @Test
    fun `release removes the token and reset deletes the backoff counter`() = runTest {
        adapter.releaseRunSlot(Platform.GITHUB, "tok")
        adapter.resetBackoff(Platform.GITHUB)
        coVerify { commands.zrem(slotsKey, "tok") }
        coVerify { commands.del(backoffKey) }
    }

    @Test
    fun `cooldownRemaining maps PTTL to a duration`() = runTest {
        coEvery { commands.pttl(cooldownKey) } returns 1500L
        assertEquals(1500.milliseconds, adapter.cooldownRemaining(Platform.GITHUB))
        coEvery { commands.pttl(cooldownKey) } returns -2L
        assertNull(adapter.cooldownRemaining(Platform.GITHUB))
        coEvery { commands.pttl(cooldownKey) } returns null
        assertNull(adapter.cooldownRemaining(Platform.GITHUB))
    }

    @Test
    fun `registerRateLimit passes retry-after, backoff bounds and jitter to the script`() = runTest {
        stubScript("30000", "60000", "3600000", "0.8", result = 30_000L)
        assertEquals(30.seconds, adapter.registerRateLimit(Platform.GITHUB, 30.seconds))

        stubScript("-1", "60000", "3600000", "0.8", result = 48_000L)
        assertEquals(48.seconds, adapter.registerRateLimit(Platform.GITHUB, null))
        assertEquals(48.seconds, adapter.registerRateLimit(Platform.GITHUB, Duration.ZERO), "zero means unknown")

        coVerify {
            commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, arrayOf(cooldownKey, backoffKey), *anyVararg())
        }
    }

    @Test
    fun `preloadScripts loads the three throttle scripts`() = runTest {
        adapter.preloadScripts()
        coVerify { commands.scriptLoad(ValkeyPlatformThrottleAdapter.ACQUIRE_SLOT_SCRIPT) }
        coVerify { commands.scriptLoad(ValkeyPlatformThrottleAdapter.EXTEND_SLOT_SCRIPT) }
        coVerify { commands.scriptLoad(ValkeyPlatformThrottleAdapter.REGISTER_RATE_LIMIT_SCRIPT) }
    }

    @Test
    fun `scripts read Valkey's clock instead of taking a timestamp`() {
        listOf(ValkeyPlatformThrottleAdapter.ACQUIRE_SLOT_SCRIPT, ValkeyPlatformThrottleAdapter.EXTEND_SLOT_SCRIPT)
            .forEach { assertTrue(it.contains("redis.call('TIME')"), it) }
    }

    @Test
    fun `invalid arguments are rejected`() = runTest {
        assertThrows<IllegalArgumentException> { adapter.tryAcquireRunSlot(Platform.GITHUB, " ", 45.seconds) }
        assertThrows<IllegalArgumentException> { adapter.extendRunSlot(Platform.GITHUB, "", 45.seconds) }
        assertThrows<IllegalArgumentException> { adapter.tryAcquireRunSlot(Platform.GITHUB, "tok", Duration.ZERO) }
        assertThrows<IllegalArgumentException> { adapter.extendRunSlot(Platform.GITHUB, "tok", Duration.INFINITE) }
    }

    @Test
    fun `throttle config is validated`() {
        assertThrows<IllegalArgumentException> { PlatformThrottleConfig(maxConcurrentRuns = 0) }
        assertThrows<IllegalArgumentException> { PlatformThrottleConfig(backoffBase = Duration.ZERO) }
        assertThrows<IllegalArgumentException> {
            PlatformThrottleConfig(backoffBase = 10.seconds, backoffMax = 5.seconds)
        }
        assertEquals(3, PlatformThrottleConfig().maxConcurrentRuns)
    }

    @Test
    fun `throttle keys share a hash tag per platform`() {
        val config = ValkeyConfig(keyPrefix = "kp")
        assertEquals("kp:{throttle:gitlab}:slots", config.throttleKeyFor(Platform.GITLAB, "slots"))
    }

    @Test
    fun `connection constructor uses the connection's coroutine commands`() {
        val connection = mockk<StatefulRedisConnection<String, String>>(relaxed = true)
        ValkeyPlatformThrottleAdapter(connection)
    }
}
