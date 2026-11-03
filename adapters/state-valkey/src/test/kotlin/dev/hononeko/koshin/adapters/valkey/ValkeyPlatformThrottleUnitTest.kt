package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
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
    private val github = CredentialScope.Token(Platform.GITHUB)
    private val slotsKey = "koshin:{throttle:github:pat}:slots"
    private val globalKey = "koshin:{throttle:global}:slots"
    private val cooldownKey = "koshin:{throttle:github:pat}:cooldown"
    private val backoffKey = "koshin:{throttle:github:pat}:backoff_n"

    /** nextDouble() == 0, the lowest jitter: a factor of exactly 0.8. */
    private val lowestJitter = object : Random() {
        override fun nextBits(bitCount: Int): Int = 0
    }

    private val adapter = ValkeyPlatformThrottleAdapter(
        commands,
        ValkeyConfig(),
        PlatformThrottleConfig(
            maxRunsPerCredential = 3,
            maxRunsGlobal = 6,
            backoffBase = 60.seconds,
            backoffMax = 3600.seconds,
        ),
        lowestJitter,
    )

    init {
        coEvery { commands.scriptLoad(any<String>()) } answers { "sha-${firstArg<String>().hashCode()}" }
    }

    private fun stubScript(vararg args: String, result: Long) {
        coEvery { commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, any<Array<String>>(), *args) } returns result
    }

    private fun stubSlots(key: String, vararg args: String, result: Long) {
        coEvery { commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, arrayOf(key), *args) } returns result
    }

    @Test
    fun `a run slot leases the scope's slot and then a global one, with no client timestamp`() = runTest {
        stubSlots(slotsKey, "tok", "45000", "3", result = 1L)
        stubSlots(globalKey, "tok", "45000", "6", result = 1L)
        assertTrue(adapter.tryAcquireRunSlot(github, "tok", 45.seconds))

        coVerifyOrder {
            commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, arrayOf(slotsKey), "tok", "45000", "3")
            commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, arrayOf(globalKey), "tok", "45000", "6")
        }
        coVerify(exactly = 0) { commands.zrem(any(), *anyVararg()) }
    }

    @Test
    fun `a full scope never takes a global slot`() = runTest {
        stubSlots(slotsKey, "tok", "45000", "3", result = 0L)

        assertFalse(adapter.tryAcquireRunSlot(github, "tok", 45.seconds))

        coVerify(exactly = 0) {
            commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, arrayOf(globalKey), *anyVararg())
        }
    }

    @Test
    fun `the scope's slot is given back when no global one is free, or leasing it fails`() = runTest {
        stubSlots(slotsKey, "tok", "45000", "3", result = 1L)
        stubSlots(globalKey, "tok", "45000", "6", result = 0L)
        assertFalse(adapter.tryAcquireRunSlot(github, "tok", 45.seconds))

        coEvery {
            commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, arrayOf(globalKey), *anyVararg())
        } throws IllegalStateException("Valkey down")
        assertThrows<IllegalStateException> { adapter.tryAcquireRunSlot(github, "tok", 45.seconds) }

        coVerify(exactly = 2) { commands.zrem(slotsKey, "tok") }
    }

    @Test
    fun `extending needs both leases`() = runTest {
        stubSlots(slotsKey, "tok", "45000", result = 1L)
        stubSlots(globalKey, "tok", "45000", result = 1L)
        assertTrue(adapter.extendRunSlot(github, "tok", 45.seconds))

        stubSlots(globalKey, "tok", "45000", result = 0L)
        assertFalse(adapter.extendRunSlot(github, "tok", 45.seconds))

        stubSlots(slotsKey, "tok", "45000", result = 0L)
        assertFalse(adapter.extendRunSlot(github, "tok", 45.seconds))
        coVerify(exactly = 2) {
            commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, arrayOf(globalKey), *anyVararg())
        }
    }

    @Test
    fun `release removes the token from both and reset deletes the backoff counter`() = runTest {
        adapter.releaseRunSlot(github, "tok")
        adapter.resetBackoff(github)
        coVerify { commands.zrem(slotsKey, "tok") }
        coVerify { commands.zrem(globalKey, "tok") }
        coVerify { commands.del(backoffKey) }
    }

    @Test
    fun `the global lease is released even when releasing the scope's fails`() = runTest {
        coEvery { commands.zrem(slotsKey, "tok") } throws IllegalStateException("Valkey down")

        assertThrows<IllegalStateException> { adapter.releaseRunSlot(github, "tok") }
        coVerify { commands.zrem(globalKey, "tok") }
    }

    @Test
    fun `cooldownRemaining maps PTTL to a duration`() = runTest {
        coEvery { commands.pttl(cooldownKey) } returns 1500L
        assertEquals(1500.milliseconds, adapter.cooldownRemaining(github))
        coEvery { commands.pttl(cooldownKey) } returns -2L
        assertNull(adapter.cooldownRemaining(github))
        coEvery { commands.pttl(cooldownKey) } returns null
        assertNull(adapter.cooldownRemaining(github))
    }

    @Test
    fun `registerRateLimit passes retry-after, backoff bounds and jitter to the script`() = runTest {
        stubScript("30000", "60000", "3600000", "0.8", result = 30_000L)
        assertEquals(30.seconds, adapter.registerRateLimit(github, 30.seconds))

        stubScript("-1", "60000", "3600000", "0.8", result = 48_000L)
        assertEquals(48.seconds, adapter.registerRateLimit(github, null))
        assertEquals(48.seconds, adapter.registerRateLimit(github, Duration.ZERO), "zero means unknown")

        coVerify {
            commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, arrayOf(cooldownKey, backoffKey), *anyVararg())
        }
    }

    @Test
    fun `preloadScripts loads the throttle scripts`() = runTest {
        adapter.preloadScripts()
        coVerify { commands.scriptLoad(ValkeyPlatformThrottleAdapter.ACQUIRE_SLOT_SCRIPT) }
        coVerify { commands.scriptLoad(ValkeyPlatformThrottleAdapter.EXTEND_SLOT_SCRIPT) }
        coVerify { commands.scriptLoad(ValkeyPlatformThrottleAdapter.REGISTER_RATE_LIMIT_SCRIPT) }
        coVerify { commands.scriptLoad(ValkeyPlatformThrottleAdapter.COUNT_SLOTS_SCRIPT) }
    }

    @Test
    fun `activeRunSlots counts the leases of the scope's slots`() = runTest {
        coEvery { commands.evalsha<Long>(any(), ScriptOutputType.INTEGER, arrayOf(slotsKey)) } returns 2L
        assertEquals(2, adapter.activeRunSlots(github))
    }

    @Test
    fun `scripts read Valkey's clock instead of taking a timestamp`() {
        listOf(ValkeyPlatformThrottleAdapter.ACQUIRE_SLOT_SCRIPT, ValkeyPlatformThrottleAdapter.EXTEND_SLOT_SCRIPT)
            .forEach { assertTrue(it.contains("redis.call('TIME')"), it) }
    }

    @Test
    fun `invalid arguments are rejected`() = runTest {
        assertThrows<IllegalArgumentException> { adapter.tryAcquireRunSlot(github, " ", 45.seconds) }
        assertThrows<IllegalArgumentException> { adapter.extendRunSlot(github, "", 45.seconds) }
        assertThrows<IllegalArgumentException> { adapter.tryAcquireRunSlot(github, "tok", Duration.ZERO) }
        assertThrows<IllegalArgumentException> { adapter.extendRunSlot(github, "tok", Duration.INFINITE) }
    }

    @Test
    fun `throttle config is validated`() {
        assertThrows<IllegalArgumentException> { PlatformThrottleConfig(maxRunsPerCredential = 0) }
        assertThrows<IllegalArgumentException> { PlatformThrottleConfig(maxRunsGlobal = 0) }
        assertThrows<IllegalArgumentException> { PlatformThrottleConfig(backoffBase = Duration.ZERO) }
        assertThrows<IllegalArgumentException> {
            PlatformThrottleConfig(backoffBase = 10.seconds, backoffMax = 5.seconds)
        }
        assertEquals(2, PlatformThrottleConfig().maxRunsPerCredential)
        assertEquals(6, PlatformThrottleConfig().maxRunsGlobal)
    }

    @Test
    fun `throttle keys share a hash tag per credential scope`() {
        val config = ValkeyConfig(keyPrefix = "kp")
        val gitlab = CredentialScope.Token(Platform.GITLAB)
        assertEquals("kp:{throttle:gitlab:pat}:slots", config.throttleKeyFor(gitlab, "slots"))
        val installation = CredentialScope.Installation(42)
        assertEquals("kp:{throttle:github:inst-42}:cooldown", config.throttleKeyFor(installation, "cooldown"))
        assertEquals("kp:{throttle:global}:slots", config.globalSlotsKey())
    }

    @Test
    fun `connection constructor uses the connection's coroutine commands`() {
        val connection = mockk<StatefulRedisConnection<String, String>>(relaxed = true)
        ValkeyPlatformThrottleAdapter(connection)
    }
}
