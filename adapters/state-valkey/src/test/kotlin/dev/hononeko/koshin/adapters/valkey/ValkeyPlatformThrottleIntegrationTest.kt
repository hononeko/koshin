package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.abs
import kotlin.random.Random
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

@OptIn(ExperimentalLettuceCoroutinesApi::class)
@Suppress("InjectDispatcher")
class ValkeyPlatformThrottleIntegrationTest {
    companion object {
        private lateinit var client: RedisClient
        private lateinit var connection: StatefulRedisConnection<String, String>
        private val config = ValkeyConfig(valkeyUrl = SharedValkeyContainer.valkeyUrl, keyPrefix = "throttle-it")

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

    private val github = CredentialScope.Token(Platform.GITHUB)
    private val gitlab = CredentialScope.Token(Platform.GITLAB)
    private val inst111 = CredentialScope.Installation(111)
    private val inst222 = CredentialScope.Installation(222)
    private val slotsKey = config.throttleKeyFor(github, "slots")
    private val cooldownKey = config.throttleKeyFor(github, "cooldown")

    @BeforeEach
    fun flushDb() {
        connection.sync().flushall()
    }

    private fun adapter(
        maxConcurrentRuns: Int = 3,
        random: Random = Random.Default,
        commands: RedisCoroutinesCommands<String, String> = connection.coroutines(),
        maxRunsGlobal: Int = 100,
    ) = ValkeyPlatformThrottleAdapter(
        commands = commands,
        config = config,
        throttle = PlatformThrottleConfig(
            maxRunsPerCredential = maxConcurrentRuns,
            maxRunsGlobal = maxRunsGlobal,
            backoffBase = 1.seconds,
            backoffMax = 4.seconds,
        ),
        random = random,
    )

    /** Always rolls the lowest jitter, so backoff steps are exact: `0.8 × min(base × 2^n, max)`. */
    private val lowestJitter = object : Random() {
        override fun nextBits(bitCount: Int): Int = 0
    }

    @Test
    fun `20 concurrent holders never exceed a cap of 3`() = runBlocking(Dispatchers.Default) {
        val throttle = adapter(maxConcurrentRuns = 3)
        val current = AtomicInteger()
        val peak = AtomicInteger()

        (1..20).map { i ->
            async {
                val token = "holder-$i"
                while (!throttle.tryAcquireRunSlot(github, token, 10.seconds)) {
                    delay(5.milliseconds)
                }
                peak.accumulateAndGet(current.incrementAndGet(), ::maxOf)
                delay(30.milliseconds)
                current.decrementAndGet()
                throttle.releaseRunSlot(github, token)
            }
        }.awaitAll()

        assertEquals(3, peak.get())
        assertEquals(0L, connection.sync().zcard(slotsKey))
    }

    @Test
    fun `a crashed holder's lease expires and frees its slot`() = runBlocking {
        val throttle = adapter(maxConcurrentRuns = 1)
        assertTrue(throttle.tryAcquireRunSlot(github, "crashed", 1.seconds))
        val start = TimeSource.Monotonic.markNow()
        assertFalse(throttle.tryAcquireRunSlot(github, "waiting", 1.seconds))

        while (!throttle.tryAcquireRunSlot(github, "waiting", 1.seconds)) {
            check(start.elapsedNow() < 5.seconds) { "lease never expired" }
            delay(50.milliseconds)
        }
        assertTrue(start.elapsedNow() >= 900.milliseconds)
        assertFalse(throttle.extendRunSlot(github, "crashed", 1.seconds), "an expired lease can't be extended")
    }

    @Test
    fun `extending keeps the lease alive past its original ttl`() = runBlocking {
        val throttle = adapter(maxConcurrentRuns = 1)
        assertTrue(throttle.tryAcquireRunSlot(github, "holder", 1.seconds))
        repeat(3) {
            delay(500.milliseconds)
            assertTrue(throttle.extendRunSlot(github, "holder", 1.seconds))
        }
        assertFalse(throttle.tryAcquireRunSlot(github, "other", 1.seconds))
        assertTrue(throttle.tryAcquireRunSlot(github, "holder", 1.seconds), "re-acquiring an own lease")
    }

    @Test
    fun `activeRunSlots counts unexpired leases only`() = runBlocking {
        val throttle = adapter(maxConcurrentRuns = 3)
        assertEquals(0, throttle.activeRunSlots(github))
        throttle.tryAcquireRunSlot(github, "long", 10.seconds)
        throttle.tryAcquireRunSlot(github, "short", 200.milliseconds)
        throttle.tryAcquireRunSlot(gitlab, "other", 10.seconds)
        assertEquals(2, throttle.activeRunSlots(github))

        delay(300.milliseconds)
        assertEquals(1, throttle.activeRunSlots(github))
        throttle.releaseRunSlot(github, "long")
        assertEquals(0, throttle.activeRunSlots(github))
    }

    @Test
    fun `credential scopes have separate slots`() = runBlocking {
        val throttle = adapter(maxConcurrentRuns = 1)
        assertTrue(throttle.tryAcquireRunSlot(github, "gh", 10.seconds))
        assertTrue(throttle.tryAcquireRunSlot(gitlab, "gl", 10.seconds))
        assertTrue(throttle.tryAcquireRunSlot(inst111, "i1", 10.seconds))
        assertTrue(throttle.tryAcquireRunSlot(inst222, "i2", 10.seconds))
        assertFalse(throttle.tryAcquireRunSlot(inst111, "i1-again", 10.seconds))
    }

    @Test
    fun `the global cap and the per-credential cap are enforced together`() = runBlocking {
        val throttle = adapter(maxConcurrentRuns = 2, maxRunsGlobal = 3)
        assertTrue(throttle.tryAcquireRunSlot(inst111, "a", 10.seconds))
        assertTrue(throttle.tryAcquireRunSlot(inst111, "b", 10.seconds))
        assertFalse(throttle.tryAcquireRunSlot(inst111, "c", 10.seconds), "inst-111's own cap")
        assertTrue(throttle.tryAcquireRunSlot(inst222, "d", 10.seconds))

        assertFalse(throttle.tryAcquireRunSlot(inst222, "e", 10.seconds), "the global cap")
        assertFalse(throttle.tryAcquireRunSlot(gitlab, "f", 10.seconds), "the global cap")
        assertEquals(1, throttle.activeRunSlots(inst222), "a lease the global cap refused isn't kept")
        assertEquals(0, throttle.activeRunSlots(gitlab))
        assertEquals(3L, connection.sync().zcard(config.globalSlotsKey()))

        throttle.releaseRunSlot(inst111, "a")
        assertEquals(2L, connection.sync().zcard(config.globalSlotsKey()), "released from both")
        assertTrue(throttle.tryAcquireRunSlot(gitlab, "f", 10.seconds))
    }

    @Test
    fun `20 concurrent holders over three scopes stay within both caps`() = runBlocking(Dispatchers.Default) {
        val throttle = adapter(maxConcurrentRuns = 2, maxRunsGlobal = 3)
        val scopes = listOf(inst111, inst222, gitlab)
        val running = scopes.associateWith { AtomicInteger() }
        val total = AtomicInteger()
        val peaks = scopes.associateWith { AtomicInteger() }
        val totalPeak = AtomicInteger()

        (1..20).map { i ->
            async {
                val scope = scopes[i % scopes.size]
                val token = "holder-$i"
                while (!throttle.tryAcquireRunSlot(scope, token, 10.seconds)) {
                    delay(5.milliseconds)
                }
                peaks.getValue(scope).accumulateAndGet(running.getValue(scope).incrementAndGet(), ::maxOf)
                totalPeak.accumulateAndGet(total.incrementAndGet(), ::maxOf)
                delay(30.milliseconds)
                running.getValue(scope).decrementAndGet()
                total.decrementAndGet()
                throttle.releaseRunSlot(scope, token)
            }
        }.awaitAll()

        assertTrue(totalPeak.get() <= 3, "global peak ${totalPeak.get()}")
        peaks.forEach { (scope, peak) -> assertTrue(peak.get() <= 2, "$scope peak ${peak.get()}") }
        assertEquals(0L, connection.sync().zcard(config.globalSlotsKey()))
    }

    @Test
    fun `a run slot whose global lease is gone can't be extended`() = runBlocking {
        val throttle = adapter()
        assertTrue(throttle.tryAcquireRunSlot(inst111, "holder", 10.seconds))
        connection.sync().zrem(config.globalSlotsKey(), "holder")

        assertFalse(throttle.extendRunSlot(inst111, "holder", 10.seconds))
    }

    @Test
    fun `rate-limiting one installation cools down that installation only`() = runBlocking {
        val throttle = adapter(maxConcurrentRuns = 2)
        throttle.registerRateLimit(inst111, 30.seconds)

        assertTrue(checkNotNull(throttle.cooldownRemaining(inst111)) > 29.seconds)
        assertNull(throttle.cooldownRemaining(inst222))
        assertNull(throttle.cooldownRemaining(gitlab))
        assertNull(throttle.cooldownRemaining(github))
        assertTrue(throttle.tryAcquireRunSlot(inst222, "a", 10.seconds))
        assertTrue(throttle.tryAcquireRunSlot(gitlab, "b", 10.seconds))
        assertEquals(1L, connection.sync().exists(config.throttleKeyFor(inst111, "backoff_n")))
        assertEquals(0L, connection.sync().exists(config.throttleKeyFor(inst222, "backoff_n")))
    }

    @Test
    fun `lease expiry comes from the Valkey clock, so caller clock skew can't affect it`() = runBlocking {
        val recording = ArgRecordingCommands(connection.coroutines())
        val throttle = adapter(commands = recording)
        assertTrue(throttle.tryAcquireRunSlot(github, "holder", 10.minutes))
        assertTrue(throttle.extendRunSlot(github, "holder", 10.minutes))

        val (seconds, micros) = connection.sync().time()
        val serverNow = seconds.toLong() * 1000 + micros.toLong() / 1000
        val expiry = checkNotNull(connection.sync().zscore(slotsKey, "holder")).toLong()
        assertTrue(abs(expiry - serverNow - 10.minutes.inWholeMilliseconds) < 2000, "expiry is TIME + ttl")

        val jvmNow = System.currentTimeMillis()
        val timestamps = recording.args
            .mapNotNull { it.toLongOrNull() }
            .filter { abs(it - jvmNow) < 1.days.inWholeMilliseconds }
        assertTrue(timestamps.isEmpty(), "scripts must not receive client timestamps: $timestamps")
    }

    @Test
    fun `unknown retry-after backs off exponentially, caps at the max and resets on success`() = runBlocking {
        val throttle = adapter(random = lowestJitter)
        val steps = (1..4).map {
            connection.sync().del(cooldownKey) // as if the previous cooldown expired
            throttle.registerRateLimit(github, null).inWholeMilliseconds
        }
        assertEquals(listOf(800L, 1600L, 3200L, 3200L), steps)

        throttle.resetBackoff(github)
        connection.sync().del(cooldownKey)
        assertEquals(800L, throttle.registerRateLimit(github, null).inWholeMilliseconds)
    }

    @Test
    fun `jitter spreads the cooldown within 20 percent`() = runBlocking {
        val throttle = adapter()
        val samples = (1..30).map {
            connection.sync().flushall()
            throttle.registerRateLimit(github, null).inWholeMilliseconds
        }
        assertTrue(samples.all { it in 800L..1200L }, "$samples")
        assertTrue(samples.toSet().size > 1, "jitter should vary")
    }

    @Test
    fun `a known retry-after sets the cooldown and is visible to every caller`() = runBlocking {
        val throttle = adapter()
        assertNull(throttle.cooldownRemaining(github))

        assertEquals(30.seconds, throttle.registerRateLimit(github, 30.seconds))
        val remaining = checkNotNull(adapter().cooldownRemaining(github))
        assertTrue(remaining > 29.seconds && remaining <= 30.seconds, "$remaining")
        assertNull(throttle.cooldownRemaining(gitlab))
    }

    @Test
    fun `a shorter rate limit never shortens a running cooldown`() = runBlocking {
        val throttle = adapter()
        throttle.registerRateLimit(github, 30.seconds)
        val effective = throttle.registerRateLimit(github, 2.seconds)
        assertTrue(effective > 29.seconds, "$effective")
    }

    @Test
    fun `scripts recover after the server loses its script cache`() = runBlocking {
        val throttle = adapter()
        throttle.preloadScripts()
        connection.sync().scriptFlush()
        assertTrue(throttle.tryAcquireRunSlot(github, "holder", 10.seconds))
        connection.sync().scriptFlush()
        assertTrue(throttle.extendRunSlot(github, "holder", 10.seconds))
        connection.sync().scriptFlush()
        assertTrue(throttle.registerRateLimit(github, null).isPositive())
    }

    private class ArgRecordingCommands(private val delegate: RedisCoroutinesCommands<String, String>) :
        RedisCoroutinesCommands<String, String> by delegate {
        val args: MutableList<String> = CopyOnWriteArrayList()

        override suspend fun <T> evalsha(
            digest: String,
            type: ScriptOutputType,
            keys: Array<String>,
            vararg values: String,
        ): T? {
            args += values
            return delegate.evalsha(digest, type, keys, *values)
        }

        override suspend fun <T> eval(
            script: String,
            type: ScriptOutputType,
            keys: Array<String>,
            vararg values: String,
        ): T? {
            args += values
            return delegate.eval(script, type, keys, *values)
        }
    }
}
