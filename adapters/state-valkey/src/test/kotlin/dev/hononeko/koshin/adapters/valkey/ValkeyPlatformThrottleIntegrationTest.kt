package dev.hononeko.koshin.adapters.valkey

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

    private val slotsKey = config.throttleKeyFor(Platform.GITHUB, "slots")
    private val cooldownKey = config.throttleKeyFor(Platform.GITHUB, "cooldown")

    @BeforeEach
    fun flushDb() {
        connection.sync().flushall()
    }

    private fun adapter(
        maxConcurrentRuns: Int = 3,
        random: Random = Random.Default,
        commands: RedisCoroutinesCommands<String, String> = connection.coroutines(),
    ) = ValkeyPlatformThrottleAdapter(
        commands = commands,
        config = config,
        throttle = PlatformThrottleConfig(maxConcurrentRuns, backoffBase = 1.seconds, backoffMax = 4.seconds),
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
                while (!throttle.tryAcquireRunSlot(Platform.GITHUB, token, 10.seconds)) {
                    delay(5.milliseconds)
                }
                peak.accumulateAndGet(current.incrementAndGet(), ::maxOf)
                delay(30.milliseconds)
                current.decrementAndGet()
                throttle.releaseRunSlot(Platform.GITHUB, token)
            }
        }.awaitAll()

        assertEquals(3, peak.get())
        assertEquals(0L, connection.sync().zcard(slotsKey))
    }

    @Test
    fun `a crashed holder's lease expires and frees its slot`() = runBlocking {
        val throttle = adapter(maxConcurrentRuns = 1)
        assertTrue(throttle.tryAcquireRunSlot(Platform.GITHUB, "crashed", 1.seconds))
        val start = TimeSource.Monotonic.markNow()
        assertFalse(throttle.tryAcquireRunSlot(Platform.GITHUB, "waiting", 1.seconds))

        while (!throttle.tryAcquireRunSlot(Platform.GITHUB, "waiting", 1.seconds)) {
            check(start.elapsedNow() < 5.seconds) { "lease never expired" }
            delay(50.milliseconds)
        }
        assertTrue(start.elapsedNow() >= 900.milliseconds)
        assertFalse(throttle.extendRunSlot(Platform.GITHUB, "crashed", 1.seconds), "an expired lease can't be extended")
    }

    @Test
    fun `extending keeps the lease alive past its original ttl`() = runBlocking {
        val throttle = adapter(maxConcurrentRuns = 1)
        assertTrue(throttle.tryAcquireRunSlot(Platform.GITHUB, "holder", 1.seconds))
        repeat(3) {
            delay(500.milliseconds)
            assertTrue(throttle.extendRunSlot(Platform.GITHUB, "holder", 1.seconds))
        }
        assertFalse(throttle.tryAcquireRunSlot(Platform.GITHUB, "other", 1.seconds))
        assertTrue(throttle.tryAcquireRunSlot(Platform.GITHUB, "holder", 1.seconds), "re-acquiring an own lease")
    }

    @Test
    fun `platforms have separate slots`() = runBlocking {
        val throttle = adapter(maxConcurrentRuns = 1)
        assertTrue(throttle.tryAcquireRunSlot(Platform.GITHUB, "gh", 10.seconds))
        assertTrue(throttle.tryAcquireRunSlot(Platform.GITLAB, "gl", 10.seconds))
    }

    @Test
    fun `lease expiry comes from the Valkey clock, so caller clock skew can't affect it`() = runBlocking {
        val recording = ArgRecordingCommands(connection.coroutines())
        val throttle = adapter(commands = recording)
        assertTrue(throttle.tryAcquireRunSlot(Platform.GITHUB, "holder", 10.minutes))
        assertTrue(throttle.extendRunSlot(Platform.GITHUB, "holder", 10.minutes))

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
            throttle.registerRateLimit(Platform.GITHUB, null).inWholeMilliseconds
        }
        assertEquals(listOf(800L, 1600L, 3200L, 3200L), steps)

        throttle.resetBackoff(Platform.GITHUB)
        connection.sync().del(cooldownKey)
        assertEquals(800L, throttle.registerRateLimit(Platform.GITHUB, null).inWholeMilliseconds)
    }

    @Test
    fun `jitter spreads the cooldown within 20 percent`() = runBlocking {
        val throttle = adapter()
        val samples = (1..30).map {
            connection.sync().flushall()
            throttle.registerRateLimit(Platform.GITHUB, null).inWholeMilliseconds
        }
        assertTrue(samples.all { it in 800L..1200L }, "$samples")
        assertTrue(samples.toSet().size > 1, "jitter should vary")
    }

    @Test
    fun `a known retry-after sets the cooldown and is visible to every caller`() = runBlocking {
        val throttle = adapter()
        assertNull(throttle.cooldownRemaining(Platform.GITHUB))

        assertEquals(30.seconds, throttle.registerRateLimit(Platform.GITHUB, 30.seconds))
        val remaining = checkNotNull(adapter().cooldownRemaining(Platform.GITHUB))
        assertTrue(remaining > 29.seconds && remaining <= 30.seconds, "$remaining")
        assertNull(throttle.cooldownRemaining(Platform.GITLAB))
    }

    @Test
    fun `a shorter rate limit never shortens a running cooldown`() = runBlocking {
        val throttle = adapter()
        throttle.registerRateLimit(Platform.GITHUB, 30.seconds)
        val effective = throttle.registerRateLimit(Platform.GITHUB, 2.seconds)
        assertTrue(effective > 29.seconds, "$effective")
    }

    @Test
    fun `scripts recover after the server loses its script cache`() = runBlocking {
        val throttle = adapter()
        throttle.preloadScripts()
        connection.sync().scriptFlush()
        assertTrue(throttle.tryAcquireRunSlot(Platform.GITHUB, "holder", 10.seconds))
        connection.sync().scriptFlush()
        assertTrue(throttle.extendRunSlot(Platform.GITHUB, "holder", 10.seconds))
        connection.sync().scriptFlush()
        assertTrue(throttle.registerRateLimit(Platform.GITHUB, null).isPositive())
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
