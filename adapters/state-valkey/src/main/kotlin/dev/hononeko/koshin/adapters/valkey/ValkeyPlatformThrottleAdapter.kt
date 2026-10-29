package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

data class PlatformThrottleConfig(
    val maxConcurrentRuns: Int = DEFAULT_MAX_CONCURRENT_RUNS,
    val backoffBase: Duration = DEFAULT_BACKOFF_BASE,
    val backoffMax: Duration = DEFAULT_BACKOFF_MAX,
) {
    init {
        require(maxConcurrentRuns >= 1) { "maxConcurrentRuns must be at least 1: $maxConcurrentRuns" }
        require(backoffBase.isPositive() && backoffBase.isFinite()) { "backoffBase must be positive: $backoffBase" }
        require(backoffMax >= backoffBase && backoffMax.isFinite()) {
            "backoffMax ($backoffMax) must be >= backoffBase ($backoffBase)"
        }
    }

    companion object {
        const val DEFAULT_MAX_CONCURRENT_RUNS = 3
        val DEFAULT_BACKOFF_BASE: Duration = 60.seconds
        val DEFAULT_BACKOFF_MAX: Duration = 3600.seconds
    }
}

/**
 * Per-platform run slots (a ZSET of `token → lease expiry`) and rate-limit cooldown. Lease expiry is computed from
 * Valkey's own clock (`TIME`) inside the scripts, so clock skew between worker pods can't expire other pods' leases.
 */
@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyPlatformThrottleAdapter(
    private val commands: RedisCoroutinesCommands<String, String>,
    private val config: ValkeyConfig = ValkeyConfig(),
    private val throttle: PlatformThrottleConfig = PlatformThrottleConfig(),
    private val random: Random = Random.Default,
) : PlatformThrottlePort {
    constructor(
        connection: StatefulRedisConnection<String, String>,
        config: ValkeyConfig = ValkeyConfig(),
        throttle: PlatformThrottleConfig = PlatformThrottleConfig(),
    ) : this(connection.coroutines(), config, throttle)

    private val acquireSlotScript = LuaScript(commands, ACQUIRE_SLOT_SCRIPT)
    private val extendSlotScript = LuaScript(commands, EXTEND_SLOT_SCRIPT)
    private val registerRateLimitScript = LuaScript(commands, REGISTER_RATE_LIMIT_SCRIPT)

    suspend fun preloadScripts() {
        acquireSlotScript.load()
        extendSlotScript.load()
        registerRateLimitScript.load()
    }

    override suspend fun tryAcquireRunSlot(platform: Platform, token: String, ttl: Duration): Boolean {
        require(token.isNotBlank()) { "token cannot be blank" }
        val keys = arrayOf(config.throttleKeyFor(platform, SLOTS))
        return acquireSlotScript.evalLong(keys, token, ttl.toLeaseMillis(), throttle.maxConcurrentRuns.toString()) == 1L
    }

    override suspend fun extendRunSlot(platform: Platform, token: String, ttl: Duration): Boolean {
        require(token.isNotBlank()) { "token cannot be blank" }
        val keys = arrayOf(config.throttleKeyFor(platform, SLOTS))
        return extendSlotScript.evalLong(keys, token, ttl.toLeaseMillis()) == 1L
    }

    override suspend fun releaseRunSlot(platform: Platform, token: String) {
        withContext(NonCancellable) {
            commands.zrem(config.throttleKeyFor(platform, SLOTS), token)
        }
    }

    override suspend fun cooldownRemaining(platform: Platform): Duration? {
        val millis = commands.pttl(config.throttleKeyFor(platform, COOLDOWN)) ?: return null
        return if (millis > 0) millis.milliseconds else null
    }

    /** Cooldown for `retryAfter` if known, else `min(base × 2^n, max) ± 20%`; never shortens a longer cooldown. */
    override suspend fun registerRateLimit(platform: Platform, retryAfter: Duration?): Duration {
        val keys = arrayOf(config.throttleKeyFor(platform, COOLDOWN), config.throttleKeyFor(platform, BACKOFF_N))
        val retryAfterMillis = retryAfter?.takeIf { it.isPositive() }?.toLeaseMillis() ?: UNKNOWN
        val jitter = 1 + (random.nextDouble() * 2 - 1) * JITTER
        return registerRateLimitScript.evalLong(
            keys,
            retryAfterMillis,
            throttle.backoffBase.inWholeMilliseconds.toString(),
            throttle.backoffMax.inWholeMilliseconds.toString(),
            jitter.toString(),
        ).milliseconds
    }

    override suspend fun resetBackoff(platform: Platform) {
        commands.del(config.throttleKeyFor(platform, BACKOFF_N))
    }

    private fun Duration.toLeaseMillis(): String {
        require(isFinite() && inWholeMilliseconds >= 1) { "Duration must be at least 1ms and finite: $this" }
        return inWholeMilliseconds.toString()
    }

    companion object {
        private const val SLOTS = "slots"
        private const val COOLDOWN = "cooldown"
        private const val BACKOFF_N = "backoff_n"
        private const val UNKNOWN = "-1"
        private const val JITTER = 0.2

        private val NOW =
            """
            local t = redis.call('TIME')
            local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
            """.trimIndent()

        // Keeps the ZSET's own TTL at least as long as its longest lease, so an abandoned key goes away.
        private val LEASE =
            """
            redis.call('ZADD', KEYS[1], now + ttl, ARGV[1])
            if redis.call('PTTL', KEYS[1]) < ttl then redis.call('PEXPIRE', KEYS[1], ttl) end
            return 1
            """.trimIndent()

        val ACQUIRE_SLOT_SCRIPT =
            """
            $NOW
            local ttl = tonumber(ARGV[2])
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
            if not redis.call('ZSCORE', KEYS[1], ARGV[1]) and redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
              return 0
            end
            $LEASE
            """.trimIndent()

        val EXTEND_SLOT_SCRIPT =
            """
            $NOW
            local ttl = tonumber(ARGV[2])
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
            if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
            $LEASE
            """.trimIndent()

        val REGISTER_RATE_LIMIT_SCRIPT =
            """
            local n = tonumber(redis.call('GET', KEYS[2]) or '0')
            local ttl = tonumber(ARGV[1])
            if ttl < 0 then
              local backoff = math.min(tonumber(ARGV[2]) * 2 ^ math.min(n, 32), tonumber(ARGV[3]))
              ttl = math.max(math.floor(backoff * tonumber(ARGV[4])), 1)
            end
            redis.call('INCR', KEYS[2])
            local remaining = redis.call('PTTL', KEYS[1])
            if remaining >= ttl then return remaining end
            redis.call('SET', KEYS[1], '1', 'PX', ttl)
            return ttl
            """.trimIndent()
    }
}
