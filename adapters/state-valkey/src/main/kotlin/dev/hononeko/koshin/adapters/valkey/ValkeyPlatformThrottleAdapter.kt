package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.CredentialScope
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

/**
 * Run-slot caps: [maxRunsPerCredential] runs per credential scope (`MAX_CONCURRENT_RUNS_PER_CREDENTIAL`) and
 * [maxRunsGlobal] across them all (`MAX_CONCURRENT_RUNS_GLOBAL`), plus the rate-limit cooldown's backoff.
 */
data class PlatformThrottleConfig(
    val maxRunsPerCredential: Int = DEFAULT_MAX_RUNS_PER_CREDENTIAL,
    val maxRunsGlobal: Int = DEFAULT_MAX_RUNS_GLOBAL,
    val backoffBase: Duration = DEFAULT_BACKOFF_BASE,
    val backoffMax: Duration = DEFAULT_BACKOFF_MAX,
) {
    init {
        require(maxRunsPerCredential >= 1) { "maxRunsPerCredential must be at least 1: $maxRunsPerCredential" }
        require(maxRunsGlobal >= 1) { "maxRunsGlobal must be at least 1: $maxRunsGlobal" }
        require(backoffBase.isPositive() && backoffBase.isFinite()) { "backoffBase must be positive: $backoffBase" }
        require(backoffMax >= backoffBase && backoffMax.isFinite()) {
            "backoffMax ($backoffMax) must be >= backoffBase ($backoffBase)"
        }
    }

    companion object {
        const val DEFAULT_MAX_RUNS_PER_CREDENTIAL = 2
        const val DEFAULT_MAX_RUNS_GLOBAL = 6
        val DEFAULT_BACKOFF_BASE: Duration = 60.seconds
        val DEFAULT_BACKOFF_MAX: Duration = 3600.seconds
    }
}

/**
 * Run slots and rate-limit cooldowns per credential scope. A run slot is a lease in two ZSETs of
 * `token → lease expiry`: the scope's (`koshin:{throttle:<scope>}:slots`) and the global one
 * (`koshin:{throttle:global}:slots`). They're in different hash slots, so they're leased one after the other and the
 * first given back if the second fails. Lease expiry is computed from Valkey's own clock (`TIME`) inside the scripts,
 * so clock skew between worker pods can't expire other pods' leases.
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
    private val countSlotsScript = LuaScript(commands, COUNT_SLOTS_SCRIPT)

    suspend fun preloadScripts() {
        acquireSlotScript.load()
        extendSlotScript.load()
        registerRateLimitScript.load()
        countSlotsScript.load()
    }

    /** The scope's lease first: a full scope then never holds a global slot, even briefly. */
    override suspend fun tryAcquireRunSlot(scope: CredentialScope, token: String, ttl: Duration): Boolean {
        require(token.isNotBlank()) { "token cannot be blank" }
        val millis = ttl.toLeaseMillis()
        val scopeKey = config.throttleKeyFor(scope, SLOTS)
        if (!lease(scopeKey, token, millis, throttle.maxRunsPerCredential)) return false
        val global = try {
            lease(config.globalSlotsKey(), token, millis, throttle.maxRunsGlobal)
        } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
            // The global lease may have been taken before the reply was lost
            withContext(NonCancellable) { runCatching { releaseRunSlot(scope, token) } }
            throw e
        }
        if (!global) withContext(NonCancellable) { commands.zrem(scopeKey, token) }
        return global
    }

    private suspend fun lease(key: String, token: String, millis: String, max: Int): Boolean =
        acquireSlotScript.evalLong(arrayOf(key), token, millis, max.toString()) == 1L

    override suspend fun extendRunSlot(scope: CredentialScope, token: String, ttl: Duration): Boolean {
        require(token.isNotBlank()) { "token cannot be blank" }
        val millis = ttl.toLeaseMillis()
        return extend(config.throttleKeyFor(scope, SLOTS), token, millis) &&
            extend(config.globalSlotsKey(), token, millis)
    }

    private suspend fun extend(key: String, token: String, millis: String): Boolean =
        extendSlotScript.evalLong(arrayOf(key), token, millis) == 1L

    /** Both leases, the global one even if releasing the scope's fails. */
    override suspend fun releaseRunSlot(scope: CredentialScope, token: String) {
        withContext(NonCancellable) {
            val scopeRelease = runCatching { commands.zrem(config.throttleKeyFor(scope, SLOTS), token) }
            commands.zrem(config.globalSlotsKey(), token)
            scopeRelease.getOrThrow()
        }
    }

    override suspend fun activeRunSlots(scope: CredentialScope): Int =
        countSlotsScript.evalLong(arrayOf(config.throttleKeyFor(scope, SLOTS))).toInt()

    override suspend fun cooldownRemaining(scope: CredentialScope): Duration? {
        val millis = commands.pttl(config.throttleKeyFor(scope, COOLDOWN)) ?: return null
        return if (millis > 0) millis.milliseconds else null
    }

    /** Cooldown for `retryAfter` if known, else `min(base × 2^n, max) ± 20%`; never shortens a longer cooldown. */
    override suspend fun registerRateLimit(scope: CredentialScope, retryAfter: Duration?): Duration {
        val keys = arrayOf(config.throttleKeyFor(scope, COOLDOWN), config.throttleKeyFor(scope, BACKOFF_N))
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

    override suspend fun resetBackoff(scope: CredentialScope) {
        commands.del(config.throttleKeyFor(scope, BACKOFF_N))
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

        val ACQUIRE_SLOT_SCRIPT =
            """
            ${LeaseScripts.NOW}
            local ttl = tonumber(ARGV[2])
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
            if not redis.call('ZSCORE', KEYS[1], ARGV[1]) and redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
              return 0
            end
            ${LeaseScripts.LEASE}
            """.trimIndent()

        val EXTEND_SLOT_SCRIPT =
            """
            ${LeaseScripts.NOW}
            local ttl = tonumber(ARGV[2])
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
            if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
            ${LeaseScripts.LEASE}
            """.trimIndent()

        /** Leases that haven't expired. */
        val COUNT_SLOTS_SCRIPT =
            """
            ${LeaseScripts.NOW}
            return redis.call('ZCOUNT', KEYS[1], '(' .. now, '+inf')
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
