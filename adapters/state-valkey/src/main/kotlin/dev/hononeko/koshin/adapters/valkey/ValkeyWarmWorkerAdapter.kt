package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.ports.WarmWorkerPort
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlin.time.Duration

/** Warm-worker leases in one ZSET (`workerId → expiry`), timed by Valkey's clock like the run slots. */
@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyWarmWorkerAdapter(
    private val commands: RedisCoroutinesCommands<String, String>,
    private val config: ValkeyConfig = ValkeyConfig(),
) : WarmWorkerPort {
    constructor(
        connection: StatefulRedisConnection<String, String>,
        config: ValkeyConfig = ValkeyConfig(),
    ) : this(connection.coroutines(), config)

    private val renewScript = LuaScript(commands, RENEW_SCRIPT)
    private val countScript = LuaScript(commands, COUNT_SCRIPT)

    suspend fun preloadScripts() {
        renewScript.load()
        countScript.load()
    }

    override suspend fun renew(workerId: String, ttl: Duration) {
        require(workerId.isNotBlank()) { "workerId cannot be blank" }
        require(ttl.isFinite() && ttl.inWholeMilliseconds >= 1) { "ttl must be at least 1ms and finite: $ttl" }
        renewScript.evalLong(arrayOf(config.warmWorkersKey()), workerId, ttl.inWholeMilliseconds.toString())
    }

    override suspend fun remove(workerId: String) {
        withContext(NonCancellable) {
            commands.zrem(config.warmWorkersKey(), workerId)
        }
    }

    override suspend fun liveCount(): Int = countScript.evalLong(arrayOf(config.warmWorkersKey())).toInt()

    companion object {
        /** KEYS: warm. ARGV: workerId, ttl (ms). Drops expired leases first. */
        val RENEW_SCRIPT =
            """
            ${LeaseScripts.NOW}
            local ttl = tonumber(ARGV[2])
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
            ${LeaseScripts.LEASE}
            """.trimIndent()

        /** KEYS: warm. Leases expiring after now. */
        val COUNT_SCRIPT =
            """
            ${LeaseScripts.NOW}
            return redis.call('ZCOUNT', KEYS[1], '(' .. now, '+inf')
            """.trimIndent()
    }
}
