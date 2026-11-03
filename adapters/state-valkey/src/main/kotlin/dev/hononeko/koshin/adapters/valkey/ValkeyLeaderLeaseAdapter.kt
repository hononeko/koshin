package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.ports.LeaderLeasePort
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlin.time.Duration

/** The worker dispatcher's leader lease: one key holding the holder's ID, expiring on Valkey's clock (`PX`). */
@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyLeaderLeaseAdapter(
    commands: RedisCoroutinesCommands<String, String>,
    private val config: ValkeyConfig = ValkeyConfig(),
) : LeaderLeasePort {
    constructor(
        connection: StatefulRedisConnection<String, String>,
        config: ValkeyConfig = ValkeyConfig(),
    ) : this(connection.coroutines(), config)

    private val acquireScript = LuaScript(commands, ACQUIRE_OR_RENEW_SCRIPT)
    private val releaseScript = LuaScript(commands, RELEASE_SCRIPT)

    suspend fun preloadScripts() {
        acquireScript.load()
        releaseScript.load()
    }

    override suspend fun acquireOrRenew(holderId: String, ttl: Duration): Boolean {
        require(holderId.isNotBlank()) { "holderId cannot be blank" }
        require(ttl.isFinite() && ttl.inWholeMilliseconds >= 1) { "ttl must be at least 1ms and finite: $ttl" }
        val keys = arrayOf(config.dispatcherLeaderKey())
        return acquireScript.evalLong(keys, holderId, ttl.inWholeMilliseconds.toString()) == 1L
    }

    override suspend fun release(holderId: String) {
        withContext(NonCancellable) {
            releaseScript.evalLong(arrayOf(config.dispatcherLeaderKey()), holderId)
        }
    }

    companion object {
        /** KEYS: leader. ARGV: holderId, ttl (ms). 1 if [holderId] holds the lease now, 0 if someone else does. */
        val ACQUIRE_OR_RENEW_SCRIPT =
            """
            local holder = redis.call('GET', KEYS[1])
            if holder == false then
              redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
              return 1
            end
            if holder == ARGV[1] then
              redis.call('PEXPIRE', KEYS[1], ARGV[2])
              return 1
            end
            return 0
            """.trimIndent()

        /** KEYS: leader. ARGV: holderId. */
        val RELEASE_SCRIPT =
            """
            if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
            return 0
            """.trimIndent()
    }
}
