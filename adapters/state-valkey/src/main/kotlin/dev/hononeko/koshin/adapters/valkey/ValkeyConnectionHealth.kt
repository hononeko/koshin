package dev.hononeko.koshin.adapters.valkey

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration

internal fun StatefulRedisConnection<*, *>.isConnected(): Boolean = isOpen

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyConnectionHealth(
    private val connection: StatefulRedisConnection<String, String>,
    private val commands: RedisCoroutinesCommands<String, String> = connection.coroutines(),
    private val pingTimeout: Duration = ValkeyConfig.DEFAULT_COMMAND_TIMEOUT,
) {
    init {
        require(pingTimeout.isFinite() && pingTimeout.isPositive()) {
            "pingTimeout must be positive and finite: $pingTimeout"
        }
    }

    fun isConnected(): Boolean = connection.isConnected()

    suspend fun ping(): Boolean {
        if (!connection.isConnected()) return false
        return try {
            withTimeoutOrNull(pingTimeout) {
                commands.ping() == PONG_RESPONSE
            } ?: false
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") _: Exception) {
            false
        }
    }

    companion object {
        const val PONG_RESPONSE = "PONG"
    }
}
