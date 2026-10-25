package dev.hononeko.koshin.adapters.valkey

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.CancellationException

fun StatefulRedisConnection<*, *>.isConnected(): Boolean = isOpen

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyConnectionHealth(
    private val connection: StatefulRedisConnection<String, String>,
    private val commands: RedisCoroutinesCommands<String, String> = connection.coroutines(),
) {
    fun isConnected(): Boolean = connection.isConnected()

    suspend fun ping(): Boolean {
        if (!connection.isConnected()) return false
        return try {
            commands.ping() == PONG_RESPONSE
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
