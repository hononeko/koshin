package dev.hononeko.koshin.adapters.valkey

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisCommandExecutionException
import io.lettuce.core.RedisNoScriptException
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

/** A Lua script run with `EVALSHA`, falling back to `EVAL` (and reloading) when the server lost its script cache. */
@OptIn(ExperimentalLettuceCoroutinesApi::class)
internal class LuaScript(private val commands: RedisCoroutinesCommands<String, String>, private val source: String) {
    private val sha = AtomicReference<String?>(null)

    suspend fun load() {
        sha.set(commands.scriptLoad(source))
    }

    suspend fun evalLong(keys: Array<String>, vararg args: String): Long {
        val cached = sha.get() ?: commands.scriptLoad(source)?.also { sha.set(it) }
        if (cached != null) {
            try {
                return checkNotNull(commands.evalsha<Long>(cached, ScriptOutputType.INTEGER, keys, *args)) {
                    "Lua script returned null"
                }
            } catch (e: RedisNoScriptException) {
                logger.debug("NOSCRIPT for sha {}, falling back to EVAL", cached, e)
            } catch (e: RedisCommandExecutionException) {
                if (e.message?.contains("NOSCRIPT", ignoreCase = true) == true) {
                    logger.debug("NOSCRIPT for sha {}, falling back to EVAL", cached, e)
                } else {
                    throw e
                }
            }
        }
        val result = checkNotNull(commands.eval<Long>(source, ScriptOutputType.INTEGER, keys, *args)) {
            "Lua script returned null"
        }
        commands.scriptLoad(source)?.let { sha.set(it) }
        return result
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(LuaScript::class.java)
    }
}
