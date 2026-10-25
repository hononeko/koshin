package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.ReleaseResult
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisCommandExecutionException
import io.lettuce.core.RedisNoScriptException
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyRepoCoordinationAdapter(
    private val commands: RedisCoroutinesCommands<String, String>,
    private val config: ValkeyConfig = ValkeyConfig(),
) : RepoCoordinationPort {
    constructor(
        connection: StatefulRedisConnection<String, String>,
        config: ValkeyConfig = ValkeyConfig(),
    ) : this(connection.coroutines(), config)

    private val logger = LoggerFactory.getLogger(ValkeyRepoCoordinationAdapter::class.java)

    private val acquireOrMarkDirtySha = AtomicReference<String?>(null)
    private val extendLockSha = AtomicReference<String?>(null)
    private val releaseLockAndTakeDirtySha = AtomicReference<String?>(null)
    private val releaseLockSha = AtomicReference<String?>(null)

    suspend fun preloadScripts() {
        acquireOrMarkDirtySha.set(commands.scriptLoad(ACQUIRE_OR_MARK_DIRTY_SCRIPT))
        extendLockSha.set(commands.scriptLoad(EXTEND_LOCK_SCRIPT))
        releaseLockAndTakeDirtySha.set(commands.scriptLoad(RELEASE_LOCK_AND_TAKE_DIRTY_SCRIPT))
        releaseLockSha.set(commands.scriptLoad(RELEASE_LOCK_SCRIPT))
    }

    override suspend fun acquireOrMarkDirty(
        repo: RepoRef,
        token: String,
        lockTtl: Duration,
        dirtyTtl: Duration,
    ): AcquireResult {
        require(token.isNotBlank()) { "token cannot be blank" }
        require(lockTtl.isFinite()) { "lockTtl must be finite: $lockTtl" }
        require(dirtyTtl.isFinite()) { "dirtyTtl must be finite: $dirtyTtl" }
        val lockSeconds = lockTtl.inWholeSeconds
        val dirtySeconds = dirtyTtl.inWholeSeconds
        require(lockSeconds >= 1) { "lockTtl must be at least 1 second: $lockTtl" }
        require(dirtySeconds >= 1) { "dirtyTtl must be at least 1 second: $dirtyTtl" }

        val keys = arrayOf(config.lockKeyFor(repo), config.dirtyKeyFor(repo))
        val outcome = evalIntegerScript(
            script = ACQUIRE_OR_MARK_DIRTY_SCRIPT,
            shaRef = acquireOrMarkDirtySha,
            keys = keys,
            token,
            lockSeconds.toString(),
            dirtySeconds.toString(),
        )

        return if (outcome == ACQUIRED_RESULT) {
            if (config.lockReplicaAckTimeoutMs > 0) {
                awaitReplicaAck(repo)
            }
            AcquireResult.ACQUIRED
        } else {
            AcquireResult.COALESCED
        }
    }

    override suspend fun extendLock(repo: RepoRef, token: String, ttl: Duration): Boolean {
        require(token.isNotBlank()) { "token cannot be blank" }
        require(ttl.isFinite()) { "ttl must be finite: $ttl" }
        val ttlSeconds = ttl.inWholeSeconds
        require(ttlSeconds >= 1) { "ttl must be at least 1 second: $ttl" }

        val keys = arrayOf(config.lockKeyFor(repo))
        val outcome = evalIntegerScript(
            script = EXTEND_LOCK_SCRIPT,
            shaRef = extendLockSha,
            keys = keys,
            token,
            ttlSeconds.toString(),
        )
        return outcome == EXTENDED_RESULT
    }

    override suspend fun releaseLockAndTakeDirty(repo: RepoRef, token: String): ReleaseResult {
        require(token.isNotBlank()) { "token cannot be blank" }

        return withContext(NonCancellable) {
            val keys = arrayOf(config.lockKeyFor(repo), config.dirtyKeyFor(repo))
            val outcome = evalIntegerScript(
                script = RELEASE_LOCK_AND_TAKE_DIRTY_SCRIPT,
                shaRef = releaseLockAndTakeDirtySha,
                keys = keys,
                token,
            )
            when (outcome) {
                NOT_OWNER_RESULT -> ReleaseResult.NOT_OWNER
                DIRTY_RESULT -> ReleaseResult.DIRTY
                CLEAN_RESULT -> ReleaseResult.CLEAN
                else -> error("Unexpected releaseLockAndTakeDirty script result: $outcome")
            }
        }
    }

    override suspend fun releaseLock(repo: RepoRef, token: String): Boolean {
        require(token.isNotBlank()) { "token cannot be blank" }

        return withContext(NonCancellable) {
            val keys = arrayOf(config.lockKeyFor(repo))
            val outcome = evalIntegerScript(
                script = RELEASE_LOCK_SCRIPT,
                shaRef = releaseLockSha,
                keys = keys,
                token,
            )
            outcome == RELEASED_RESULT
        }
    }

    private suspend fun awaitReplicaAck(repo: RepoRef) {
        try {
            val ackedReplicas = commands.waitForReplication(REPLICA_ACK_COUNT, config.lockReplicaAckTimeoutMs)
            if (ackedReplicas == null || ackedReplicas < REPLICA_ACK_COUNT) {
                logger.warn(
                    "WAIT for lock on {} acknowledged by {} replicas (expected >= {}) within {}ms",
                    repo,
                    ackedReplicas ?: 0L,
                    REPLICA_ACK_COUNT,
                    config.lockReplicaAckTimeoutMs,
                )
            }
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            logger.warn(
                "WAIT for lock replication on {} failed after lock was already acquired on primary",
                repo,
                e,
            )
        }
    }

    private suspend fun evalIntegerScript(
        script: String,
        shaRef: AtomicReference<String?>,
        keys: Array<String>,
        vararg values: String,
    ): Long {
        val sha = shaRef.get() ?: commands.scriptLoad(script)?.also { shaRef.set(it) }
        if (sha != null) {
            try {
                return checkNotNull(commands.evalsha<Long>(sha, ScriptOutputType.INTEGER, keys, *values)) {
                    "Lua script returned null"
                }
            } catch (e: RedisNoScriptException) {
                logger.debug("NOSCRIPT for sha {}, falling back to EVAL", sha, e)
            } catch (e: RedisCommandExecutionException) {
                if (e.message?.contains("NOSCRIPT", ignoreCase = true) == true) {
                    logger.debug("NOSCRIPT for sha {}, falling back to EVAL", sha, e)
                } else {
                    throw e
                }
            }
        }
        val result = checkNotNull(commands.eval<Long>(script, ScriptOutputType.INTEGER, keys, *values)) {
            "Lua script returned null"
        }
        commands.scriptLoad(script)?.let { shaRef.set(it) }
        return result
    }

    companion object {
        private const val ACQUIRED_RESULT = 1L
        private const val EXTENDED_RESULT = 1L
        private const val RELEASED_RESULT = 1L
        private const val NOT_OWNER_RESULT = -1L
        private const val DIRTY_RESULT = 1L
        private const val CLEAN_RESULT = 0L
        private const val REPLICA_ACK_COUNT = 1

        val ACQUIRE_OR_MARK_DIRTY_SCRIPT =
            """
            if redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2]) then return 1 end
            redis.call("SET", KEYS[2], "1", "EX", ARGV[3])
            return 0
            """.trimIndent()

        val EXTEND_LOCK_SCRIPT =
            """
            if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("EXPIRE", KEYS[1], ARGV[2]) end
            return 0
            """.trimIndent()

        val RELEASE_LOCK_AND_TAKE_DIRTY_SCRIPT =
            """
            if redis.call("GET", KEYS[1]) ~= ARGV[1] then return -1 end
            redis.call("DEL", KEYS[1])
            if redis.call("GETDEL", KEYS[2]) then return 1 end
            return 0
            """.trimIndent()

        val RELEASE_LOCK_SCRIPT =
            """
            if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end
            return 0
            """.trimIndent()
    }
}
