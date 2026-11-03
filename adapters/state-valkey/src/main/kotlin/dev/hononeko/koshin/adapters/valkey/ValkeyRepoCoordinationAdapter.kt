package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.EnqueueDecision
import dev.hononeko.koshin.core.ports.ReleaseResult
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
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

    private val enqueueOrCoalesceScript = LuaScript(commands, ENQUEUE_OR_COALESCE_SCRIPT)
    private val acquireOrMarkDirtyScript = LuaScript(commands, ACQUIRE_OR_MARK_DIRTY_SCRIPT)
    private val extendLockScript = LuaScript(commands, EXTEND_LOCK_SCRIPT)
    private val releaseLockAndTakeDirtyScript = LuaScript(commands, RELEASE_LOCK_AND_TAKE_DIRTY_SCRIPT)
    private val releaseLockScript = LuaScript(commands, RELEASE_LOCK_SCRIPT)

    suspend fun preloadScripts() {
        enqueueOrCoalesceScript.load()
        acquireOrMarkDirtyScript.load()
        extendLockScript.load()
        releaseLockAndTakeDirtyScript.load()
        releaseLockScript.load()
    }

    override suspend fun enqueueOrCoalesce(repo: RepoRef, queuedTtl: Duration, dirtyTtl: Duration): EnqueueDecision {
        val keys = arrayOf(config.lockKeyFor(repo), config.dirtyKeyFor(repo), config.queuedKeyFor(repo))
        val outcome = enqueueOrCoalesceScript.evalLong(keys, queuedTtl.toSeconds(), dirtyTtl.toSeconds())
        return when (outcome) {
            ENQUEUE_RESULT -> EnqueueDecision.ENQUEUE
            ALREADY_QUEUED_RESULT -> EnqueueDecision.ALREADY_QUEUED
            MARKED_DIRTY_RESULT -> EnqueueDecision.MARKED_DIRTY
            else -> error("Unexpected enqueueOrCoalesce script result: $outcome")
        }
    }

    override suspend fun clearQueued(repo: RepoRef) {
        withContext(NonCancellable) {
            commands.del(config.queuedKeyFor(repo))
        }
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

        val keys = arrayOf(config.lockKeyFor(repo), config.dirtyKeyFor(repo), config.queuedKeyFor(repo))
        val outcome = acquireOrMarkDirtyScript.evalLong(
            keys,
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
        val outcome = extendLockScript.evalLong(
            keys,
            token,
            ttlSeconds.toString(),
        )
        return outcome == EXTENDED_RESULT
    }

    override suspend fun releaseLockAndTakeDirty(repo: RepoRef, token: String, queuedTtl: Duration?): ReleaseResult {
        require(token.isNotBlank()) { "token cannot be blank" }
        val queuedSeconds = queuedTtl?.toSeconds() ?: NO_QUEUED_MARK

        return withContext(NonCancellable) {
            val keys = arrayOf(config.lockKeyFor(repo), config.dirtyKeyFor(repo), config.queuedKeyFor(repo))
            val outcome = releaseLockAndTakeDirtyScript.evalLong(
                keys,
                token,
                queuedSeconds,
            )
            when (outcome) {
                NOT_OWNER_RESULT -> ReleaseResult.NOT_OWNER
                DIRTY_RESULT -> ReleaseResult.DIRTY
                CLEAN_RESULT -> ReleaseResult.CLEAN
                else -> error("Unexpected releaseLockAndTakeDirty script result: $outcome")
            }
        }
    }

    override suspend fun releaseLock(repo: RepoRef, token: String, queuedTtl: Duration?): Boolean {
        require(token.isNotBlank()) { "token cannot be blank" }
        val queuedSeconds = queuedTtl?.toSeconds() ?: NO_QUEUED_MARK

        return withContext(NonCancellable) {
            val keys = arrayOf(config.lockKeyFor(repo), config.queuedKeyFor(repo))
            val outcome = releaseLockScript.evalLong(
                keys,
                token,
                queuedSeconds,
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

    private fun Duration.toSeconds(): String {
        require(isFinite() && inWholeSeconds >= 1) { "TTL must be at least 1 second and finite: $this" }
        return inWholeSeconds.toString()
    }

    companion object {
        private const val ENQUEUE_RESULT = 0L
        private const val ALREADY_QUEUED_RESULT = 1L
        private const val MARKED_DIRTY_RESULT = 2L
        private const val NO_QUEUED_MARK = "0"
        private const val ACQUIRED_RESULT = 1L
        private const val EXTENDED_RESULT = 1L
        private const val RELEASED_RESULT = 1L
        private const val NOT_OWNER_RESULT = -1L
        private const val DIRTY_RESULT = 1L
        private const val CLEAN_RESULT = 0L
        private const val REPLICA_ACK_COUNT = 1

        /** KEYS: lock, dirty, queued. ARGV: queuedTtl, dirtyTtl. 0 ENQUEUE, 1 ALREADY_QUEUED, 2 MARKED_DIRTY. */
        val ENQUEUE_OR_COALESCE_SCRIPT =
            """
            if redis.call("EXISTS", KEYS[1]) == 1 then
              redis.call("SET", KEYS[2], "1", "EX", ARGV[2])
              return 2
            end
            if redis.call("SET", KEYS[3], "1", "NX", "EX", ARGV[1]) then return 0 end
            return 1
            """.trimIndent()

        /** KEYS: lock, dirty, queued. ARGV: token, lockTtl, dirtyTtl. 1 ACQUIRED (queued cleared), 0 COALESCED. */
        val ACQUIRE_OR_MARK_DIRTY_SCRIPT =
            """
            if redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2]) then
              redis.call("DEL", KEYS[3])
              return 1
            end
            redis.call("SET", KEYS[2], "1", "EX", ARGV[3])
            return 0
            """.trimIndent()

        val EXTEND_LOCK_SCRIPT =
            """
            if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("EXPIRE", KEYS[1], ARGV[2]) end
            return 0
            """.trimIndent()

        /** KEYS: lock, dirty, queued. ARGV: token, queuedTtl (0: no mark). -1 NOT_OWNER, 1 DIRTY, 0 CLEAN. */
        val RELEASE_LOCK_AND_TAKE_DIRTY_SCRIPT =
            """
            if redis.call("GET", KEYS[1]) ~= ARGV[1] then return -1 end
            redis.call("DEL", KEYS[1])
            if redis.call("GETDEL", KEYS[2]) then
              if tonumber(ARGV[2]) > 0 then redis.call("SET", KEYS[3], "1", "EX", ARGV[2]) end
              return 1
            end
            return 0
            """.trimIndent()

        /** KEYS: lock, queued. ARGV: token, queuedTtl (0: no mark). Keeps the dirty flag. 1 released, 0 not owner. */
        val RELEASE_LOCK_SCRIPT =
            """
            if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
            redis.call("DEL", KEYS[1])
            if tonumber(ARGV[2]) > 0 then redis.call("SET", KEYS[2], "1", "EX", ARGV[2]) end
            return 1
            """.trimIndent()
    }
}
