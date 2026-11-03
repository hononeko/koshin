package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.EnqueueDecision
import dev.hononeko.koshin.core.ports.ReleaseResult
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import kotlinx.coroutines.CompletableDeferred
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
import java.util.concurrent.locks.LockSupport
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
@Suppress("InjectDispatcher")
class ValkeyRepoCoordinationIntegrationTest {
    companion object {
        private lateinit var client: RedisClient
        private lateinit var connection: StatefulRedisConnection<String, String>
        private lateinit var config: ValkeyConfig

        @BeforeAll
        @JvmStatic
        fun setUp() {
            config = ValkeyConfig(valkeyUrl = SharedValkeyContainer.valkeyUrl)
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

    @BeforeEach
    fun flushDb() {
        connection.sync().flushall()
    }

    @Test
    fun `connection health check returns true when connected`() = runBlocking {
        assertTrue(connection.isConnected())
        val health = ValkeyConnectionHealth(connection)
        assertTrue(health.isConnected())
        assertTrue(health.ping())
    }

    @Test
    fun `10 concurrent coroutines across separate connections yield 1 ACQUIRED, 9 COALESCED, and 1 DIRTY on release`() =
        runBlocking(Dispatchers.Default) {
            val workerConnections = List(10) { client.connect() }
            try {
                val adapters = workerConnections.map { conn ->
                    ValkeyRepoCoordinationAdapter(conn, config).also { it.preloadScripts() }
                }

                val repo = RepoRef(Platform.GITHUB, "hononeko/concurrent-repo")
                val startGate = CompletableDeferred<Unit>()

                val competitors = adapters.mapIndexed { index, adapter ->
                    val token = "worker-token-${index + 1}"
                    async {
                        startGate.await()
                        token to adapter.acquireOrMarkDirty(
                            repo = repo,
                            token = token,
                            lockTtl = 30.seconds,
                            dirtyTtl = 300.seconds,
                        )
                    }
                }

                startGate.complete(Unit)
                val outcomes = competitors.awaitAll()

                val acquired = outcomes.filter { it.second == AcquireResult.ACQUIRED }
                val coalesced = outcomes.filter { it.second == AcquireResult.COALESCED }

                assertEquals(1, acquired.size)
                assertEquals(9, coalesced.size)

                val winnerToken = acquired.single().first
                val lockKey = config.lockKeyFor(repo)
                val dirtyKey = config.dirtyKeyFor(repo)

                assertEquals(winnerToken, connection.coroutines().get(lockKey))
                assertEquals("1", connection.coroutines().get(dirtyKey))
                assertTrue((connection.coroutines().ttl(lockKey) ?: -1L) in 1L..30L)
                assertTrue((connection.coroutines().ttl(dirtyKey) ?: -1L) in 1L..300L)

                // Winner releases lock and atomically takes dirty flag
                val releaseResult = adapters.first().releaseLockAndTakeDirty(repo, winnerToken)
                assertEquals(ReleaseResult.DIRTY, releaseResult)

                // Both lock and dirty keys must now be removed
                assertNull(connection.coroutines().get(lockKey))
                assertNull(connection.coroutines().get(dirtyKey))

                // Follow-up run acquires cleanly and releases as CLEAN
                val followUpAcquire = adapters.first().acquireOrMarkDirty(
                    repo,
                    "followup-token",
                    30.seconds,
                    300.seconds,
                )
                assertEquals(AcquireResult.ACQUIRED, followUpAcquire)
                val followUpRelease = adapters.first().releaseLockAndTakeDirty(repo, "followup-token")
                assertEquals(ReleaseResult.CLEAN, followUpRelease)
            } finally {
                workerConnections.forEach { it.close() }
            }
        }

    @Test
    fun `stale lock holder cannot extend or release expired lock with or without a new holder`() = runBlocking {
        val adapter = ValkeyRepoCoordinationAdapter(connection, config)
        val repo = RepoRef(Platform.GITHUB, "hononeko/stale-holder-repo")
        val lockKey = config.lockKeyFor(repo)
        val dirtyKey = config.dirtyKeyFor(repo)

        // Token 1 acquires lock and another delivery coalesces to set dirty flag
        assertEquals(AcquireResult.ACQUIRED, adapter.acquireOrMarkDirty(repo, "token-1", 10.seconds, 300.seconds))
        assertEquals(AcquireResult.COALESCED, adapter.acquireOrMarkDirty(repo, "token-c1", 10.seconds, 300.seconds))

        // Fast-expire the lock on the server (100ms) while dirty flag remains
        connection.coroutines().pexpire(lockKey, 100L)
        delay(150.milliseconds)

        // Case A: Lock expired and NO new holder acquired yet — stale release returns NOT_OWNER and keeps dirty flag
        assertFalse(adapter.extendLock(repo, "token-1", 30.seconds))
        assertFalse(adapter.releaseLock(repo, "token-1"))
        assertEquals(ReleaseResult.NOT_OWNER, adapter.releaseLockAndTakeDirty(repo, "token-1"))
        assertEquals("1", connection.coroutines().get(dirtyKey))

        // Case B: Token 2 acquires the lock after expiry
        assertEquals(AcquireResult.ACQUIRED, adapter.acquireOrMarkDirty(repo, "token-2", 10.seconds, 300.seconds))

        // Stale holder (token-1) tries to extend, releaseLock, or releaseLockAndTakeDirty -> all rejected
        assertFalse(adapter.extendLock(repo, "token-1", 30.seconds))
        assertFalse(adapter.releaseLock(repo, "token-1"))
        assertEquals(ReleaseResult.NOT_OWNER, adapter.releaseLockAndTakeDirty(repo, "token-1"))
        assertEquals("token-2", connection.coroutines().get(lockKey))
        assertEquals("1", connection.coroutines().get(dirtyKey))

        // Active holder (token-2) extends lock to 60s and verifies TTL actually increased > 10s
        assertTrue(adapter.extendLock(repo, "token-2", 60.seconds))
        assertTrue((connection.coroutines().ttl(lockKey) ?: 0L) > 10L)

        // Active holder (token-2) releases and observes the preserved dirty flag
        assertEquals(ReleaseResult.DIRTY, adapter.releaseLockAndTakeDirty(repo, "token-2"))
    }

    @Test
    fun `interleaved releaseLockAndTakeDirty and acquireOrMarkDirty across connections never orphan dirty flag`() =
        runBlocking(Dispatchers.Default) {
            val holderConnection = client.connect()
            val contenderConnection = client.connect()
            try {
                val holderAdapter = ValkeyRepoCoordinationAdapter(holderConnection, config).also {
                    it.preloadScripts()
                }
                val contenderAdapter = ValkeyRepoCoordinationAdapter(contenderConnection, config).also {
                    it.preloadScripts()
                }
                val repo = RepoRef(Platform.GITLAB, "hononeko/race-repo")
                var coalescedObservations = 0
                var acquiredObservations = 0

                repeat(100) { iteration ->
                    val acquireOutcome = runRaceIteration(holderAdapter, contenderAdapter, repo, iteration)
                    when (acquireOutcome) {
                        AcquireResult.COALESCED -> coalescedObservations++
                        AcquireResult.ACQUIRED -> acquiredObservations++
                    }
                }

                assertTrue(coalescedObservations > 0, "Expected at least one COALESCED race outcome")
                assertTrue(acquiredObservations > 0, "Expected at least one ACQUIRED race outcome")
            } finally {
                holderConnection.close()
                contenderConnection.close()
            }
        }

    private suspend fun runRaceIteration(
        holderAdapter: ValkeyRepoCoordinationAdapter,
        contenderAdapter: ValkeyRepoCoordinationAdapter,
        repo: RepoRef,
        iteration: Int,
    ): AcquireResult = kotlinx.coroutines.coroutineScope {
        val initialToken = "holder-$iteration"
        val contenderToken = "contender-$iteration"

        assertEquals(
            AcquireResult.ACQUIRED,
            holderAdapter.acquireOrMarkDirty(repo, initialToken, 30.seconds, 300.seconds),
        )

        val gate = CompletableDeferred<Unit>()
        val releaseDeferred = async {
            gate.await()
            if (iteration % 3 == 0) {
                LockSupport.parkNanos(250_000L)
            }
            holderAdapter.releaseLockAndTakeDirty(repo, initialToken)
        }
        val acquireDeferred = async {
            gate.await()
            if (iteration % 3 == 1) {
                LockSupport.parkNanos(250_000L)
            }
            contenderAdapter.acquireOrMarkDirty(repo, contenderToken, 30.seconds, 300.seconds)
        }

        gate.complete(Unit)
        val releaseOutcome = releaseDeferred.await()
        val acquireOutcome = acquireDeferred.await()

        when (acquireOutcome) {
            AcquireResult.COALESCED -> {
                assertEquals(ReleaseResult.DIRTY, releaseOutcome)
                assertNull(connection.coroutines().get(config.dirtyKeyFor(repo)))
                assertNull(connection.coroutines().get(config.lockKeyFor(repo)))
            }

            AcquireResult.ACQUIRED -> {
                assertEquals(ReleaseResult.CLEAN, releaseOutcome)
                assertEquals(contenderToken, connection.coroutines().get(config.lockKeyFor(repo)))
                assertNull(connection.coroutines().get(config.dirtyKeyFor(repo)))
                assertEquals(
                    ReleaseResult.CLEAN,
                    contenderAdapter.releaseLockAndTakeDirty(repo, contenderToken),
                )
            }
        }
        acquireOutcome
    }

    @Test
    fun `releaseLock removes lock for owner while keeping dirty flag intact for next holder`() = runBlocking {
        val adapter = ValkeyRepoCoordinationAdapter(connection, config)
        val repo = RepoRef(Platform.GITHUB, "hononeko/requeue-repo")

        assertEquals(AcquireResult.ACQUIRED, adapter.acquireOrMarkDirty(repo, "token-1", 30.seconds, 300.seconds))
        assertEquals(AcquireResult.COALESCED, adapter.acquireOrMarkDirty(repo, "token-2", 30.seconds, 300.seconds))
        assertEquals("1", connection.coroutines().get(config.dirtyKeyFor(repo)))

        // SIGTERM / rate-limit requeue path calls releaseLock (keeps dirty flag)
        assertTrue(adapter.releaseLock(repo, "token-1"))
        assertNull(connection.coroutines().get(config.lockKeyFor(repo)))
        assertEquals("1", connection.coroutines().get(config.dirtyKeyFor(repo)))

        // Next worker acquires the lock and still observes the preserved dirty flag on completion
        assertEquals(AcquireResult.ACQUIRED, adapter.acquireOrMarkDirty(repo, "token-3", 30.seconds, 300.seconds))
        assertEquals(ReleaseResult.DIRTY, adapter.releaseLockAndTakeDirty(repo, "token-3"))
    }

    @Test
    fun `EVALSHA falls back to EVAL after SCRIPT FLUSH, reloads script cache, and executes WAIT`() = runBlocking {
        val waitConfig = ValkeyConfig(
            valkeyUrl = SharedValkeyContainer.valkeyUrl,
            lockReplicaAckTimeoutMs = 20L,
        )
        val adapter = ValkeyRepoCoordinationAdapter(connection, waitConfig)
        adapter.preloadScripts()

        val repo = RepoRef(Platform.GITHUB, "hononeko/noscript-repo")
        val acquireDigest = checkNotNull(
            connection.coroutines().digest(
                ValkeyRepoCoordinationAdapter.ACQUIRE_OR_MARK_DIRTY_SCRIPT,
            ),
        )

        // Flush scripts on Valkey server after preloadScripts() cached SHA1 digests
        connection.coroutines().scriptFlush()
        assertEquals(listOf(false), connection.coroutines().scriptExists(acquireDigest))

        // First call after flush recovers from NOSCRIPT via EVAL and reloads script into cache
        assertEquals(
            AcquireResult.ACQUIRED,
            adapter.acquireOrMarkDirty(repo, "token-1", 30.seconds, 300.seconds),
        )
        assertEquals(listOf(true), connection.coroutines().scriptExists(acquireDigest))

        connection.coroutines().scriptFlush()
        assertTrue(adapter.extendLock(repo, "token-1", 45.seconds))

        connection.coroutines().scriptFlush()
        assertEquals(ReleaseResult.CLEAN, adapter.releaseLockAndTakeDirty(repo, "token-1"))

        assertEquals(
            AcquireResult.ACQUIRED,
            adapter.acquireOrMarkDirty(repo, "token-2", 30.seconds, 300.seconds),
        )
        connection.coroutines().scriptFlush()
        assertTrue(adapter.releaseLock(repo, "token-2"))
    }

    private val commands get() = connection.coroutines()

    private suspend fun queued(repo: RepoRef) = commands.get(config.queuedKeyFor(repo))

    @Test
    fun `10 concurrent enqueueOrCoalesce calls for an idle repo enqueue it once`() = runBlocking(Dispatchers.IO) {
        val connections = List(10) { client.connect() }
        try {
            val adapters = connections.map { ValkeyRepoCoordinationAdapter(it, config) }
            adapters.forEach { it.preloadScripts() }
            val repo = RepoRef(Platform.GITHUB, "hononeko/enqueue-repo")
            val startGate = CompletableDeferred<Unit>()

            val decisions = adapters.map { adapter ->
                async {
                    startGate.await()
                    adapter.enqueueOrCoalesce(repo, queuedTtl = 600.seconds, dirtyTtl = 300.seconds)
                }
            }.also { startGate.complete(Unit) }.awaitAll()

            assertEquals(1, decisions.count { it == EnqueueDecision.ENQUEUE })
            assertEquals(9, decisions.count { it == EnqueueDecision.ALREADY_QUEUED })
            assertEquals("1", queued(repo))
            assertTrue((commands.ttl(config.queuedKeyFor(repo)) ?: -1L) in 1L..600L)
            assertNull(commands.get(config.dirtyKeyFor(repo)), "queuing marks nothing dirty")
        } finally {
            connections.forEach { it.close() }
        }
    }

    @Test
    fun `enqueueOrCoalesce marks a running repo dirty without queuing it`() = runBlocking {
        val adapter = ValkeyRepoCoordinationAdapter(connection, config)
        val repo = RepoRef(Platform.GITLAB, "group/sub/running-repo")
        assertEquals(AcquireResult.ACQUIRED, adapter.acquireOrMarkDirty(repo, "token-1", 30.seconds, 300.seconds))

        assertEquals(EnqueueDecision.MARKED_DIRTY, adapter.enqueueOrCoalesce(repo, 600.seconds, 300.seconds))

        assertEquals("1", commands.get(config.dirtyKeyFor(repo)))
        assertTrue((commands.ttl(config.dirtyKeyFor(repo)) ?: -1L) in 1L..300L)
        assertNull(queued(repo))
        assertEquals(ReleaseResult.DIRTY, adapter.releaseLockAndTakeDirty(repo, "token-1", 600.seconds))
    }

    @Test
    fun `acquiring the lock clears the queued mark, and coalescing keeps it`() = runBlocking {
        val adapter = ValkeyRepoCoordinationAdapter(connection, config)
        val repo = RepoRef(Platform.GITHUB, "hononeko/acquire-clears-queued")
        assertEquals(EnqueueDecision.ENQUEUE, adapter.enqueueOrCoalesce(repo, 600.seconds, 300.seconds))

        assertEquals(AcquireResult.ACQUIRED, adapter.acquireOrMarkDirty(repo, "token-1", 30.seconds, 300.seconds))
        assertNull(queued(repo))

        // A fail-open duplicate published while the repo ran
        commands.set(config.queuedKeyFor(repo), "1")
        assertEquals(AcquireResult.COALESCED, adapter.acquireOrMarkDirty(repo, "token-2", 30.seconds, 300.seconds))
        assertEquals("1", queued(repo))
        assertEquals("1", commands.get(config.dirtyKeyFor(repo)))
    }

    @Test
    fun `a dirty release re-arms the queued mark for the follow-up, a clean or plain one doesn't`() = runBlocking {
        val adapter = ValkeyRepoCoordinationAdapter(connection, config)
        val repo = RepoRef(Platform.GITHUB, "hononeko/follow-up-repo")

        adapter.acquireOrMarkDirty(repo, "token-1", 30.seconds, 300.seconds)
        adapter.enqueueOrCoalesce(repo, 600.seconds, 300.seconds)
        assertEquals(ReleaseResult.DIRTY, adapter.releaseLockAndTakeDirty(repo, "token-1", 600.seconds))
        assertEquals("1", queued(repo))
        assertTrue((commands.ttl(config.queuedKeyFor(repo)) ?: -1L) in 1L..600L)

        adapter.acquireOrMarkDirty(repo, "token-2", 30.seconds, 300.seconds)
        assertEquals(ReleaseResult.CLEAN, adapter.releaseLockAndTakeDirty(repo, "token-2", 600.seconds))
        assertNull(queued(repo))

        // A rejected repo drops its dirty flag without a follow-up
        adapter.acquireOrMarkDirty(repo, "token-3", 30.seconds, 300.seconds)
        adapter.enqueueOrCoalesce(repo, 600.seconds, 300.seconds)
        assertEquals(ReleaseResult.DIRTY, adapter.releaseLockAndTakeDirty(repo, "token-3"))
        assertNull(queued(repo))

        assertEquals(ReleaseResult.NOT_OWNER, adapter.releaseLockAndTakeDirty(repo, "token-4", 600.seconds))
        assertNull(queued(repo))
    }

    @Test
    fun `releaseLock re-arms the queued mark only when asked and only for the owner`() = runBlocking {
        val adapter = ValkeyRepoCoordinationAdapter(connection, config)
        val repo = RepoRef(Platform.GITHUB, "hononeko/requeue-repo")

        adapter.acquireOrMarkDirty(repo, "token-1", 30.seconds, 300.seconds)
        assertTrue(adapter.releaseLock(repo, "token-1", 600.seconds))
        assertEquals("1", queued(repo))

        adapter.acquireOrMarkDirty(repo, "token-2", 30.seconds, 300.seconds)
        assertFalse(adapter.releaseLock(repo, "someone-else", 600.seconds))
        assertNull(queued(repo))
        assertTrue(adapter.releaseLock(repo, "token-2"))
        assertNull(queued(repo))
    }

    @Test
    fun `clearQueued lets the next webhook enqueue again`() = runBlocking {
        val adapter = ValkeyRepoCoordinationAdapter(connection, config)
        val repo = RepoRef(Platform.GITHUB, "hononeko/dead-lettered-repo")
        assertEquals(EnqueueDecision.ENQUEUE, adapter.enqueueOrCoalesce(repo, 600.seconds, 300.seconds))
        assertEquals(EnqueueDecision.ALREADY_QUEUED, adapter.enqueueOrCoalesce(repo, 600.seconds, 300.seconds))

        adapter.clearQueued(repo)

        assertEquals(EnqueueDecision.ENQUEUE, adapter.enqueueOrCoalesce(repo, 600.seconds, 300.seconds))
    }
}
