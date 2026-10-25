package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.ReleaseResult
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import kotlinx.coroutines.CompletableDeferred
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
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
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
    fun `10 concurrent coroutines competing for the same repo yield 1 ACQUIRED, 9 COALESCED, and 1 DIRTY on release`() =
        runBlocking {
            val adapter = ValkeyRepoCoordinationAdapter(connection, config)
            adapter.preloadScripts()

            val repo = RepoRef(Platform.GITHUB, "hononeko/concurrent-repo")
            val startGate = CompletableDeferred<Unit>()

            val competitors = (1..10).map { index ->
                val token = "worker-token-$index"
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
            assertEquals(winnerToken, connection.coroutines().get(config.lockKeyFor(repo)))
            assertEquals("1", connection.coroutines().get(config.dirtyKeyFor(repo)))

            // Winner releases lock and atomically takes dirty flag
            val releaseResult = adapter.releaseLockAndTakeDirty(repo, winnerToken)
            assertEquals(ReleaseResult.DIRTY, releaseResult)

            // Both lock and dirty keys must now be removed
            assertNull(connection.coroutines().get(config.lockKeyFor(repo)))
            assertNull(connection.coroutines().get(config.dirtyKeyFor(repo)))

            // Follow-up run acquires cleanly and releases as CLEAN
            val followUpAcquire = adapter.acquireOrMarkDirty(repo, "followup-token", 30.seconds, 300.seconds)
            assertEquals(AcquireResult.ACQUIRED, followUpAcquire)
            val followUpRelease = adapter.releaseLockAndTakeDirty(repo, "followup-token")
            assertEquals(ReleaseResult.CLEAN, followUpRelease)
        }

    @Test
    fun `stale lock holder cannot extend or release lock acquired by a new holder after TTL expiry`() = runBlocking {
        val adapter = ValkeyRepoCoordinationAdapter(connection, config)
        val repo = RepoRef(Platform.GITHUB, "hononeko/stale-holder-repo")

        // Token 1 acquires lock with a short 1s TTL
        val firstAcquire = adapter.acquireOrMarkDirty(repo, "token-1", 1.seconds, 300.seconds)
        assertEquals(AcquireResult.ACQUIRED, firstAcquire)

        // Wait for lock TTL to expire
        delay(1200.milliseconds)

        // Token 2 acquires the lock after expiry
        val secondAcquire = adapter.acquireOrMarkDirty(repo, "token-2", 30.seconds, 300.seconds)
        assertEquals(AcquireResult.ACQUIRED, secondAcquire)

        // Another webhook arrives and coalesces (marks dirty)
        val coalesced = adapter.acquireOrMarkDirty(repo, "token-3", 30.seconds, 300.seconds)
        assertEquals(AcquireResult.COALESCED, coalesced)

        // Stale holder (token-1) tries to extend lock -> false
        assertFalse(adapter.extendLock(repo, "token-1", 30.seconds))

        // Stale holder (token-1) tries to releaseLock -> false
        assertFalse(adapter.releaseLock(repo, "token-1"))

        // Stale holder (token-1) tries to releaseLockAndTakeDirty -> NOT_OWNER, leaving new lock and dirty flag intact
        assertEquals(ReleaseResult.NOT_OWNER, adapter.releaseLockAndTakeDirty(repo, "token-1"))
        assertEquals("token-2", connection.coroutines().get(config.lockKeyFor(repo)))
        assertEquals("1", connection.coroutines().get(config.dirtyKeyFor(repo)))

        // Active holder (token-2) can still extend and release normally, observing the dirty flag
        assertTrue(adapter.extendLock(repo, "token-2", 45.seconds))
        assertEquals(ReleaseResult.DIRTY, adapter.releaseLockAndTakeDirty(repo, "token-2"))
    }

    @Test
    fun `interleaved releaseLockAndTakeDirty and acquireOrMarkDirty never leave an orphaned dirty flag`() =
        runBlocking {
            val adapter = ValkeyRepoCoordinationAdapter(connection, config)
            adapter.preloadScripts()
            val repo = RepoRef(Platform.GITLAB, "hononeko/race-repo")

            repeat(50) { iteration ->
                val initialToken = "holder-$iteration"
                val contenderToken = "contender-$iteration"

                assertEquals(
                    AcquireResult.ACQUIRED,
                    adapter.acquireOrMarkDirty(repo, initialToken, 30.seconds, 300.seconds),
                )

                val gate = CompletableDeferred<Unit>()
                val releaseDeferred = async {
                    gate.await()
                    adapter.releaseLockAndTakeDirty(repo, initialToken)
                }
                val acquireDeferred = async {
                    gate.await()
                    adapter.acquireOrMarkDirty(repo, contenderToken, 30.seconds, 300.seconds)
                }

                gate.complete(Unit)
                val releaseOutcome = releaseDeferred.await()
                val acquireOutcome = acquireDeferred.await()

                when (acquireOutcome) {
                    AcquireResult.COALESCED -> {
                        // Contender ran BEFORE holder released: holder MUST see DIRTY
                        assertEquals(ReleaseResult.DIRTY, releaseOutcome)
                        assertNull(connection.coroutines().get(config.dirtyKeyFor(repo)))
                        assertNull(connection.coroutines().get(config.lockKeyFor(repo)))
                    }

                    AcquireResult.ACQUIRED -> {
                        // Contender ran AFTER holder released: holder saw CLEAN, and contender now holds the lock
                        assertEquals(ReleaseResult.CLEAN, releaseOutcome)
                        assertEquals(contenderToken, connection.coroutines().get(config.lockKeyFor(repo)))
                        assertNull(connection.coroutines().get(config.dirtyKeyFor(repo)))
                        assertEquals(ReleaseResult.CLEAN, adapter.releaseLockAndTakeDirty(repo, contenderToken))
                    }
                }
            }
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
    fun `EVALSHA falls back to EVAL after SCRIPT FLUSH and WAIT executes when lockReplicaAckTimeoutMs is set`() =
        runBlocking {
            val waitConfig = ValkeyConfig(
                valkeyUrl = SharedValkeyContainer.valkeyUrl,
                lockReplicaAckTimeoutMs = 20L,
            )
            val adapter = ValkeyRepoCoordinationAdapter(connection, waitConfig)
            adapter.preloadScripts()

            val repo = RepoRef(Platform.GITHUB, "hononeko/noscript-repo")

            // Flush scripts on Valkey server after preloadScripts() cached SHA1 digests
            connection.coroutines().scriptFlush()

            // All 4 operations must transparently recover from NOSCRIPT via EVAL
            assertEquals(
                AcquireResult.ACQUIRED,
                adapter.acquireOrMarkDirty(repo, "token-1", 30.seconds, 300.seconds),
            )

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
}
