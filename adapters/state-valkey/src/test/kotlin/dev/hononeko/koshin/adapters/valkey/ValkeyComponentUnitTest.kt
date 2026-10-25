package dev.hononeko.koshin.adapters.valkey

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.ReleaseResult
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisCommandExecutionException
import io.lettuce.core.RedisNoScriptException
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyComponentUnitTest {
    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val lockKey = "koshin:{github:hononeko/koshin}:lock"
    private val dirtyKey = "koshin:{github:hononeko/koshin}:dirty"

    @Test
    fun `connection health reports status, ping result, and enforces pingTimeout`() = runTest {
        val mockConnection = mockk<StatefulRedisConnection<String, String>>()
        val mockCommands = mockk<RedisCoroutinesCommands<String, String>>()
        val health = ValkeyConnectionHealth(mockConnection, mockCommands, pingTimeout = 100.milliseconds)

        every { mockConnection.isOpen } returns false
        assertFalse(health.isConnected())
        assertFalse(health.ping())

        every { mockConnection.isOpen } returns true
        coEvery { mockCommands.ping() } returns "PONG"
        assertTrue(health.isConnected())
        assertTrue(health.ping())

        coEvery { mockCommands.ping() } returns "LOADING"
        assertFalse(health.ping())

        coEvery { mockCommands.ping() } throws RedisCommandExecutionException("ERR broken")
        assertFalse(health.ping())

        coEvery { mockCommands.ping() } coAnswers {
            delay(500.milliseconds)
            "PONG"
        }
        assertFalse(health.ping())

        assertThrows<IllegalArgumentException> {
            ValkeyConnectionHealth(mockConnection, mockCommands, pingTimeout = 0.seconds)
        }
        assertThrows<IllegalArgumentException> {
            ValkeyConnectionHealth(mockConnection, mockCommands, pingTimeout = Duration.INFINITE)
        }
    }

    @Test
    fun `connection health rethrows CancellationException on ping`() = runTest {
        val mockConnection = mockk<StatefulRedisConnection<String, String>>()
        val mockCommands = mockk<RedisCoroutinesCommands<String, String>>()
        val health = ValkeyConnectionHealth(mockConnection, mockCommands)

        every { mockConnection.isOpen } returns true
        coEvery { mockCommands.ping() } throws CancellationException("cancelled")

        assertThrows<CancellationException> {
            health.ping()
        }
    }

    @Test
    fun `secondary constructors with StatefulRedisConnection initialize properly`() {
        val mockConnection = mockk<StatefulRedisConnection<String, String>>(relaxed = true)
        every { mockConnection.isOpen } returns true
        assertTrue(mockConnection.isConnected())

        val health = ValkeyConnectionHealth(mockConnection)
        assertNotNull(health)

        val adapter = ValkeyRepoCoordinationAdapter(mockConnection)
        assertNotNull(adapter)
    }

    @Test
    fun `preloadScripts loads all four coordination scripts and uses EVALSHA`() = runTest {
        val mockCommands = mockk<RedisCoroutinesCommands<String, String>>()
        coEvery {
            mockCommands.scriptLoad(ValkeyRepoCoordinationAdapter.ACQUIRE_OR_MARK_DIRTY_SCRIPT)
        } returns "sha-acquire"
        coEvery {
            mockCommands.scriptLoad(ValkeyRepoCoordinationAdapter.EXTEND_LOCK_SCRIPT)
        } returns "sha-extend"
        coEvery {
            mockCommands.scriptLoad(ValkeyRepoCoordinationAdapter.RELEASE_LOCK_AND_TAKE_DIRTY_SCRIPT)
        } returns "sha-release-dirty"
        coEvery {
            mockCommands.scriptLoad(ValkeyRepoCoordinationAdapter.RELEASE_LOCK_SCRIPT)
        } returns "sha-release"

        val adapter = ValkeyRepoCoordinationAdapter(mockCommands)
        adapter.preloadScripts()

        coVerify(exactly = 1) {
            mockCommands.scriptLoad(ValkeyRepoCoordinationAdapter.ACQUIRE_OR_MARK_DIRTY_SCRIPT)
            mockCommands.scriptLoad(ValkeyRepoCoordinationAdapter.EXTEND_LOCK_SCRIPT)
            mockCommands.scriptLoad(ValkeyRepoCoordinationAdapter.RELEASE_LOCK_AND_TAKE_DIRTY_SCRIPT)
            mockCommands.scriptLoad(ValkeyRepoCoordinationAdapter.RELEASE_LOCK_SCRIPT)
        }

        coEvery {
            mockCommands.evalsha<Long>(
                "sha-acquire",
                ScriptOutputType.INTEGER,
                arrayOf(lockKey, dirtyKey),
                "tok-1",
                "45",
                "3600",
            )
        } returns 1L

        val res = adapter.acquireOrMarkDirty(repo, "tok-1", 45.seconds, 3600.seconds)
        assertEquals(AcquireResult.ACQUIRED, res)
    }

    @Test
    fun `acquireOrMarkDirty handles WAIT success, insufficient replicas, errors, and cancellation`() = runTest {
        val mockCommands = mockk<RedisCoroutinesCommands<String, String>>()
        val config = ValkeyConfig(lockReplicaAckTimeoutMs = 150L)
        val adapter = ValkeyRepoCoordinationAdapter(mockCommands, config)

        coEvery { mockCommands.scriptLoad(any<String>()) } returns "sha-acquire"
        coEvery {
            mockCommands.evalsha<Long>(
                "sha-acquire",
                ScriptOutputType.INTEGER,
                arrayOf(lockKey, dirtyKey),
                any(),
                "45",
                "3600",
            )
        } returns 1L

        // 1. Replica acknowledged (1 >= 1)
        coEvery { mockCommands.waitForReplication(1, 150L) } returns 1L
        assertEquals(AcquireResult.ACQUIRED, adapter.acquireOrMarkDirty(repo, "tok-1", 45.seconds, 3600.seconds))

        // 2. Zero replicas acknowledged (0 < 1) -> logs warning, still returns ACQUIRED
        coEvery { mockCommands.waitForReplication(1, 150L) } returns 0L
        assertEquals(AcquireResult.ACQUIRED, adapter.acquireOrMarkDirty(repo, "tok-2", 45.seconds, 3600.seconds))

        // 3. Null replica count -> logs warning, still returns ACQUIRED
        coEvery { mockCommands.waitForReplication(1, 150L) } returns null
        assertEquals(AcquireResult.ACQUIRED, adapter.acquireOrMarkDirty(repo, "tok-3", 45.seconds, 3600.seconds))

        // 4. WAIT command throws exception -> logs warning, still returns ACQUIRED
        coEvery {
            mockCommands.waitForReplication(1, 150L)
        } throws RedisCommandExecutionException("ERR WAIT failed")
        assertEquals(AcquireResult.ACQUIRED, adapter.acquireOrMarkDirty(repo, "tok-4", 45.seconds, 3600.seconds))

        // 5. WAIT command throws CancellationException -> rethrown
        coEvery {
            mockCommands.waitForReplication(1, 150L)
        } throws CancellationException("cancelled")
        assertThrows<CancellationException> {
            adapter.acquireOrMarkDirty(repo, "tok-5", 45.seconds, 3600.seconds)
        }

        // 6. Coalesced outcome does not call WAIT
        coEvery {
            mockCommands.evalsha<Long>(
                "sha-acquire",
                ScriptOutputType.INTEGER,
                arrayOf(lockKey, dirtyKey),
                "tok-6",
                "45",
                "3600",
            )
        } returns 0L
        assertEquals(AcquireResult.COALESCED, adapter.acquireOrMarkDirty(repo, "tok-6", 45.seconds, 3600.seconds))
        coVerify(exactly = 5) { mockCommands.waitForReplication(1, 150L) }
    }

    @Test
    fun `extendLock, releaseLockAndTakeDirty, and releaseLock map script results accurately`() = runTest {
        val mockCommands = mockk<RedisCoroutinesCommands<String, String>>()
        val adapter = ValkeyRepoCoordinationAdapter(mockCommands)

        coEvery { mockCommands.scriptLoad(any<String>()) } returns "sha-any"

        coEvery {
            mockCommands.evalsha<Long>("sha-any", ScriptOutputType.INTEGER, arrayOf(lockKey), "tok-1", "45")
        } returns 1L
        assertTrue(adapter.extendLock(repo, "tok-1", 45.seconds))

        coEvery {
            mockCommands.evalsha<Long>("sha-any", ScriptOutputType.INTEGER, arrayOf(lockKey), "tok-1", "45")
        } returns 0L
        assertFalse(adapter.extendLock(repo, "tok-1", 45.seconds))

        coEvery {
            mockCommands.evalsha<Long>("sha-any", ScriptOutputType.INTEGER, arrayOf(lockKey, dirtyKey), "tok-1")
        } returns 0L
        assertEquals(ReleaseResult.CLEAN, adapter.releaseLockAndTakeDirty(repo, "tok-1"))

        coEvery {
            mockCommands.evalsha<Long>("sha-any", ScriptOutputType.INTEGER, arrayOf(lockKey, dirtyKey), "tok-1")
        } returns 1L
        assertEquals(ReleaseResult.DIRTY, adapter.releaseLockAndTakeDirty(repo, "tok-1"))

        coEvery {
            mockCommands.evalsha<Long>("sha-any", ScriptOutputType.INTEGER, arrayOf(lockKey, dirtyKey), "tok-1")
        } returns -1L
        assertEquals(ReleaseResult.NOT_OWNER, adapter.releaseLockAndTakeDirty(repo, "tok-1"))

        coEvery {
            mockCommands.evalsha<Long>("sha-any", ScriptOutputType.INTEGER, arrayOf(lockKey, dirtyKey), "tok-1")
        } returns 99L
        assertThrows<IllegalStateException> {
            adapter.releaseLockAndTakeDirty(repo, "tok-1")
        }

        coEvery {
            mockCommands.evalsha<Long>("sha-any", ScriptOutputType.INTEGER, arrayOf(lockKey), "tok-1")
        } returns 1L
        assertTrue(adapter.releaseLock(repo, "tok-1"))

        coEvery {
            mockCommands.evalsha<Long>("sha-any", ScriptOutputType.INTEGER, arrayOf(lockKey), "tok-1")
        } returns 0L
        assertFalse(adapter.releaseLock(repo, "tok-1"))
    }

    @Test
    fun `releaseLockAndTakeDirty and releaseLock complete non-cancellably when caller job is cancelled`() = runTest {
        val mockCommands = mockk<RedisCoroutinesCommands<String, String>>()
        val adapter = ValkeyRepoCoordinationAdapter(mockCommands)
        coEvery { mockCommands.scriptLoad(any<String>()) } returns "sha-release"

        val enteredEval = CompletableDeferred<Unit>()
        val allowFinish = CompletableDeferred<Unit>()
        var observedDirtyResult: ReleaseResult? = null

        coEvery {
            mockCommands.evalsha<Long>("sha-release", ScriptOutputType.INTEGER, arrayOf(lockKey, dirtyKey), "tok-1")
        } coAnswers {
            enteredEval.complete(Unit)
            allowFinish.await()
            1L
        }

        val job1 = launch {
            observedDirtyResult = adapter.releaseLockAndTakeDirty(repo, "tok-1")
        }
        enteredEval.await()
        job1.cancel()
        allowFinish.complete(Unit)
        job1.join()
        assertEquals(ReleaseResult.DIRTY, observedDirtyResult)

        val enteredRelease = CompletableDeferred<Unit>()
        val allowReleaseFinish = CompletableDeferred<Unit>()
        var observedReleaseResult: Boolean? = null

        coEvery {
            mockCommands.evalsha<Long>("sha-release", ScriptOutputType.INTEGER, arrayOf(lockKey), "tok-1")
        } coAnswers {
            enteredRelease.complete(Unit)
            allowReleaseFinish.await()
            1L
        }

        val job2 = launch {
            observedReleaseResult = adapter.releaseLock(repo, "tok-1")
        }
        enteredRelease.await()
        job2.cancel()
        allowReleaseFinish.complete(Unit)
        job2.join()
        assertEquals(true, observedReleaseResult)
    }

    @Test
    fun `evalsha falls back to eval on NOSCRIPT, reloads sha, and never falls through on null result`() = runTest {
        val mockCommands = mockk<RedisCoroutinesCommands<String, String>>()
        val adapter = ValkeyRepoCoordinationAdapter(mockCommands)

        coEvery { mockCommands.scriptLoad(any<String>()) } returns "sha-extend"
        coEvery {
            mockCommands.evalsha<Long>("sha-extend", ScriptOutputType.INTEGER, arrayOf(lockKey), "tok-1", "45")
        } throws RedisNoScriptException("NOSCRIPT No matching script. Please use EVAL.")
        coEvery {
            mockCommands.eval<Long>(
                ValkeyRepoCoordinationAdapter.EXTEND_LOCK_SCRIPT,
                ScriptOutputType.INTEGER,
                arrayOf(lockKey),
                "tok-1",
                "45",
            )
        } returns 1L

        assertTrue(adapter.extendLock(repo, "tok-1", 45.seconds))
        coVerify(exactly = 2) { mockCommands.scriptLoad(ValkeyRepoCoordinationAdapter.EXTEND_LOCK_SCRIPT) }

        // Generic RedisCommandExecutionException containing NOSCRIPT also falls back
        coEvery {
            mockCommands.evalsha<Long>("sha-extend", ScriptOutputType.INTEGER, arrayOf(lockKey), "tok-1", "45")
        } throws RedisCommandExecutionException("ERR NOSCRIPT flushed")

        assertTrue(adapter.extendLock(repo, "tok-1", 45.seconds))

        // Null EVALSHA result throws IllegalStateException immediately without calling EVAL
        coEvery {
            mockCommands.evalsha<Long>("sha-extend", ScriptOutputType.INTEGER, arrayOf(lockKey), "tok-1", "45")
        } returns null
        assertThrows<IllegalStateException> {
            adapter.extendLock(repo, "tok-1", 45.seconds)
        }
        // EVAL count must still be 2 (not 3)
        coVerify(exactly = 2) {
            mockCommands.eval<Long>(
                ValkeyRepoCoordinationAdapter.EXTEND_LOCK_SCRIPT,
                ScriptOutputType.INTEGER,
                arrayOf(lockKey),
                "tok-1",
                "45",
            )
        }

        // Non-NOSCRIPT RedisCommandExecutionException is rethrown
        coEvery {
            mockCommands.evalsha<Long>("sha-extend", ScriptOutputType.INTEGER, arrayOf(lockKey), "tok-1", "45")
        } throws RedisCommandExecutionException("OOM command not allowed when used memory > 'maxmemory'.")

        assertThrows<RedisCommandExecutionException> {
            adapter.extendLock(repo, "tok-1", 45.seconds)
        }
    }

    @Test
    fun `falls back to eval when scriptLoad returns null`() = runTest {
        val mockCommands = mockk<RedisCoroutinesCommands<String, String>>()
        val adapter = ValkeyRepoCoordinationAdapter(mockCommands)

        coEvery { mockCommands.scriptLoad(any<String>()) } returns null
        coEvery {
            mockCommands.eval<Long>(
                ValkeyRepoCoordinationAdapter.RELEASE_LOCK_SCRIPT,
                ScriptOutputType.INTEGER,
                arrayOf(lockKey),
                "tok-1",
            )
        } returns 1L

        assertTrue(adapter.releaseLock(repo, "tok-1"))
    }

    @Test
    fun `validates tokens and TTL durations`() = runTest {
        val mockCommands = mockk<RedisCoroutinesCommands<String, String>>(relaxed = true)
        val adapter = ValkeyRepoCoordinationAdapter(mockCommands)

        assertThrows<IllegalArgumentException> {
            adapter.acquireOrMarkDirty(repo, " ", 45.seconds, 3600.seconds)
        }
        assertThrows<IllegalArgumentException> {
            adapter.acquireOrMarkDirty(repo, "tok", 500.milliseconds, 3600.seconds)
        }
        assertThrows<IllegalArgumentException> {
            adapter.acquireOrMarkDirty(repo, "tok", Duration.INFINITE, 3600.seconds)
        }
        assertThrows<IllegalArgumentException> {
            adapter.acquireOrMarkDirty(repo, "tok", 45.seconds, 500.milliseconds)
        }
        assertThrows<IllegalArgumentException> {
            adapter.acquireOrMarkDirty(repo, "tok", 45.seconds, Duration.INFINITE)
        }
        assertThrows<IllegalArgumentException> {
            adapter.extendLock(repo, " ", 45.seconds)
        }
        assertThrows<IllegalArgumentException> {
            adapter.extendLock(repo, "tok", 500.milliseconds)
        }
        assertThrows<IllegalArgumentException> {
            adapter.extendLock(repo, "tok", Duration.INFINITE)
        }
        assertThrows<IllegalArgumentException> {
            adapter.releaseLockAndTakeDirty(repo, " ")
        }
        assertThrows<IllegalArgumentException> {
            adapter.releaseLock(repo, " ")
        }
    }
}
