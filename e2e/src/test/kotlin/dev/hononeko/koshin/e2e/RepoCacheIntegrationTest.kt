package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.readText
import kotlin.io.path.writeText

/** Renovate's repository cache (KSH-10) following a repo across workers through the NATS Object Store. */
class RepoCacheIntegrationTest {
    private val cluster = E2eCluster()
    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")

    /** The cache each run found when it started (`null` = cold). */
    private val seen = CopyOnWriteArrayList<String>()

    @AfterEach
    fun tearDown() {
        cluster.close()
    }

    /** Plays Renovate: records the cache it finds, writes [next], and ends with [result]. */
    private fun TestWorker.renovate(next: String, result: JobExecutionResult) {
        cli.outcome = { repo ->
            val file = cacheFile(repo)
            seen += if (file.exists()) file.readText() else COLD
            file.parent.createDirectories()
            file.writeText(next)
            result
        }
    }

    private fun run() {
        val runs = seen.size + 1
        cluster.publish(RenovateJobEvent(repo, UUID.randomUUID().toString(), TriggerReason.WEBHOOK))
        awaitUntil("run $runs") { seen.size == runs }
        cluster.awaitQuiescent(repo)
    }

    @Test
    fun `a successful run's cache is restored by the next run on another worker`() {
        val workerA = cluster.startWorker("worker-a").apply { renovate("cache-v1", JobExecutionResult.Success) }
        run()
        assertEquals("cache-v1", cluster.storedCache(repo))
        workerA.sigterm()

        cluster.startWorker("worker-b").renovate("cache-v2", JobExecutionResult.Success)
        run()

        assertEquals(listOf(COLD, "cache-v1"), seen, "first run is cold, the second restores worker A's cache")
        assertEquals(listOf("worker-a", "worker-b"), cluster.runs.map { it.worker })
        assertEquals("cache-v2", cluster.storedCache(repo))
    }

    @Test
    fun `failed and timed-out runs don't persist their cache`() {
        val worker = cluster.startWorker("worker-a")
        worker.renovate("cache-v1", JobExecutionResult.Success)
        run()
        worker.renovate("from-failed-run", JobExecutionResult.Failed(1))
        run()
        assertEquals("cache-v1", cluster.storedCache(repo))
        worker.renovate("from-timed-out-run", JobExecutionResult.TimedOut)
        run()
        assertEquals("cache-v1", cluster.storedCache(repo))

        worker.renovate("cache-v2", JobExecutionResult.Success)
        run()

        // Each run restores the stored cache over whatever the previous, unpersisted run left behind.
        assertEquals(listOf(COLD, "cache-v1", "cache-v1", "cache-v1"), seen)
        assertEquals("cache-v2", cluster.storedCache(repo))
    }

    @Test
    fun `without a stored cache the run proceeds cold, ignoring what a failed run left on the worker`() {
        cluster.startWorker("worker-a").renovate("from-failed-run", JobExecutionResult.Failed(1))
        run()
        run()

        assertEquals(listOf(COLD, COLD), seen)
        assertNull(cluster.storedCache(repo))
    }

    private companion object {
        const val COLD = "<cold>"
    }
}
