package dev.hononeko.koshin.core.model

import dev.hononeko.koshin.core.ports.JobDelivery
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class DomainModelTest {

    @Test
    fun `RepoRef toString and validation`() {
        val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
        assertEquals("github:hononeko/koshin", repo.toString())

        assertThrows(IllegalArgumentException::class.java) {
            RepoRef(Platform.GITHUB, "   ")
        }
    }

    @Test
    fun `RenovateJobEvent validation`() {
        val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")

        assertThrows(IllegalArgumentException::class.java) {
            RenovateJobEvent(repo, "   ", TriggerReason.WEBHOOK)
        }

        assertThrows(IllegalArgumentException::class.java) {
            RenovateJobEvent(repo, "id-1", TriggerReason.WEBHOOK, rateLimitAttempt = -1)
        }
    }

    @Test
    fun `PlatformCredentials validation`() {
        assertThrows(IllegalArgumentException::class.java) {
            PlatformCredentials("   ")
        }
    }

    @Test
    fun `JobExecutionResult hierarchy`() {
        val success: JobExecutionResult = JobExecutionResult.Success
        val failed: JobExecutionResult = JobExecutionResult.Failed(1)
        val timedOut: JobExecutionResult = JobExecutionResult.TimedOut
        val rateLimited: JobExecutionResult = JobExecutionResult.RateLimited(60.seconds)

        assertEquals(JobExecutionResult.Success, success)
        assertEquals(1, (failed as JobExecutionResult.Failed).exitCode)
        assertEquals(JobExecutionResult.TimedOut, timedOut)
        assertEquals(60.seconds, (rateLimited as JobExecutionResult.RateLimited).retryAfter)
    }

    @Test
    fun `JobDelivery default nack parameter`() = runTest {
        var nackDelay: Duration? = null
        val testDelivery = object : JobDelivery {
            override val event: RenovateJobEvent = RenovateJobEvent(
                RepoRef(Platform.GITHUB, "hononeko/koshin"),
                "del-1",
                TriggerReason.WEBHOOK,
            )
            override val deliveryAttempt: Int = 1
            override suspend fun ack() = Unit
            override suspend fun nack(delay: Duration) {
                nackDelay = delay
            }
            override suspend fun heartbeat() = Unit
        }
        testDelivery.nack()
        assertEquals(Duration.ZERO, nackDelay)
    }
}
