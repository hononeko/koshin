package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.CliExecutorPort
import dev.hononeko.koshin.core.ports.JobDelivery
import dev.hononeko.koshin.core.ports.JobQueueConsumer
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.PlatformCredentialsPort
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.ports.QueueDepth
import dev.hononeko.koshin.core.ports.QueueDepthPort
import dev.hononeko.koshin.core.ports.ReleaseResult
import dev.hononeko.koshin.core.ports.RepoCachePort
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * How `processNext` picks among credential scopes (KSH-24), and the scope a job's run slot, follow-ups and repo cache
 * (KSH-26) use.
 */
class ProcessRenovateJobScopesTest {
    private val github = CredentialScope.Token(Platform.GITHUB)
    private val depths = mutableMapOf<CredentialScope, QueueDepth>(github to QueueDepth(pending = 1, ackPending = 0))
    private val queueDepth = object : QueueDepthPort {
        override suspend fun depths(platforms: Collection<Platform>) = depths.filterKeys { it.platform in platforms }
    }
    private val queueConsumer = mockk<JobQueueConsumer>()
    private val queuePublisher = mockk<JobQueuePublisher>(relaxed = true)
    private val coordinationPort = mockk<RepoCoordinationPort> {
        coEvery { acquireOrMarkDirty(any(), any(), any(), any()) } returns AcquireResult.ACQUIRED
        coEvery { extendLock(any(), any(), any()) } returns true
        coEvery { releaseLockAndTakeDirty(any(), any(), any()) } returns ReleaseResult.CLEAN
        coEvery { releaseLock(any(), any(), any()) } returns true
    }
    private val throttlePort = mockk<PlatformThrottlePort>(relaxed = true) {
        coEvery { cooldownRemaining(any()) } returns null
        coEvery { tryAcquireRunSlot(any(), any(), any()) } returns true
        coEvery { extendRunSlot(any(), any(), any()) } returns true
        coEvery { activeRunSlots(any()) } returns 0
    }
    private val cliExecutor = mockk<CliExecutorPort> {
        coEvery { execute(any(), any(), any()) } returns JobExecutionResult.Success
    }
    private val cachePort = mockk<RepoCachePort>(relaxed = true)
    private val credentialsPort = mockk<PlatformCredentialsPort> {
        coEvery { credentialsFor(any()) } returns PlatformCredentials("token")
    }
    private val useCase = ProcessRenovateJobUseCase(
        queueConsumer = queueConsumer,
        queueDepth = queueDepth,
        queuePublisher = queuePublisher,
        coordinationPort = coordinationPort,
        throttlePort = throttlePort,
        cachePort = cachePort,
        credentialsPort = credentialsPort,
        cliExecutor = cliExecutor,
        workspaceCleaner = mockk(relaxed = true),
        config = WorkerConfig(heartbeatInterval = 100.milliseconds),
    )
    private val testEvent = RenovateJobEvent(RepoRef(Platform.GITHUB, "hononeko/koshin"), "d", TriggerReason.WEBHOOK)
    private val delivery = mockk<JobDelivery>(relaxed = true) {
        every { event } returns testEvent
        every { deliveryAttempt } returns 1
    }

    @Test
    fun `the repo cache is the scope of the credential the run used, not the queue's (KSH-26)`() = runTest {
        // Queued as github:pat (e.g. by a proxy without App auth), run with installation 42's token
        val repo = testEvent.repo
        val creds = PlatformCredentials("ghs_installation42", installationId = 42)
        coEvery { credentialsPort.credentialsFor(repo) } returns creds

        useCase.processDelivery(delivery, "token-1")

        val installation = CredentialScope.Installation(42)
        coVerifyOrder {
            cachePort.restore(repo, installation)
            cliExecutor.execute(repo, creds, any())
            cachePort.persist(repo, installation)
            throttlePort.releaseRunSlot(github, "token-1")
        }
        coVerify(exactly = 0) { cachePort.restore(any(), github) }
    }

    @Test
    fun `processNext goes round-robin over the scopes with queued jobs`() = runTest {
        val inst1 = CredentialScope.Installation(1)
        val inst2 = CredentialScope.Installation(2)
        depths[inst2] = QueueDepth(pending = 100, ackPending = 0)
        depths[inst1] = QueueDepth(pending = 100, ackPending = 0)
        coEvery { queueConsumer.fetchNext(any(), any()) } returns delivery

        repeat(4) { assertTrue(useCase.processNext(listOf(Platform.GITHUB))) }

        coVerifyOrder {
            queueConsumer.fetchNext(inst1, any())
            queueConsumer.fetchNext(inst2, any())
            queueConsumer.fetchNext(github, any())
            queueConsumer.fetchNext(inst1, any())
        }
        coVerify(exactly = 2) { throttlePort.releaseRunSlot(inst1, any()) }
    }

    @Test
    fun `a scope without a free run slot doesn't hold up the next one`() = runTest {
        val busy = CredentialScope.Installation(1)
        depths[busy] = QueueDepth(pending = 300, ackPending = 2)
        coEvery { throttlePort.tryAcquireRunSlot(busy, any(), any()) } returns false
        coEvery { queueConsumer.fetchNext(github, any()) } returns delivery

        assertTrue(useCase.processNext(listOf(Platform.GITHUB)))

        coVerify(exactly = 0) { queueConsumer.fetchNext(busy, any()) }
        coVerify(exactly = 1) { delivery.ack() }
    }

    @Test
    fun `a cooled-down scope's jobs are left in the queue, untouched`() = runTest {
        val limited = CredentialScope.Installation(111)
        depths.clear()
        depths[limited] = QueueDepth(pending = 5, ackPending = 0)
        coEvery { throttlePort.cooldownRemaining(limited) } returns 10.seconds

        assertFalse(useCase.processNext(listOf(Platform.GITHUB)))

        coVerify(exactly = 0) { throttlePort.tryAcquireRunSlot(any(), any(), any()) }
        coVerify(exactly = 0) { queueConsumer.fetchNext(any(), any()) }
    }

    @Test
    fun `nothing is fetched while there's no job, or only running ones`() = runTest {
        depths.clear()
        assertFalse(useCase.processNext(listOf(Platform.GITHUB)))

        depths[github] = QueueDepth(pending = 0, ackPending = 2)
        coEvery { throttlePort.activeRunSlots(github) } returns 2
        assertFalse(useCase.processNext(listOf(Platform.GITHUB)))

        coVerify(exactly = 0) { throttlePort.tryAcquireRunSlot(any(), any(), any()) }
    }

    @Test
    fun `a delivery NAKed for a later retry is fetched again`() = runTest {
        depths[github] = QueueDepth(pending = 0, ackPending = 2)
        coEvery { throttlePort.activeRunSlots(github) } returns 1
        coEvery { queueConsumer.fetchNext(github, any()) } returns delivery

        assertTrue(useCase.processNext(listOf(Platform.GITHUB)))
    }

    @Test
    fun `an installation's follow-up and rate-limit requeue stay in its scope`() = runTest {
        val installed = testEvent.copy(installationId = 42)
        val scope = CredentialScope.Installation(42)
        every { delivery.event } returns installed
        coEvery { coordinationPort.releaseLockAndTakeDirty(any(), any(), any()) } returns ReleaseResult.DIRTY
        val published = mutableListOf<RenovateJobEvent>()
        coEvery { queuePublisher.publish(capture(published)) } returns Unit

        useCase.processDelivery(delivery, "token-1")
        coEvery { cliExecutor.execute(any(), any(), any()) } returns JobExecutionResult.RateLimited(30.seconds)
        useCase.processDelivery(delivery, "token-2")

        assertEquals(listOf(42L, 42L), published.map { it.installationId })
        assertEquals(TriggerReason.RATE_LIMIT_REQUEUE, published.last().triggerReason)
        coVerify(exactly = 1) { throttlePort.resetBackoff(scope) }
        coVerify(exactly = 1) { throttlePort.registerRateLimit(scope, 30.seconds) }
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(scope, "token-1") }
        coVerify(exactly = 0) { throttlePort.registerRateLimit(github, any()) }
    }
}
