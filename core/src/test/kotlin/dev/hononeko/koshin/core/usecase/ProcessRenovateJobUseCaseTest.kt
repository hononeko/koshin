package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.CliExecutorPort
import dev.hononeko.koshin.core.ports.JobContextPort
import dev.hononeko.koshin.core.ports.JobDelivery
import dev.hononeko.koshin.core.ports.JobQueueConsumer
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.PlatformCredentialsPort
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.ports.ReleaseResult
import dev.hononeko.koshin.core.ports.RepoCachePort
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import dev.hononeko.koshin.core.ports.WorkspaceCleanerPort
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TestTimeSource

@OptIn(ExperimentalCoroutinesApi::class)
class ProcessRenovateJobUseCaseTest {

    private val queueConsumer = mockk<JobQueueConsumer>()
    private val queuePublisher = mockk<JobQueuePublisher>()
    private val coordinationPort = mockk<RepoCoordinationPort>()
    private val throttlePort = mockk<PlatformThrottlePort>()
    private val cachePort = mockk<RepoCachePort>()
    private val credentialsPort = mockk<PlatformCredentialsPort>()
    private val cliExecutor = mockk<CliExecutorPort>()
    private val workspaceCleaner = mockk<WorkspaceCleanerPort>()

    private val testConfig = WorkerConfig(
        natsAckWait = 60.seconds,
        lockTtl = 45.seconds,
        dirtyTtl = 3600.seconds,
        cliTimeout = 45.minutes,
        heartbeatInterval = 100.milliseconds,
        lockCollisionRetryDelay = 30.seconds,
        maxRateLimitRequeues = 5,
        fetchTimeout = 5.seconds,
        infraErrorBackoffBase = 1.seconds,
        infraErrorBackoffMax = 30.seconds,
    )

    private var uuidCounter = 0
    private val testTimeSource = TestTimeSource()
    private val useCase = ProcessRenovateJobUseCase(
        queueConsumer = queueConsumer,
        queuePublisher = queuePublisher,
        coordinationPort = coordinationPort,
        throttlePort = throttlePort,
        cachePort = cachePort,
        credentialsPort = credentialsPort,
        cliExecutor = cliExecutor,
        workspaceCleaner = workspaceCleaner,
        config = testConfig,
        uuidGenerator = { "uuid-${++uuidCounter}" },
        timeSource = testTimeSource,
    )

    private val testRepo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val testCreds = PlatformCredentials("test-token")
    private val testEvent = RenovateJobEvent(
        repo = testRepo,
        deliveryId = "initial-delivery-id",
        triggerReason = TriggerReason.WEBHOOK,
        rateLimitAttempt = 0,
    )
    private val delivery = mockk<JobDelivery>()

    @BeforeEach
    fun setup() {
        uuidCounter = 0
        every { delivery.event } returns testEvent
        every { delivery.deliveryAttempt } returns 1
        coEvery { delivery.ack() } returns Unit
        coEvery { delivery.nack(any()) } returns Unit
        coEvery { delivery.heartbeat() } returns Unit

        coEvery { throttlePort.cooldownRemaining(any()) } returns null
        coEvery { throttlePort.tryAcquireRunSlot(any(), any(), any()) } returns true
        coEvery { throttlePort.extendRunSlot(any(), any(), any()) } returns true
        coEvery { throttlePort.releaseRunSlot(any(), any()) } returns Unit
        coEvery { throttlePort.resetBackoff(any()) } returns Unit
        coEvery { throttlePort.registerRateLimit(any(), any()) } returns 60.seconds

        coEvery { coordinationPort.acquireOrMarkDirty(any(), any(), any(), any()) } returns AcquireResult.ACQUIRED
        coEvery { coordinationPort.extendLock(any(), any(), any()) } returns true
        coEvery { coordinationPort.releaseLockAndTakeDirty(any(), any()) } returns ReleaseResult.CLEAN
        coEvery { coordinationPort.releaseLock(any(), any()) } returns true

        coEvery { cachePort.restore(any()) } returns true
        coEvery { cachePort.persist(any()) } returns Unit

        coEvery { credentialsPort.credentialsFor(any()) } returns testCreds
        coEvery { cliExecutor.execute(any(), any(), any()) } returns JobExecutionResult.Success

        coEvery { workspaceCleaner.cleanup(any()) } returns Unit
        coEvery { queuePublisher.publish(any()) } returns Unit
        coEvery { queuePublisher.publishToDeadLetter(any()) } returns Unit
    }

    @Test
    fun `processNext skips platform when platform is in cooldown`() = runTest {
        coEvery { throttlePort.cooldownRemaining(Platform.GITHUB) } returns 42.seconds
        coEvery { throttlePort.cooldownRemaining(Platform.GITLAB) } returns null
        coEvery { queueConsumer.fetchNext(Platform.GITLAB, any()) } returns null

        val result = useCase.processNext(listOf(Platform.GITHUB, Platform.GITLAB))

        assertFalse(result)
        coVerify(exactly = 0) { throttlePort.tryAcquireRunSlot(Platform.GITHUB, any(), any()) }
        coVerify(exactly = 1) { throttlePort.tryAcquireRunSlot(Platform.GITLAB, any(), any()) }
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(Platform.GITLAB, any()) }
    }

    @Test
    fun `processNext skips platform when run slot cannot be acquired`() = runTest {
        coEvery { throttlePort.tryAcquireRunSlot(Platform.GITHUB, any(), any()) } returns false

        val result = useCase.processNext(listOf(Platform.GITHUB))

        assertFalse(result)
        coVerify(exactly = 0) { queueConsumer.fetchNext(any(), any()) }
    }

    @Test
    fun `processNext releases run slot when queueConsumer returns null`() = runTest {
        coEvery { queueConsumer.fetchNext(Platform.GITHUB, any()) } returns null

        val result = useCase.processNext(listOf(Platform.GITHUB))

        assertFalse(result)
        coVerify(exactly = 1) { throttlePort.tryAcquireRunSlot(Platform.GITHUB, any(), any()) }
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(Platform.GITHUB, any()) }
    }

    @Test
    fun `processNext releases run slot and rethrows when fetchNext throws exception`() = runTest {
        coEvery { queueConsumer.fetchNext(Platform.GITHUB, any()) } throws IllegalStateException("NATS broker down")

        var thrown: Throwable? = null
        try {
            useCase.processNext(listOf(Platform.GITHUB))
        } catch (e: Throwable) {
            thrown = e
        }

        assertTrue(thrown is IllegalStateException)
        assertEquals("NATS broker down", thrown?.message)
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(Platform.GITHUB, any()) }
    }

    @Test
    fun `acquireOrMarkDirty exception nacks delivery with backoff and releases run slot`() = runTest {
        coEvery {
            coordinationPort.acquireOrMarkDirty(any(), any(), any(), any())
        } throws IllegalStateException("Valkey connection timeout")

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { delivery.nack(testConfig.infraErrorBackoffBase) }
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(testRepo.platform, "token-1") }
        coVerify(exactly = 0) { cliExecutor.execute(any(), any(), any()) }
    }

    @Test
    fun `lock collision COALESCED on first delivery (attempt 1) ACKs and releases run slot`() = runTest {
        every { delivery.deliveryAttempt } returns 1
        coEvery {
            coordinationPort.acquireOrMarkDirty(any(), any(), any(), any())
        } returns AcquireResult.COALESCED

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { delivery.ack() }
        coVerify(exactly = 0) { delivery.nack(any()) }
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(testRepo.platform, "token-1") }
        coVerify(exactly = 0) { cliExecutor.execute(any(), any(), any()) }
    }

    @Test
    fun `lock collision COALESCED on redelivery (attempt 2) NAKs with delay and releases run slot`() = runTest {
        every { delivery.deliveryAttempt } returns 2
        coEvery {
            coordinationPort.acquireOrMarkDirty(any(), any(), any(), any())
        } returns AcquireResult.COALESCED

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { delivery.nack(testConfig.lockCollisionRetryDelay) }
        coVerify(exactly = 0) { delivery.ack() }
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(testRepo.platform, "token-1") }
        coVerify(exactly = 0) { cliExecutor.execute(any(), any(), any()) }
    }

    @Test
    fun `successful CLI execution with CLEAN release persists cache, resets backoff, ACKs`() = runTest {
        coEvery { coordinationPort.releaseLockAndTakeDirty(testRepo, "token-1") } returns ReleaseResult.CLEAN

        useCase.processDelivery(delivery, "token-1")

        coVerifyOrder {
            cachePort.restore(testRepo)
            credentialsPort.credentialsFor(testRepo)
            cliExecutor.execute(testRepo, testCreds, testConfig.cliTimeout)
            cachePort.persist(testRepo)
            coordinationPort.releaseLockAndTakeDirty(testRepo, "token-1")
            throttlePort.resetBackoff(testRepo.platform)
            delivery.ack()
            throttlePort.releaseRunSlot(testRepo.platform, "token-1")
            workspaceCleaner.cleanup(testRepo)
        }
        coVerify(exactly = 0) { queuePublisher.publish(any()) }
    }

    @Test
    fun `successful CLI execution with DIRTY release publishes follow-up with fresh ID`() = runTest {
        coEvery { coordinationPort.releaseLockAndTakeDirty(testRepo, "token-1") } returns ReleaseResult.DIRTY
        val publishedSlot = slot<RenovateJobEvent>()
        coEvery { queuePublisher.publish(capture(publishedSlot)) } returns Unit

        useCase.processDelivery(delivery, "token-1")

        coVerifyOrder {
            cachePort.restore(testRepo)
            credentialsPort.credentialsFor(testRepo)
            cliExecutor.execute(testRepo, testCreds, testConfig.cliTimeout)
            cachePort.persist(testRepo)
            coordinationPort.releaseLockAndTakeDirty(testRepo, "token-1")
            queuePublisher.publish(any())
            throttlePort.resetBackoff(testRepo.platform)
            delivery.ack()
            throttlePort.releaseRunSlot(testRepo.platform, "token-1")
            workspaceCleaner.cleanup(testRepo)
        }
        val followUp = publishedSlot.captured
        assertEquals(testRepo, followUp.repo)
        assertEquals(TriggerReason.DIRTY_FOLLOWUP, followUp.triggerReason)
        assertEquals(0, followUp.rateLimitAttempt)
        assertNotEquals(testEvent.deliveryId, followUp.deliveryId)
    }

    @Test
    fun `failed CLI execution with NOT_OWNER release NAKs with ZERO delay without persisting cache`() = runTest {
        coEvery { cliExecutor.execute(any(), any(), any()) } returns JobExecutionResult.Failed(1)
        coEvery { coordinationPort.releaseLockAndTakeDirty(testRepo, "token-1") } returns ReleaseResult.NOT_OWNER

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { delivery.nack(Duration.ZERO) }
        coVerify(exactly = 0) { cachePort.persist(any()) }
        coVerify(exactly = 0) { delivery.ack() }
        coVerify(exactly = 0) { throttlePort.resetBackoff(any()) }
    }

    @Test
    fun `successful CLI execution with NOT_OWNER release persists cache before release and NAKs`() = runTest {
        coEvery { coordinationPort.releaseLockAndTakeDirty(testRepo, "token-1") } returns ReleaseResult.NOT_OWNER

        useCase.processDelivery(delivery, "token-1")

        coVerifyOrder {
            cachePort.persist(testRepo)
            coordinationPort.releaseLockAndTakeDirty(testRepo, "token-1")
            delivery.nack(Duration.ZERO)
        }
        coVerify(exactly = 1) { workspaceCleaner.cleanup(testRepo) }
    }

    @Test
    fun `failed CLI execution with CLEAN release ACKs without persisting cache`() = runTest {
        coEvery { cliExecutor.execute(any(), any(), any()) } returns JobExecutionResult.Failed(1)
        coEvery { coordinationPort.releaseLockAndTakeDirty(testRepo, "token-1") } returns ReleaseResult.CLEAN

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { delivery.ack() }
        coVerify(exactly = 0) { cachePort.persist(any()) }
        coVerify(exactly = 0) { queuePublisher.publish(any()) }
    }

    @Test
    fun `timed out CLI execution with DIRTY release publishes follow-up and ACKs`() = runTest {
        coEvery { cliExecutor.execute(any(), any(), any()) } returns JobExecutionResult.TimedOut
        coEvery { coordinationPort.releaseLockAndTakeDirty(testRepo, "token-1") } returns ReleaseResult.DIRTY
        val publishedSlot = slot<RenovateJobEvent>()
        coEvery { queuePublisher.publish(capture(publishedSlot)) } returns Unit

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { queuePublisher.publish(any()) }
        assertEquals(TriggerReason.DIRTY_FOLLOWUP, publishedSlot.captured.triggerReason)
        coVerify(exactly = 1) { delivery.ack() }
        coVerify(exactly = 0) { cachePort.persist(any()) }
    }

    @Test
    fun `failed CLI execution with NOT_OWNER release NAKs with ZERO delay`() = runTest {
        coEvery { cliExecutor.execute(any(), any(), any()) } returns JobExecutionResult.Failed(2)
        coEvery { coordinationPort.releaseLockAndTakeDirty(testRepo, "token-1") } returns ReleaseResult.NOT_OWNER

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { delivery.nack(Duration.ZERO) }
        coVerify(exactly = 0) { delivery.ack() }
    }

    @Test
    fun `rate limited execution with rateLimitAttempt below max increments attempt and requeues`() = runTest {
        coEvery { cliExecutor.execute(any(), any(), any()) } returns JobExecutionResult.RateLimited(45.seconds)
        coEvery { coordinationPort.releaseLock(testRepo, "token-1") } returns true
        val publishedSlot = slot<RenovateJobEvent>()
        coEvery { queuePublisher.publish(capture(publishedSlot)) } returns Unit

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { coordinationPort.releaseLock(testRepo, "token-1") }
        coVerify(exactly = 1) { throttlePort.registerRateLimit(testRepo.platform, 45.seconds) }
        coVerify(exactly = 1) { queuePublisher.publish(any()) }
        val requeued = publishedSlot.captured
        assertEquals(1, requeued.rateLimitAttempt)
        assertEquals(TriggerReason.RATE_LIMIT_REQUEUE, requeued.triggerReason)
        assertNotEquals(testEvent.deliveryId, requeued.deliveryId)
        coVerify(exactly = 1) { delivery.ack() }
        coVerify(exactly = 0) { cachePort.persist(any()) }
        coVerify(exactly = 0) { queuePublisher.publishToDeadLetter(any()) }
    }

    @Test
    fun `rate limited execution exceeding max requeues routes to DLQ and ACKs`() = runTest {
        val exhaustedEvent = testEvent.copy(rateLimitAttempt = 5)
        every { delivery.event } returns exhaustedEvent
        coEvery { cliExecutor.execute(any(), any(), any()) } returns JobExecutionResult.RateLimited(null)
        coEvery { coordinationPort.releaseLock(testRepo, "token-1") } returns true
        val dlqSlot = slot<RenovateJobEvent>()
        coEvery { queuePublisher.publishToDeadLetter(capture(dlqSlot)) } returns Unit

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { queuePublisher.publishToDeadLetter(exhaustedEvent) }
        coVerify(exactly = 0) { queuePublisher.publish(any()) }
        coVerify(exactly = 1) { delivery.ack() }
    }

    @Test
    fun `rate limited execution when releaseLock returns false (NOT_OWNER) NAKs without requeueing`() = runTest {
        coEvery { cliExecutor.execute(any(), any(), any()) } returns JobExecutionResult.RateLimited(null)
        coEvery { coordinationPort.releaseLock(testRepo, "token-1") } returns false

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { delivery.nack(Duration.ZERO) }
        coVerify(exactly = 0) { queuePublisher.publish(any()) }
        coVerify(exactly = 0) { queuePublisher.publishToDeadLetter(any()) }
        coVerify(exactly = 0) { delivery.ack() }
    }

    @Test
    fun `lost lock during execution kills CLI and NAKs with ZERO delay`() = runTest {
        val cliStarted = CompletableDeferred<Unit>()
        val cliShouldFinish = CompletableDeferred<Unit>()

        coEvery { cliExecutor.execute(any(), any(), any()) } coAnswers {
            cliStarted.complete(Unit)
            cliShouldFinish.await()
            JobExecutionResult.Success
        }
        // First extendLock returns false (simulating lock lost to another worker)
        coEvery { coordinationPort.extendLock(testRepo, "token-1", any()) } returns false

        val job = launch {
            useCase.processDelivery(delivery, "token-1")
        }

        cliStarted.await()
        advanceTimeBy(testConfig.heartbeatInterval + 10.milliseconds)
        job.join()

        coVerify(exactly = 1) { coordinationPort.releaseLock(testRepo, "token-1") }
        coVerify(exactly = 1) { delivery.nack(Duration.ZERO) }
        coVerify(exactly = 0) { delivery.ack() }
        coVerify(exactly = 0) { cachePort.persist(any()) }
        coVerify(exactly = 1) { workspaceCleaner.cleanup(testRepo) }
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(testRepo.platform, "token-1") }
    }

    @Test
    fun `extendRunSlot returning false cancels CLI and releases lock`() = runTest {
        val cliStarted = CompletableDeferred<Unit>()
        val cliShouldFinish = CompletableDeferred<Unit>()

        coEvery { cliExecutor.execute(any(), any(), any()) } coAnswers {
            cliStarted.complete(Unit)
            cliShouldFinish.await()
            JobExecutionResult.Success
        }
        coEvery { throttlePort.extendRunSlot(testRepo.platform, "token-1", any()) } returns false

        val job = launch {
            useCase.processDelivery(delivery, "token-1")
        }

        cliStarted.await()
        advanceTimeBy(testConfig.heartbeatInterval + 10.milliseconds)
        job.join()

        coVerify(exactly = 1) { coordinationPort.releaseLock(testRepo, "token-1") }
        coVerify(exactly = 1) { delivery.nack(Duration.ZERO) }
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(testRepo.platform, "token-1") }
    }

    @Test
    fun `transient heartbeat error within tolerance window does not cancel CLI run`() = runTest {
        val cliStarted = CompletableDeferred<Unit>()
        val cliShouldFinish = CompletableDeferred<Unit>()

        coEvery { cliExecutor.execute(any(), any(), any()) } coAnswers {
            cliStarted.complete(Unit)
            cliShouldFinish.await()
            JobExecutionResult.Success
        }
        var callCount = 0
        coEvery { coordinationPort.extendLock(testRepo, "token-1", any()) } answers {
            callCount++
            if (callCount == 1) {
                error("Temporary network blip")
            } else {
                true
            }
        }

        val job = launch {
            useCase.processDelivery(delivery, "token-1")
        }

        cliStarted.await()
        advanceTimeBy(testConfig.heartbeatInterval + 10.milliseconds)
        advanceTimeBy(testConfig.heartbeatInterval)
        cliShouldFinish.complete(Unit)
        job.join()

        coVerify(exactly = 1) { delivery.ack() }
        coVerify(exactly = 0) { delivery.nack(any()) }
    }

    @Test
    fun `heartbeat transient errors exceeding tolerance window cancel CLI and NAK with ZERO delay`() = runTest {
        val cliStarted = CompletableDeferred<Unit>()
        val cliShouldFinish = CompletableDeferred<Unit>()

        coEvery { cliExecutor.execute(any(), any(), any()) } coAnswers {
            cliStarted.complete(Unit)
            cliShouldFinish.await()
            JobExecutionResult.Success
        }
        coEvery {
            coordinationPort.extendLock(testRepo, "token-1", any())
        } throws IllegalStateException("Lock extension network failure")

        val job = launch {
            useCase.processDelivery(delivery, "token-1")
        }

        cliStarted.await()
        testTimeSource += testConfig.lockTtl - testConfig.heartbeatInterval + 1.seconds
        advanceTimeBy(testConfig.heartbeatInterval + 10.milliseconds)
        job.join()

        coVerify(exactly = 1) { coordinationPort.releaseLock(testRepo, "token-1") }
        coVerify(exactly = 1) { delivery.nack(Duration.ZERO) }
    }

    @Test
    fun `heartbeat loop successfully extends lock and run slot across multiple intervals`() = runTest {
        val cliStarted = CompletableDeferred<Unit>()
        val cliShouldFinish = CompletableDeferred<Unit>()

        coEvery { cliExecutor.execute(any(), any(), any()) } coAnswers {
            cliStarted.complete(Unit)
            cliShouldFinish.await()
            JobExecutionResult.Success
        }

        val job = launch {
            useCase.processDelivery(delivery, "token-1")
        }

        cliStarted.await()
        advanceTimeBy(testConfig.heartbeatInterval * 2 + 10.milliseconds)
        cliShouldFinish.complete(Unit)
        job.join()

        coVerify(atLeast = 2) { delivery.heartbeat() }
        coVerify(atLeast = 2) { coordinationPort.extendLock(testRepo, "token-1", testConfig.lockTtl) }
        coVerify(atLeast = 2) { throttlePort.extendRunSlot(testRepo.platform, "token-1", testConfig.lockTtl) }
        coVerify(exactly = 1) { delivery.ack() }
    }

    @Test
    fun `infra error during execution releases lock and NAKs with computed backoff`() = runTest {
        every { delivery.deliveryAttempt } returns 3
        coEvery { credentialsPort.credentialsFor(testRepo) } throws IllegalStateException("GitHub App API 500 error")

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { coordinationPort.releaseLock(testRepo, "token-1") }
        // Attempt 3 backoff: 1s * 2^(3-1) = 4s
        coVerify(exactly = 1) { delivery.nack(4.seconds) }
        coVerify(exactly = 0) { delivery.ack() }
        coVerify(exactly = 1) { workspaceCleaner.cleanup(testRepo) }
    }

    @Test
    fun `SIGTERM or CancellationException releases lock, NAKs with ZERO, and rethrows`() = runTest {
        val cliStarted = CompletableDeferred<Unit>()

        coEvery { cliExecutor.execute(any(), any(), any()) } coAnswers {
            cliStarted.complete(Unit)
            delay(10.minutes)
            JobExecutionResult.Success
        }

        val job = launch {
            useCase.processDelivery(delivery, "token-1")
        }

        cliStarted.await()
        job.cancel(CancellationException("SIGTERM received"))
        job.join()

        coVerify(exactly = 1) { coordinationPort.releaseLock(testRepo, "token-1") }
        coVerify(exactly = 1) { delivery.nack(Duration.ZERO) }
        coVerify(exactly = 1) { workspaceCleaner.cleanup(testRepo) }
    }

    @Test
    fun `processNext returns true when a job is successfully fetched and processed`() = runTest {
        coEvery { queueConsumer.fetchNext(Platform.GITHUB, any()) } returns delivery

        val result = useCase.processNext(listOf(Platform.GITHUB))

        assertTrue(result)
        coVerify(exactly = 1) { delivery.ack() }
    }

    @Test
    fun `computeBackoffDelay calculates exponential backoff capped at max`() {
        assertEquals(1.seconds, useCase.computeBackoffDelay(1))
        assertEquals(2.seconds, useCase.computeBackoffDelay(2))
        assertEquals(4.seconds, useCase.computeBackoffDelay(3))
        assertEquals(8.seconds, useCase.computeBackoffDelay(4))
        assertEquals(16.seconds, useCase.computeBackoffDelay(5))
        assertEquals(30.seconds, useCase.computeBackoffDelay(6))
        assertEquals(30.seconds, useCase.computeBackoffDelay(10))
    }

    @Test
    fun `processNext with default parameters iterates all platforms`() = runTest {
        coEvery { queueConsumer.fetchNext(any(), any()) } returns null

        val result = useCase.processNext()

        assertFalse(result)
        coVerify(exactly = Platform.entries.size) { throttlePort.tryAcquireRunSlot(any(), any(), any()) }
    }

    @Test
    fun `processDelivery with default lockToken generates token via uuidGenerator`() = runTest {
        useCase.processDelivery(delivery)

        coVerify(exactly = 1) { coordinationPort.acquireOrMarkDirty(testRepo, "uuid-1", any(), any()) }
    }

    @Test
    fun `processNext proceeds when cooldown is zero`() = runTest {
        coEvery { throttlePort.cooldownRemaining(Platform.GITHUB) } returns Duration.ZERO
        coEvery { queueConsumer.fetchNext(Platform.GITHUB, any()) } returns null

        val result = useCase.processNext(listOf(Platform.GITHUB))

        assertFalse(result)
        coVerify(exactly = 1) { throttlePort.tryAcquireRunSlot(Platform.GITHUB, any(), any()) }
    }

    @Test
    fun `cancellation during fetch releases run slot and rethrows CancellationException`() = runTest {
        coEvery { queueConsumer.fetchNext(Platform.GITHUB, any()) } throws CancellationException("Worker shutdown")

        var thrown: Throwable? = null
        try {
            useCase.processNext(listOf(Platform.GITHUB))
        } catch (e: Throwable) {
            thrown = e
        }

        assertTrue(thrown is CancellationException)
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(Platform.GITHUB, any()) }
    }

    @Test
    fun `cancellation during acquire releases run slot, releases lock, NAKs with 0, and rethrows`() = runTest {
        coEvery {
            coordinationPort.acquireOrMarkDirty(any(), any(), any(), any())
        } throws CancellationException("Worker shutdown")

        var thrown: Throwable? = null
        try {
            useCase.processDelivery(delivery, "token-1")
        } catch (e: Throwable) {
            thrown = e
        }

        assertTrue(thrown is CancellationException)
        coVerify(exactly = 1) { coordinationPort.releaseLock(testRepo, "token-1") }
        coVerify(exactly = 1) { delivery.nack(Duration.ZERO) }
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(testRepo.platform, "token-1") }
    }

    @Test
    fun `workspace cleaner throwing exception does not prevent run slot release`() = runTest {
        coEvery { workspaceCleaner.cleanup(testRepo) } throws IllegalStateException("Disk IO error")

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { throttlePort.releaseRunSlot(testRepo.platform, "token-1") }
        coVerify(exactly = 1) { delivery.ack() }
    }

    @Test
    fun `NATS heartbeat blip does not prevent Valkey lock extension`() = runTest {
        val cliStarted = CompletableDeferred<Unit>()
        val cliShouldFinish = CompletableDeferred<Unit>()

        coEvery { cliExecutor.execute(any(), any(), any()) } coAnswers {
            cliStarted.complete(Unit)
            cliShouldFinish.await()
            JobExecutionResult.Success
        }
        coEvery { delivery.heartbeat() } throws IllegalStateException("NATS connection timeout")

        val job = launch {
            useCase.processDelivery(delivery, "token-1")
        }

        cliStarted.await()
        advanceTimeBy(testConfig.heartbeatInterval + 10.milliseconds)
        cliShouldFinish.complete(Unit)
        job.join()

        coVerify(atLeast = 1) { coordinationPort.extendLock(testRepo, "token-1", testConfig.lockTtl) }
        coVerify(atLeast = 1) { throttlePort.extendRunSlot(testRepo.platform, "token-1", testConfig.lockTtl) }
        coVerify(exactly = 1) { delivery.ack() }
    }

    @Test
    fun `cache persist failure does not fail or rerun successful job`() = runTest {
        coEvery { cachePort.persist(testRepo) } throws IllegalStateException("Object store write failure")

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { delivery.ack() }
        coVerify(exactly = 0) { delivery.nack(any()) }
        coVerify(exactly = 1) { throttlePort.resetBackoff(testRepo.platform) }
        coVerify(exactly = 1) { throttlePort.releaseRunSlot(testRepo.platform, "token-1") }
    }

    @Test
    fun `cache restore failure runs the job cold`() = runTest {
        coEvery { cachePort.restore(testRepo) } throws IllegalStateException("Object store unreachable")

        useCase.processDelivery(delivery, "token-1")

        coVerify(exactly = 1) { cliExecutor.execute(testRepo, any(), any()) }
        coVerify(exactly = 1) { cachePort.persist(testRepo) }
        coVerify(exactly = 1) { delivery.ack() }
        coVerify(exactly = 0) { delivery.nack(any()) }
    }

    @Test
    fun `cancellation during cache persist is not swallowed`() = runTest {
        coEvery { cachePort.persist(testRepo) } throws CancellationException("SIGTERM")

        val thrown = runCatching { useCase.processDelivery(delivery, "token-1") }.exceptionOrNull()

        assertTrue(thrown is CancellationException, "got $thrown")
        coVerify(exactly = 0) { delivery.ack() }
        coVerify(exactly = 1) { delivery.nack(Duration.ZERO) }
        coVerify(exactly = 1) { coordinationPort.releaseLock(testRepo, "token-1") }
    }

    @Test
    fun `the whole delivery runs within the job context`() = runTest {
        val calls = mutableListOf<String>()
        val jobContext = object : JobContextPort {
            override suspend fun within(event: RenovateJobEvent, deliveryAttempt: Int, block: suspend () -> Unit) {
                calls += "enter ${event.deliveryId} #$deliveryAttempt"
                block()
                calls += "exit"
            }
        }
        coEvery { cliExecutor.execute(any(), any(), any()) } coAnswers {
            calls += "cli"
            JobExecutionResult.Success
        }
        val contextual = ProcessRenovateJobUseCase(
            queueConsumer = queueConsumer,
            queuePublisher = queuePublisher,
            coordinationPort = coordinationPort,
            throttlePort = throttlePort,
            cachePort = cachePort,
            credentialsPort = credentialsPort,
            cliExecutor = cliExecutor,
            workspaceCleaner = workspaceCleaner,
            config = testConfig,
            jobContext = jobContext,
        )

        contextual.processDelivery(delivery, "token-1")

        assertEquals(listOf("enter initial-delivery-id #1", "cli", "exit"), calls)
        coVerify(exactly = 1) { delivery.ack() }
    }
}
