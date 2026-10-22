package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
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
import dev.hononeko.koshin.core.ports.ReleaseResult
import dev.hononeko.koshin.core.ports.RepoCachePort
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import dev.hononeko.koshin.core.ports.WorkspaceCleanerPort
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import java.util.UUID
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration

class ProcessRenovateJobUseCase(
    private val queueConsumer: JobQueueConsumer,
    private val queuePublisher: JobQueuePublisher,
    private val coordinationPort: RepoCoordinationPort,
    private val throttlePort: PlatformThrottlePort,
    private val cachePort: RepoCachePort,
    private val credentialsPort: PlatformCredentialsPort,
    private val cliExecutor: CliExecutorPort,
    private val workspaceCleaner: WorkspaceCleanerPort,
    private val config: WorkerConfig = WorkerConfig(),
    private val uuidGenerator: () -> String = { UUID.randomUUID().toString() },
) {

    suspend fun processNext(platforms: Collection<Platform> = Platform.entries): Boolean {
        for (platform in platforms) {
            val handled = tryProcessPlatform(platform)
            if (handled) return true
        }
        return false
    }

    private suspend fun tryProcessPlatform(platform: Platform): Boolean {
        val cooldown = throttlePort.cooldownRemaining(platform)
        val inCooldown = cooldown != null && cooldown > Duration.ZERO
        val lockToken = uuidGenerator()

        if (inCooldown || !throttlePort.tryAcquireRunSlot(platform, lockToken, config.lockTtl)) {
            return false
        }

        val delivery = try {
            queueConsumer.fetchNext(platform, config.fetchTimeout)
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            throttlePort.releaseRunSlot(platform, lockToken)
            throw e
        }

        return if (delivery != null) {
            processDelivery(delivery, lockToken)
            true
        } else {
            throttlePort.releaseRunSlot(platform, lockToken)
            false
        }
    }

    suspend fun processDelivery(delivery: JobDelivery, lockToken: String = uuidGenerator()) {
        val event = delivery.event
        val repo = event.repo

        val acquireResult = try {
            coordinationPort.acquireOrMarkDirty(
                repo = repo,
                token = lockToken,
                lockTtl = config.lockTtl,
                dirtyTtl = config.dirtyTtl,
            )
        } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") e: Exception) {
            try {
                delivery.nack(computeBackoffDelay(delivery.deliveryAttempt))
            } finally {
                throttlePort.releaseRunSlot(repo.platform, lockToken)
            }
            return
        }

        if (acquireResult == AcquireResult.COALESCED) {
            handleCoalesced(delivery, repo, lockToken)
            return
        }

        executeAcquiredJob(delivery, lockToken)
    }

    private suspend fun handleCoalesced(delivery: JobDelivery, repo: RepoRef, lockToken: String) {
        try {
            if (delivery.deliveryAttempt <= 1) {
                delivery.ack()
            } else {
                delivery.nack(config.lockCollisionRetryDelay)
            }
        } finally {
            throttlePort.releaseRunSlot(repo.platform, lockToken)
        }
    }

    private suspend fun executeAcquiredJob(delivery: JobDelivery, lockToken: String) {
        val event = delivery.event
        val repo = event.repo

        coroutineScope {
            var lostLock = false
            var executionJob: Job = Job()

            val heartbeatJob = launch {
                runHeartbeatLoop(delivery, repo, lockToken) {
                    lostLock = true
                    executionJob.cancel(LostLockException(repo))
                }
            }

            try {
                coroutineScope {
                    executionJob = coroutineContext.job
                    runExecution(delivery, lockToken) { lostLock = true }
                }
            } catch (@Suppress("SwallowedException") e: LostLockException) {
                delivery.nack(Duration.ZERO)
            } catch (e: CancellationException) {
                runCatching { coordinationPort.releaseLock(repo, lockToken) }
                delivery.nack(Duration.ZERO)
                throw e
            } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") e: Exception) {
                runCatching { coordinationPort.releaseLock(repo, lockToken) }
                delivery.nack(computeBackoffDelay(delivery.deliveryAttempt))
            } finally {
                heartbeatJob.cancel()
                workspaceCleaner.cleanup(repo)
                throttlePort.releaseRunSlot(repo.platform, lockToken)
            }
        }
    }

    private suspend fun runExecution(delivery: JobDelivery, lockToken: String, onLostLock: () -> Unit) {
        val event = delivery.event
        val repo = event.repo

        cachePort.restore(repo)
        val creds = credentialsPort.credentialsFor(repo)
        val result = cliExecutor.execute(repo, creds, config.cliTimeout)

        when (result) {
            is JobExecutionResult.Success,
            is JobExecutionResult.Failed,
            is JobExecutionResult.TimedOut,
            -> {
                handleTerminalResult(delivery, lockToken, result, onLostLock)
            }

            is JobExecutionResult.RateLimited -> {
                handleRateLimited(delivery, lockToken, result, onLostLock)
            }
        }
    }

    private suspend fun runHeartbeatLoop(
        delivery: JobDelivery,
        repo: RepoRef,
        lockToken: String,
        onLostLock: () -> Unit,
    ) {
        while (true) {
            delay(config.heartbeatInterval)
            val keepGoing = performHeartbeatStep(delivery, repo, lockToken)
            if (!keepGoing) {
                onLostLock()
                return
            }
        }
    }

    private suspend fun performHeartbeatStep(delivery: JobDelivery, repo: RepoRef, lockToken: String): Boolean = try {
        delivery.heartbeat()
        val lockExtended = coordinationPort.extendLock(repo, lockToken, config.lockTtl)
        throttlePort.extendRunSlot(repo.platform, lockToken, config.lockTtl)
        lockExtended
    } catch (e: CancellationException) {
        throw e
    } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") e: Exception) {
        false
    }

    private suspend fun handleTerminalResult(
        delivery: JobDelivery,
        lockToken: String,
        result: JobExecutionResult,
        onLostLock: () -> Unit,
    ) {
        val repo = delivery.event.repo
        val release = coordinationPort.releaseLockAndTakeDirty(repo, lockToken)

        when (release) {
            ReleaseResult.DIRTY -> {
                publishDirtyFollowUp(repo)
                if (result is JobExecutionResult.Success) {
                    cachePort.persist(repo)
                    throttlePort.resetBackoff(repo.platform)
                }
                delivery.ack()
            }

            ReleaseResult.CLEAN -> {
                if (result is JobExecutionResult.Success) {
                    cachePort.persist(repo)
                    throttlePort.resetBackoff(repo.platform)
                }
                delivery.ack()
            }

            ReleaseResult.NOT_OWNER -> {
                onLostLock()
                delivery.nack(Duration.ZERO)
            }
        }
    }

    private suspend fun publishDirtyFollowUp(repo: RepoRef) {
        queuePublisher.publish(
            RenovateJobEvent(
                repo = repo,
                deliveryId = uuidGenerator(),
                triggerReason = TriggerReason.DIRTY_FOLLOWUP,
                rateLimitAttempt = 0,
            ),
        )
    }

    private suspend fun handleRateLimited(
        delivery: JobDelivery,
        lockToken: String,
        result: JobExecutionResult.RateLimited,
        onLostLock: () -> Unit,
    ) {
        val event = delivery.event
        val repo = event.repo
        val released = coordinationPort.releaseLock(repo, lockToken)

        if (!released) {
            onLostLock()
            delivery.nack(Duration.ZERO)
            return
        }

        throttlePort.registerRateLimit(repo.platform, result.retryAfter)
        if (event.rateLimitAttempt < config.maxRateLimitRequeues) {
            queuePublisher.publish(
                RenovateJobEvent(
                    repo = repo,
                    deliveryId = uuidGenerator(),
                    triggerReason = TriggerReason.RATE_LIMIT_REQUEUE,
                    rateLimitAttempt = event.rateLimitAttempt + 1,
                ),
            )
        } else {
            queuePublisher.publishToDeadLetter(event)
        }
        delivery.ack()
    }

    fun computeBackoffDelay(deliveryAttempt: Int): Duration {
        val factor = 1 shl (deliveryAttempt - 1).coerceIn(0, 6)
        val computed = config.infraErrorBackoffBase * factor
        return computed.coerceAtMost(config.infraErrorBackoffMax)
    }
}
