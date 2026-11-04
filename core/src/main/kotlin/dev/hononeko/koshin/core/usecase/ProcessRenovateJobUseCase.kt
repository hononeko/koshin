package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
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
import dev.hononeko.koshin.core.ports.QueueDepth
import dev.hononeko.koshin.core.ports.QueueDepthPort
import dev.hononeko.koshin.core.ports.ReleaseResult
import dev.hononeko.koshin.core.ports.RepoCachePort
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import dev.hononeko.koshin.core.ports.RepositoryRejectedException
import dev.hononeko.koshin.core.ports.WorkspaceCleanerPort
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.TimeSource

/**
 * Runs one queued job (`processNext`). Jobs are queued per credential scope (KSH-24): it goes round-robin over the
 * scopes with queued jobs, so a tenant with hundreds of them can't starve the others, and skips a scope in a
 * rate-limit cooldown or without a free run slot before fetching, so its jobs wait in the queue without spending
 * delivery attempts.
 */
@Suppress("LongParameterList") // The use case's ports; the last four have defaults
class ProcessRenovateJobUseCase(
    private val queueConsumer: JobQueueConsumer,
    private val queueDepth: QueueDepthPort,
    private val queuePublisher: JobQueuePublisher,
    private val coordinationPort: RepoCoordinationPort,
    private val throttlePort: PlatformThrottlePort,
    private val cachePort: RepoCachePort,
    private val credentialsPort: PlatformCredentialsPort,
    private val cliExecutor: CliExecutorPort,
    private val workspaceCleaner: WorkspaceCleanerPort,
    private val config: WorkerConfig = WorkerConfig(),
    private val uuidGenerator: () -> String = { UUID.randomUUID().toString() },
    private val timeSource: TimeSource = TimeSource.Monotonic,
    private val jobContext: JobContextPort = JobContextPort.None,
) {
    /** The scope served last: the next round starts after it. */
    @Volatile
    private var lastServed: String? = null

    /** Runs one job of [platforms] if one can run now; false if none could. */
    suspend fun processNext(platforms: Collection<Platform> = Platform.entries): Boolean {
        val queued = queueDepth.depths(platforms).filterValues { !it.isEmpty }
        for (scope in roundRobin(queued.keys)) {
            if (tryProcessScope(scope, queued.getValue(scope))) {
                lastServed = scope.toString()
                return true
            }
        }
        return false
    }

    /** [scopes] in a stable order, starting after the one served last. */
    private fun roundRobin(scopes: Collection<CredentialScope>): List<CredentialScope> {
        val sorted = scopes.sortedBy { it.toString() }
        val last = lastServed ?: return sorted
        val (after, upToLast) = sorted.partition { it.toString() > last }
        return after + upToLast
    }

    private suspend fun tryProcessScope(scope: CredentialScope, depth: QueueDepth): Boolean {
        val lockToken = uuidGenerator()
        if (!isRunnable(scope, depth) || !throttlePort.tryAcquireRunSlot(scope, lockToken, config.lockTtl)) {
            return false
        }

        val delivery = try {
            queueConsumer.fetchNext(scope, config.fetchTimeout)
        } catch (e: CancellationException) {
            withContext(NonCancellable) {
                throttlePort.releaseRunSlot(scope, lockToken)
            }
            throw e
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            throttlePort.releaseRunSlot(scope, lockToken)
            throw e
        }

        return if (delivery != null) {
            processDelivery(delivery, lockToken, scope)
            true
        } else {
            throttlePort.releaseRunSlot(scope, lockToken)
            false
        }
    }

    /**
     * Not cooling down, and with a job to fetch: one never delivered, or a delivery more than the scope's run slots
     * hold (one NAKed for a later retry), rather than only jobs already running.
     */
    private suspend fun isRunnable(scope: CredentialScope, depth: QueueDepth): Boolean {
        val cooldown = throttlePort.cooldownRemaining(scope)
        if (cooldown != null && cooldown > Duration.ZERO) return false
        return depth.pending > 0 || throttlePort.activeRunSlots(scope) < depth.ackPending
    }

    /** Runs [delivery], holding run slot [lockToken] of [scope], which it releases. */
    suspend fun processDelivery(
        delivery: JobDelivery,
        lockToken: String = uuidGenerator(),
        scope: CredentialScope = delivery.event.scope,
    ) {
        jobContext.within(delivery.event, delivery.deliveryAttempt) { process(delivery, lockToken, scope) }
    }

    private suspend fun process(delivery: JobDelivery, lockToken: String, scope: CredentialScope) {
        val event = delivery.event
        val repo = event.repo

        val acquireResult = try {
            coordinationPort.acquireOrMarkDirty(
                repo = repo,
                token = lockToken,
                lockTtl = config.lockTtl,
                dirtyTtl = config.dirtyTtl,
            )
        } catch (e: CancellationException) {
            withContext(NonCancellable) {
                runCatching { coordinationPort.releaseLock(repo, lockToken, config.queuedTtl) }
                runCatching { delivery.nack(Duration.ZERO) }
                runCatching { throttlePort.releaseRunSlot(scope, lockToken) }
            }
            throw e
        } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") e: Exception) {
            try {
                delivery.nack(computeBackoffDelay(delivery.deliveryAttempt))
            } finally {
                throttlePort.releaseRunSlot(scope, lockToken)
            }
            return
        }

        if (acquireResult == AcquireResult.COALESCED) {
            handleCoalesced(delivery, scope, lockToken)
            return
        }

        executeAcquiredJob(delivery, lockToken, scope)
    }

    private suspend fun handleCoalesced(delivery: JobDelivery, scope: CredentialScope, lockToken: String) {
        try {
            if (delivery.deliveryAttempt <= 1) {
                delivery.ack()
            } else {
                delivery.nack(config.lockCollisionRetryDelay)
            }
        } finally {
            withContext(NonCancellable) {
                throttlePort.releaseRunSlot(scope, lockToken)
            }
        }
    }

    private suspend fun executeAcquiredJob(delivery: JobDelivery, lockToken: String, scope: CredentialScope) {
        val event = delivery.event
        val repo = event.repo
        var heartbeatJob: Job? = null

        try {
            coroutineScope {
                val executionScope = this
                val hb = launch {
                    runHeartbeatLoop(delivery, scope, lockToken) {
                        executionScope.cancel(LostLockException(repo))
                    }
                }
                heartbeatJob = hb

                runExecution(delivery, lockToken, scope, hb)
            }
        } catch (@Suppress("SwallowedException") e: LostLockException) {
            withContext(NonCancellable) {
                runCatching { coordinationPort.releaseLock(repo, lockToken, config.queuedTtl) }
                delivery.nack(Duration.ZERO)
            }
        } catch (e: CancellationException) {
            withContext(NonCancellable) {
                runCatching { coordinationPort.releaseLock(repo, lockToken, config.queuedTtl) }
                delivery.nack(Duration.ZERO)
            }
            throw e
        } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") e: Exception) {
            withContext(NonCancellable) {
                runCatching { coordinationPort.releaseLock(repo, lockToken, config.queuedTtl) }
                delivery.nack(computeBackoffDelay(delivery.deliveryAttempt))
            }
        } finally {
            withContext(NonCancellable) {
                heartbeatJob?.cancelAndJoin()
                try {
                    throttlePort.releaseRunSlot(scope, lockToken)
                } finally {
                    runCatching { workspaceCleaner.cleanup(repo) }
                }
            }
        }
    }

    private suspend fun runExecution(
        delivery: JobDelivery,
        lockToken: String,
        scope: CredentialScope,
        heartbeatJob: Job,
    ) {
        val event = delivery.event
        val repo = event.repo

        // Credentials first: a rejected repo costs no cache download.
        val creds = try {
            credentialsPort.credentialsFor(repo)
        } catch (@Suppress("SwallowedException") _: RepositoryRejectedException) {
            // The credentials port logged why.
            handleRejected(delivery, lockToken, heartbeatJob)
            return
        }
        // The cache is the credential's (KSH-26), which needn't be the scope the job was queued in: a proxy without
        // GitHub App auth queues every GitHub job as `github:pat`.
        val cacheScope = creds.scope(repo.platform)
        restoreCache(repo, cacheScope)
        val result = cliExecutor.execute(repo, creds, config.cliTimeout)

        when (result) {
            is JobExecutionResult.Success,
            is JobExecutionResult.Failed,
            is JobExecutionResult.TimedOut,
            -> {
                handleTerminalResult(delivery, lockToken, scope, result, heartbeatJob, cacheScope)
            }

            is JobExecutionResult.RateLimited -> {
                handleRateLimited(delivery, lockToken, scope, result, heartbeatJob)
            }
        }
    }

    /** A failed upload only means a colder next run. */
    private suspend fun persistCache(repo: RepoRef, cacheScope: CredentialScope) {
        try {
            cachePort.persist(repo, cacheScope)
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") _: Exception) {
            // Keep the successful result
        }
    }

    /** A missing or unreadable cache only means a cold run. */
    private suspend fun restoreCache(repo: RepoRef, cacheScope: CredentialScope) {
        try {
            cachePort.restore(repo, cacheScope)
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") _: Exception) {
            // Run cold
        }
    }

    private suspend fun runHeartbeatLoop(
        delivery: JobDelivery,
        scope: CredentialScope,
        lockToken: String,
        onLostLock: () -> Unit,
    ) {
        var lastSuccessfulRenewal = timeSource.markNow()
        val toleranceWindow = config.lockTtl - config.heartbeatInterval

        while (true) {
            delay(config.heartbeatInterval)
            when (performHeartbeatStep(delivery, scope, lockToken)) {
                HeartbeatStepResult.SUCCESS -> {
                    lastSuccessfulRenewal = timeSource.markNow()
                }

                HeartbeatStepResult.LOST -> {
                    onLostLock()
                    return
                }

                HeartbeatStepResult.TRANSIENT_ERROR -> {
                    if (lastSuccessfulRenewal.elapsedNow() >= toleranceWindow) {
                        onLostLock()
                        return
                    }
                }
            }
        }
    }

    private suspend fun performHeartbeatStep(
        delivery: JobDelivery,
        scope: CredentialScope,
        lockToken: String,
    ): HeartbeatStepResult {
        val repo = delivery.event.repo
        try {
            delivery.heartbeat()
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") _: Exception) {
            // Broker blip is tolerated; do not let broker heartbeat failure block lock renewal
        }

        return try {
            val lockExtended = coordinationPort.extendLock(repo, lockToken, config.lockTtl)
            if (!lockExtended) {
                HeartbeatStepResult.LOST
            } else {
                val slotExtended = throttlePort.extendRunSlot(scope, lockToken, config.lockTtl)
                if (!slotExtended) HeartbeatStepResult.LOST else HeartbeatStepResult.SUCCESS
            }
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") e: Exception) {
            HeartbeatStepResult.TRANSIENT_ERROR
        }
    }

    @Suppress("LongParameterList")
    private suspend fun handleTerminalResult(
        delivery: JobDelivery,
        lockToken: String,
        scope: CredentialScope,
        result: JobExecutionResult,
        heartbeatJob: Job,
        cacheScope: CredentialScope,
    ) {
        val repo = delivery.event.repo

        if (result is JobExecutionResult.Success) {
            persistCache(repo, cacheScope)
        }

        heartbeatJob.cancelAndJoin()

        // A consumed dirty flag marks the repo queued for the follow-up published below
        val release = coordinationPort.releaseLockAndTakeDirty(repo, lockToken, config.queuedTtl)

        when (release) {
            ReleaseResult.DIRTY -> {
                publishDirtyFollowUp(delivery.event)
                if (result is JobExecutionResult.Success) {
                    throttlePort.resetBackoff(scope)
                }
                delivery.ack()
            }

            ReleaseResult.CLEAN -> {
                if (result is JobExecutionResult.Success) {
                    throttlePort.resetBackoff(scope)
                }
                delivery.ack()
            }

            ReleaseResult.NOT_OWNER -> {
                delivery.nack(Duration.ZERO)
            }
        }
    }

    /** Final: ACKed without a retry, and a dirty flag is dropped rather than turned into a follow-up run. */
    private suspend fun handleRejected(delivery: JobDelivery, lockToken: String, heartbeatJob: Job) {
        heartbeatJob.cancelAndJoin()
        coordinationPort.releaseLockAndTakeDirty(delivery.event.repo, lockToken)
        delivery.ack()
    }

    /** The follow-up runs as the same installation, so it's queued in the same scope. */
    private suspend fun publishDirtyFollowUp(event: RenovateJobEvent) {
        queuePublisher.publish(
            RenovateJobEvent(
                repo = event.repo,
                deliveryId = uuidGenerator(),
                triggerReason = TriggerReason.DIRTY_FOLLOWUP,
                rateLimitAttempt = 0,
                installationId = event.installationId,
            ),
        )
    }

    private suspend fun handleRateLimited(
        delivery: JobDelivery,
        lockToken: String,
        scope: CredentialScope,
        result: JobExecutionResult.RateLimited,
        heartbeatJob: Job,
    ) {
        val event = delivery.event
        val repo = event.repo

        heartbeatJob.cancelAndJoin()

        val requeue = event.rateLimitAttempt < config.maxRateLimitRequeues
        val released = coordinationPort.releaseLock(repo, lockToken, config.queuedTtl.takeIf { requeue })
        if (!released) {
            delivery.nack(Duration.ZERO)
            return
        }

        throttlePort.registerRateLimit(scope, result.retryAfter)
        if (requeue) {
            queuePublisher.publish(
                RenovateJobEvent(
                    repo = repo,
                    deliveryId = uuidGenerator(),
                    triggerReason = TriggerReason.RATE_LIMIT_REQUEUE,
                    rateLimitAttempt = event.rateLimitAttempt + 1,
                    installationId = event.installationId,
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

    private enum class HeartbeatStepResult {
        SUCCESS,
        LOST,
        TRANSIENT_ERROR,
    }
}
