package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.usecase.ProcessRenovateJobUseCase
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Runs one job at a time (1 job per pod) until stopped. */
class WorkerDaemon(
    private val platforms: Collection<Platform>,
    private val idleDelay: Duration = 1.seconds,
    private val errorDelay: Duration = 5.seconds,
    dispatcher: CoroutineDispatcher = Dispatchers.Default,
    private val awaitUseCase: suspend () -> ProcessRenovateJobUseCase,
) {
    private val logger = LoggerFactory.getLogger(WorkerDaemon::class.java)
    private val scope = CoroutineScope(SupervisorJob() + dispatcher)
    private var job: Job? = null

    @Synchronized
    fun start() {
        if (job == null) {
            job = scope.launch { run() }
        }
    }

    private suspend fun run() {
        val useCase = awaitUseCase()
        logger.info("Worker daemon polling platforms {}", platforms)
        while (currentCoroutineContext().isActive) {
            try {
                if (!useCase.processNext(platforms)) {
                    delay(idleDelay)
                }
            } catch (e: CancellationException) {
                currentCoroutineContext().ensureActive()
                logger.warn("Job loop iteration was cancelled internally; retrying in {}", errorDelay, e)
                delay(errorDelay)
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.error("Job loop iteration failed; retrying in {}", errorDelay, e)
                delay(errorDelay)
            }
        }
    }

    /**
     * Stops fetching and cancels the in-flight job without waiting for the run to finish: the use case kills the
     * CLI, releases the lock (keeping the dirty flag), NAKs the delivery and releases the run slot.
     */
    fun stop(timeout: Duration = DEFAULT_STOP_TIMEOUT): Boolean {
        val current = synchronized(this) { job }
        val stopped = runBlocking {
            withTimeoutOrNull(timeout) { current?.cancelAndJoin() } != null || current == null
        }
        if (!stopped) {
            logger.warn("In-flight job did not stop within {}", timeout)
        }
        scope.cancel()
        return stopped
    }

    companion object {
        val DEFAULT_STOP_TIMEOUT: Duration = 20.seconds
    }
}
