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
import kotlin.time.TimeMark
import kotlin.time.TimeSource

/**
 * Runs one job at a time (1 job per pod). With a zero [idleTimeout] it runs until stopped (a warm or static worker);
 * otherwise it drains runnable jobs back to back and finishes once it found none for [idleTimeout] (a burst `Job`
 * pod). A positive [maxLifetime] finishes it after the job in flight once it's that old. Either way it then
 * calls [onFinished], for the process to exit 0.
 */
@Suppress("LongParameterList") // All but the platforms and the use case have defaults
class WorkerDaemon(
    private val platforms: Collection<Platform>,
    private val idleDelay: Duration = 1.seconds,
    private val errorDelay: Duration = 5.seconds,
    dispatcher: CoroutineDispatcher = Dispatchers.Default,
    private val idleTimeout: Duration = Duration.ZERO,
    private val maxLifetime: Duration = Duration.ZERO,
    private val timeSource: TimeSource = TimeSource.Monotonic,
    private val onFinished: () -> Unit = {},
    private val awaitUseCase: suspend () -> ProcessRenovateJobUseCase,
) {
    private val logger = LoggerFactory.getLogger(WorkerDaemon::class.java)
    private val scope = CoroutineScope(SupervisorJob() + dispatcher)
    private var job: Job? = null

    init {
        require(!idleTimeout.isNegative()) { "idleTimeout cannot be negative: $idleTimeout" }
        require(!maxLifetime.isNegative()) { "maxLifetime cannot be negative: $maxLifetime" }
    }

    @Synchronized
    fun start() {
        if (job == null) {
            job = scope.launch {
                loop()
                // The loop also returns once stop() cancels it between suspension points; that isn't finishing
                ensureActive()
                onFinished()
            }
        }
    }

    /** Returns once the daemon finishes on its own; cancelled by [stop]. */
    private suspend fun loop() {
        val startedAt = timeSource.markNow()
        val useCase = connect() ?: return
        logger.info(
            "Worker daemon polling platforms {} (idle timeout {}, max lifetime {})",
            platforms,
            idleTimeout.takeIf { it.isPositive() } ?: "none",
            maxLifetime.takeIf { it.isPositive() } ?: "none",
        )
        var idleSince = timeSource.markNow()
        while (currentCoroutineContext().isActive && !isDone(startedAt, idleSince)) {
            if (runIteration(useCase)) idleSince = timeSource.markNow()
        }
    }

    /** Past the max lifetime, or idle for the idle timeout: checked between jobs, never during one. */
    private fun isDone(startedAt: TimeMark, idleSince: TimeMark): Boolean = when {
        maxLifetime.isPositive() && startedAt.elapsedNow() >= maxLifetime -> {
            logger.info("Worker reached its max lifetime of {}; exiting", maxLifetime)
            true
        }

        idleTimeout.isPositive() && idleSince.elapsedNow() >= idleTimeout -> {
            logger.info("No runnable job for {}; exiting", idleTimeout)
            true
        }

        else -> false
    }

    /** Waiting for NATS and Valkey counts as idle: a burst worker that can't reach them gives up (`null`). */
    private suspend fun connect(): ProcessRenovateJobUseCase? {
        if (!idleTimeout.isPositive()) return awaitUseCase()
        val useCase = withTimeoutOrNull(idleTimeout) { awaitUseCase() }
        if (useCase == null) logger.warn("Not connected within {}; exiting", idleTimeout)
        return useCase
    }

    /** Whether it processed a job; waits a little before returning false. A failure isn't a job. */
    private suspend fun runIteration(useCase: ProcessRenovateJobUseCase): Boolean {
        try {
            if (useCase.processNext(platforms)) return true
            delay(idleDelay)
        } catch (e: CancellationException) {
            currentCoroutineContext().ensureActive()
            logger.warn("Job loop iteration was cancelled internally; retrying in {}", errorDelay, e)
            delay(errorDelay)
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            logger.error("Job loop iteration failed; retrying in {}", errorDelay, e)
            delay(errorDelay)
        }
        return false
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
