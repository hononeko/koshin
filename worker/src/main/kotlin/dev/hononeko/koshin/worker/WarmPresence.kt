package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.ports.WarmWorkerPort
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * A daemon-mode worker's warm-worker lease: renewed every [interval] for [ttl], and dropped on [stop], so the
 * Job dispatcher counts this worker as ready for a single job while it runs.
 */
class WarmPresence(
    private val interval: Duration,
    private val ttl: Duration,
    private val workerId: String = UUID.randomUUID().toString(),
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val awaitPort: suspend () -> WarmWorkerPort,
) {
    private val logger = LoggerFactory.getLogger(WarmPresence::class.java)
    private val scope = CoroutineScope(SupervisorJob() + dispatcher)
    private var job: Job? = null

    @Volatile
    private var port: WarmWorkerPort? = null

    init {
        require(interval.isPositive() && ttl > interval) { "ttl ($ttl) must exceed the interval ($interval)" }
    }

    @Synchronized
    fun start() {
        if (job != null) return
        job = scope.launch {
            val leases = awaitPort().also { port = it }
            logger.info("Keeping warm-worker lease {} (renewed every {}, for {})", workerId, interval, ttl)
            while (isActive) {
                try {
                    leases.renew(workerId, ttl)
                } catch (e: CancellationException) {
                    throw e
                } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                    logger.warn("Could not renew warm-worker lease {}; retrying in {}", workerId, interval, e)
                }
                delay(interval)
            }
        }
    }

    /** Stops renewing and drops the lease, best effort: otherwise it expires after [ttl]. */
    fun stop(timeout: Duration = DEFAULT_STOP_TIMEOUT) {
        val current = synchronized(this) { job } ?: return
        runBlocking {
            current.cancelAndJoin()
            // Not connected yet: there's no lease
            val leases = port ?: return@runBlocking
            try {
                withTimeoutOrNull(timeout) { leases.remove(workerId) }
                    ?: logger.warn("Timed out dropping warm-worker lease {}", workerId)
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("Could not drop warm-worker lease {}; it expires after {}", workerId, ttl, e)
            }
        }
        scope.cancel()
    }

    companion object {
        val DEFAULT_STOP_TIMEOUT: Duration = 2.seconds
    }
}
