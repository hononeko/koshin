package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import io.nats.client.Connection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import kotlin.random.Random
import kotlin.time.Duration

/**
 * The work queue's upkeep, run by the always-on proxy so it works with no worker running: MaxDeliver
 * exhaustion → DLQ, through the advisory forwarder plus the sweeper for advisories nobody handled, and the
 * per-scope consumers: moving jobs off the legacy per-platform queue, deleting idle consumers and creating
 * missing ones.
 */
object NatsHousekeeping {
    private val logger = LoggerFactory.getLogger(NatsHousekeeping::class.java)

    /** Starts both on [connection], the periodic pass in [scope]; closing the result stops them. */
    @Suppress("LongParameterList") // All but the first three have defaults
    fun start(
        connection: Connection,
        config: NatsConfig,
        scope: CoroutineScope,
        platforms: Collection<Platform> = Platform.entries,
        ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
        onDeadLettered: (RenovateJobEvent) -> Unit = {},
    ): AutoCloseable {
        val forwarder = MaxDeliverDlqForwarder(connection, config, onDeadLettered = onDeadLettered)
        val pass = if (config.exhaustedSweepInterval.isPositive()) {
            val jsm = connection.jetStreamManagement()
            val consumers = NatsScopeConsumers(jsm, config, connection.jetStream())
            val sweeper = ExhaustedMessageSweeper(jsm, config, ioDispatcher = ioDispatcher) { seq ->
                forwarder.moveToDeadLetter(seq, config.maxDeliver, MaxDeliverDlqForwarder.SOURCE_SWEEP)
            }
            periodically(scope, config.exhaustedSweepInterval) {
                withContext(ioDispatcher) { tidy(consumers, platforms) }
                sweeper.sweep()
            }
        } else {
            null
        }
        return AutoCloseable {
            pass?.cancel()
            forwarder.close()
        }
    }

    /**
     * The scope consumers' upkeep, each step on its own: one failing doesn't stop the others, and the next pass
     * retries it.
     */
    fun tidy(consumers: NatsScopeConsumers, platforms: Collection<Platform> = Platform.entries) {
        step("move jobs off the per-platform queues") { consumers.migrateLegacy(platforms) }
        step("delete idle consumers") { consumers.collectIdle() }
        step("create missing consumers") { consumers.ensureForStoredJobs() }
    }

    private inline fun step(name: String, action: () -> Unit) {
        try {
            action()
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            logger.warn("Could not {}; the next pass retries", name, e)
        }
    }

    /** Runs [pass] every [interval], starting after a random part of it so proxy replicas don't all run at once. */
    internal fun periodically(scope: CoroutineScope, interval: Duration, pass: suspend () -> Unit): Job = scope.launch {
        delay(interval * Random.nextDouble())
        while (isActive) {
            try {
                pass()
            } catch (e: CancellationException) {
                throw e
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("NATS housekeeping failed; retrying in {}", interval, e)
            }
            delay(interval)
        }
    }
}
