package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import io.nats.client.Connection
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers

/**
 * MaxDeliver exhaustion → DLQ: the advisory forwarder, plus the sweeper for advisories nobody handled (KSH-18).
 * Run by the always-on proxy (KSH-30), so it works with no worker running.
 */
object NatsDlqHousekeeping {
    /** Starts both on [connection], the sweeper in [scope]; closing the result stops them. */
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
        val sweep = if (config.exhaustedSweepInterval.isPositive()) {
            ExhaustedMessageSweeper(
                connection.jetStreamManagement(),
                config,
                platforms,
                ioDispatcher = ioDispatcher,
            ) { seq ->
                forwarder.moveToDeadLetter(seq, config.maxDeliver, MaxDeliverDlqForwarder.SOURCE_SWEEP)
            }.start(scope, config.exhaustedSweepInterval)
        } else {
            null
        }
        return AutoCloseable {
            sweep?.cancel()
            forwarder.close()
        }
    }
}
