package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.nats.NatsHousekeeping
import dev.hononeko.koshin.adapters.nats.NatsJetStreamBootstrap
import dev.hononeko.koshin.adapters.nats.NatsJobQueuePublisher
import dev.hononeko.koshin.adapters.nats.NatsQueueDepth
import dev.hononeko.koshin.adapters.nats.NatsReconnectBootstrapper
import dev.hononeko.koshin.adapters.nats.NatsScopeConsumers
import dev.hononeko.koshin.adapters.nats.isConnected
import dev.hononeko.koshin.adapters.nats.slf4jLogging
import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.QueueDepth
import dev.hononeko.koshin.core.ports.QueueDepthPort
import io.nats.client.Connection
import io.nats.client.ConnectionListener
import io.nats.client.Nats
import io.nats.client.Options
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
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * The proxy's NATS connection: publishes jobs, keeps the streams and consumers in place (re-creating them after a
 * reconnect) and runs the DLQ housekeeping (KSH-30), so all of it works with no worker running.
 */
@Suppress("LongParameterList") // All but the first two have defaults
class ProxyNatsLifecycle(
    private val natsUrl: String,
    private val natsConfig: NatsConfig,
    private val retryDelay: Duration = 2.seconds,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val connectionFactory: (String, ConnectionListener) -> Connection = { url, listener ->
        val options = Options.Builder()
            .server(url)
            .maxReconnects(-1)
            .slf4jLogging(listener)
            .build()
        Nats.connect(options)
    },
    /** The streams, then the scope consumers' upkeep (KSH-24), every platform's whichever are enabled. */
    private val bootstrap: (Connection, NatsConfig) -> Unit = { conn, cfg ->
        NatsJetStreamBootstrap(conn, cfg).bootstrap()
        NatsHousekeeping.tidy(NatsScopeConsumers(conn.jetStreamManagement(), cfg, conn.jetStream()), Platform.entries)
    },
    private val publisherFactory: (Connection, NatsConfig) -> JobQueuePublisher = { conn, cfg ->
        NatsJobQueuePublisher(conn, cfg)
    },
    /** Called with each job moved to the DLQ. */
    private val onDeadLettered: (RenovateJobEvent) -> Unit = {},
    private val startHousekeeping: (Connection, CoroutineScope) -> AutoCloseable = { conn, scope ->
        NatsHousekeeping.start(conn, natsConfig, scope, ioDispatcher = ioDispatcher, onDeadLettered = onDeadLettered)
    },
) : JobQueuePublisher,
    AutoCloseable {

    private val logger = LoggerFactory.getLogger(ProxyNatsLifecycle::class.java)
    private val scope = CoroutineScope(SupervisorJob() + ioDispatcher)
    private val connectionRef = AtomicReference<Connection?>(null)
    private val publisherRef = AtomicReference<JobQueuePublisher?>(null)
    private val bootstrapped = AtomicBoolean(false)
    private val housekeeping = AtomicReference<AutoCloseable?>(null)
    private var initJob: Job? = null
    private val reconnectBootstrapper = NatsReconnectBootstrapper(scope) { conn -> bootstrap(conn, natsConfig) }

    fun startAsync() {
        initJob = scope.launch {
            while (isActive && !bootstrapped.get()) {
                try {
                    val conn = connectionRef.get() ?: withContext(ioDispatcher) {
                        connectionFactory(natsUrl, reconnectBootstrapper)
                    }.also { connectionRef.set(it) }

                    withContext(ioDispatcher) {
                        bootstrap(conn, natsConfig)
                    }
                    publisherRef.set(publisherFactory(conn, natsConfig))
                    housekeeping.compareAndSet(null, startHousekeeping(conn, scope))
                    bootstrapped.set(true)
                    logger.info(
                        "Connected to NATS and ensured JetStream streams ({}, {}) and consumers",
                        natsConfig.jobsStreamName,
                        natsConfig.dlqStreamName,
                    )
                } catch (e: CancellationException) {
                    throw e
                } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                    logger.warn("NATS connection/stream bootstrap not ready yet, retrying in {}...", retryDelay, e)
                    delay(retryDelay)
                }
            }
        }
    }

    /** The scope consumers' depths, for the worker dispatcher; fails until NATS is connected. */
    val queueDepth: QueueDepthPort = object : QueueDepthPort {
        override suspend fun depths(platforms: Collection<Platform>): Map<CredentialScope, QueueDepth> {
            val conn = connectionRef.get()?.takeIf { bootstrapped.get() }
                ?: throw IOException("NATS is not connected yet")
            return NatsQueueDepth(conn.jetStreamManagement(), natsConfig, ioDispatcher).depths(platforms)
        }
    }

    fun isReady(): Boolean {
        val conn = connectionRef.get() ?: return false
        return bootstrapped.get() && conn.isConnected()
    }

    override suspend fun publish(event: RenovateJobEvent) {
        val pub = publisherRef.get() ?: throw IOException("NATS publisher is not initialized yet")
        pub.publish(event)
    }

    override suspend fun publishToDeadLetter(event: RenovateJobEvent) {
        val pub = publisherRef.get() ?: throw IOException("NATS publisher is not initialized yet")
        pub.publishToDeadLetter(event)
    }

    override fun close() {
        runBlocking {
            initJob?.cancelAndJoin()
        }
        bootstrapped.set(false)
        publisherRef.set(null)
        housekeeping.getAndSet(null)?.let { runCatching { it.close() } }
        scope.cancel()
        connectionRef.getAndSet(null)?.let { conn ->
            runCatching { conn.close() }
        }
    }
}
