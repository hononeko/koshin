package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.nats.NatsJetStreamBootstrap
import dev.hononeko.koshin.adapters.nats.NatsJobQueuePublisher
import dev.hononeko.koshin.adapters.nats.NatsReconnectBootstrapper
import dev.hononeko.koshin.adapters.nats.isConnected
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobQueuePublisher
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

class ProxyNatsLifecycle(
    private val natsUrl: String,
    private val natsConfig: NatsConfig,
    private val retryDelay: Duration = 2.seconds,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val connectionFactory: (String, ConnectionListener) -> Connection = { url, listener ->
        val options = Options.Builder()
            .server(url)
            .maxReconnects(-1)
            .connectionListener(listener)
            .build()
        Nats.connect(options)
    },
    private val bootstrapStreams: (Connection, NatsConfig) -> Unit = { conn, cfg ->
        val bootstrap = NatsJetStreamBootstrap(conn, cfg)
        bootstrap.ensureJobsStream()
        bootstrap.ensureDlqStream()
    },
    private val publisherFactory: (Connection, NatsConfig) -> JobQueuePublisher = { conn, cfg ->
        NatsJobQueuePublisher(conn, cfg)
    },
) : JobQueuePublisher,
    AutoCloseable {

    private val logger = LoggerFactory.getLogger(ProxyNatsLifecycle::class.java)
    private val scope = CoroutineScope(SupervisorJob() + ioDispatcher)
    private val connectionRef = AtomicReference<Connection?>(null)
    private val publisherRef = AtomicReference<JobQueuePublisher?>(null)
    private val bootstrapped = AtomicBoolean(false)
    private var initJob: Job? = null
    private val reconnectBootstrapper = NatsReconnectBootstrapper(scope) { conn -> bootstrapStreams(conn, natsConfig) }

    fun startAsync() {
        initJob = scope.launch {
            while (isActive && !bootstrapped.get()) {
                try {
                    val conn = connectionRef.get() ?: withContext(ioDispatcher) {
                        connectionFactory(natsUrl, reconnectBootstrapper)
                    }.also { connectionRef.set(it) }

                    withContext(ioDispatcher) {
                        bootstrapStreams(conn, natsConfig)
                    }
                    publisherRef.set(publisherFactory(conn, natsConfig))
                    bootstrapped.set(true)
                    logger.info(
                        "Connected to NATS and ensured JetStream streams ({}, {})",
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
        connectionRef.getAndSet(null)?.let { conn ->
            runCatching { conn.close() }
        }
    }
}
