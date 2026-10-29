package dev.hononeko.koshin.worker

import dev.hononeko.koshin.adapters.nats.MaxDeliverDlqForwarder
import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.nats.NatsJetStreamBootstrap
import dev.hononeko.koshin.adapters.nats.NatsJobQueueConsumer
import dev.hononeko.koshin.adapters.nats.NatsJobQueuePublisher
import dev.hononeko.koshin.adapters.nats.NatsReconnectBootstrapper
import dev.hononeko.koshin.adapters.nats.isConnected
import dev.hononeko.koshin.adapters.valkey.PlatformThrottleConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyConnectionHealth
import dev.hononeko.koshin.adapters.valkey.ValkeyPlatformThrottleAdapter
import dev.hononeko.koshin.adapters.valkey.ValkeyRepoCoordinationAdapter
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.ports.JobQueueConsumer
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class NatsBackend(
    val consumer: JobQueueConsumer,
    val publisher: JobQueuePublisher,
    private val healthy: () -> Boolean,
    private val onClose: () -> Unit = {},
) : AutoCloseable {
    fun isHealthy(): Boolean = healthy()
    override fun close() = onClose()
}

class ValkeyBackend(
    val coordination: RepoCoordinationPort,
    val throttle: PlatformThrottlePort,
    private val healthy: suspend () -> Boolean,
    private val onClose: () -> Unit = {},
) : AutoCloseable {
    suspend fun isHealthy(): Boolean = healthy()
    override fun close() = onClose()
}

/** Connects NATS and Valkey in the background (retrying), so `/healthz` works before either is reachable. */
class WorkerBackends(
    private val connectNats: () -> NatsBackend,
    private val connectValkey: suspend () -> ValkeyBackend,
    private val retryDelay: Duration = 2.seconds,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {

    data class Ports(
        val consumer: JobQueueConsumer,
        val publisher: JobQueuePublisher,
        val coordination: RepoCoordinationPort,
        val throttle: PlatformThrottlePort,
    )

    private val logger = LoggerFactory.getLogger(WorkerBackends::class.java)
    private val scope = CoroutineScope(SupervisorJob() + ioDispatcher)
    private val nats = AtomicReference<NatsBackend?>(null)
    private val valkey = AtomicReference<ValkeyBackend?>(null)
    private val ready = CompletableDeferred<Ports>()
    private var initJob: Job? = null

    fun startAsync() {
        initJob = scope.launch {
            val natsBackend = retrying("NATS") { withContext(ioDispatcher) { connectNats() } }
            nats.set(natsBackend)
            val valkeyBackend = retrying("Valkey") { connectValkey() }
            valkey.set(valkeyBackend)
            ready.complete(
                Ports(natsBackend.consumer, natsBackend.publisher, valkeyBackend.coordination, valkeyBackend.throttle),
            )
            logger.info("Connected to NATS and Valkey")
        }
    }

    suspend fun awaitPorts(): Ports = ready.await()

    suspend fun isReady(): Boolean {
        val natsBackend = nats.get()
        val valkeyBackend = valkey.get()
        return ready.isCompleted &&
            natsBackend != null &&
            valkeyBackend != null &&
            natsBackend.isHealthy() &&
            valkeyBackend.isHealthy()
    }

    private suspend fun <T> retrying(name: String, connect: suspend () -> T): T {
        while (true) {
            try {
                return connect()
            } catch (e: CancellationException) {
                throw e
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("{} not ready yet, retrying in {}...", name, retryDelay, e)
                delay(retryDelay)
            }
        }
    }

    override fun close() {
        runBlocking { initJob?.cancelAndJoin() }
        ready.cancel()
        nats.getAndSet(null)?.let { runCatching { it.close() } }
        valkey.getAndSet(null)?.let { runCatching { it.close() } }
    }

    companion object {
        fun connectNats(
            url: String,
            config: NatsConfig,
            platforms: Collection<Platform>,
            ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
        ): NatsBackend {
            val reconnectScope = CoroutineScope(SupervisorJob() + ioDispatcher)
            val bootstrap = { conn: Connection -> NatsJetStreamBootstrap(conn, config).bootstrap(platforms) }
            val options = Options.Builder()
                .server(url)
                .maxReconnects(-1)
                .connectionListener(NatsReconnectBootstrapper(reconnectScope, bootstrap = bootstrap))
                .build()
            val connection = Nats.connect(options)
            try {
                bootstrap(connection)
                val forwarder = MaxDeliverDlqForwarder(connection, config)
                val consumer = NatsJobQueueConsumer(connection, config)
                return NatsBackend(
                    consumer = consumer,
                    publisher = NatsJobQueuePublisher(connection, config),
                    healthy = connection::isConnected,
                ) {
                    reconnectScope.cancel()
                    runCatching { consumer.close() }
                    runCatching { forwarder.close() }
                    connection.close()
                }
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                reconnectScope.cancel()
                runCatching { connection.close() }
                throw e
            }
        }

        @OptIn(ExperimentalLettuceCoroutinesApi::class)
        suspend fun connectValkey(
            config: ValkeyConfig,
            throttleConfig: PlatformThrottleConfig = PlatformThrottleConfig(),
            ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
        ): ValkeyBackend {
            val client = RedisClient.create(config.toRedisUri())
            try {
                val connection = withContext(ioDispatcher) { client.connect() }
                val coordination = ValkeyRepoCoordinationAdapter(connection, config)
                coordination.preloadScripts()
                val throttle = ValkeyPlatformThrottleAdapter(connection, config, throttleConfig)
                throttle.preloadScripts()
                val health = ValkeyConnectionHealth(connection, pingTimeout = config.commandTimeout)
                return ValkeyBackend(coordination, throttle, health::ping) {
                    runCatching { connection.close() }
                    client.shutdown()
                }
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                runCatching { client.shutdown() }
                throw e
            }
        }
    }
}
