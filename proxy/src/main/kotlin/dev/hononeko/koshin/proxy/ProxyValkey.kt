package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.adapters.valkey.ValkeyConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyLeaderLeaseAdapter
import dev.hononeko.koshin.adapters.valkey.ValkeyPlatformThrottleAdapter
import dev.hononeko.koshin.adapters.valkey.ValkeyRepoCoordinationAdapter
import dev.hononeko.koshin.adapters.valkey.ValkeyWarmWorkerAdapter
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.LeaderLeasePort
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.ports.ReleaseResult
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import dev.hononeko.koshin.core.ports.WarmWorkerPort
import io.lettuce.core.ClientOptions
import io.lettuce.core.RedisClient
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * The proxy's coordination Valkey, for pre-queue coalescing (KSH-30). It connects in the background, retrying; until
 * then, and while Lettuce is reconnecting (e.g. during a Sentinel failover), every call fails at once, so ingest fails
 * open instead of waiting.
 */
class ProxyValkey(
    private val retryDelay: Duration = 2.seconds,
    private val clearTimeout: Duration = 5.seconds,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val connect: suspend () -> Connection,
) : AutoCloseable {
    class Connection(
        val coordination: RepoCoordinationPort,
        val dispatch: DispatchPorts? = null,
        private val onClose: () -> Unit = {},
    ) : AutoCloseable {
        override fun close() = onClose()
    }

    /** What the worker dispatcher reads from Valkey (KSH-32). */
    class DispatchPorts(
        val leader: LeaderLeasePort,
        val throttle: PlatformThrottlePort,
        val warmWorkers: WarmWorkerPort,
    )

    private val logger = LoggerFactory.getLogger(ProxyValkey::class.java)
    private val scope = CoroutineScope(SupervisorJob() + ioDispatcher)
    private val connection = AtomicReference<Connection?>(null)

    val isConnected: Boolean
        get() = connection.get() != null

    /** The dispatcher's ports, or `null` while not connected. */
    fun dispatchPorts(): DispatchPorts? = connection.get()?.dispatch

    /** Fails with [IllegalStateException] until connected. */
    val coordination: RepoCoordinationPort = object : RepoCoordinationPort {
        private fun port() = checkNotNull(connection.get()) { "Valkey is not connected yet" }.coordination

        override suspend fun enqueueOrCoalesce(repo: RepoRef, queuedTtl: Duration, dirtyTtl: Duration) =
            port().enqueueOrCoalesce(repo, queuedTtl, dirtyTtl)

        override suspend fun clearQueued(repo: RepoRef) = port().clearQueued(repo)

        override suspend fun acquireOrMarkDirty(
            repo: RepoRef,
            token: String,
            lockTtl: Duration,
            dirtyTtl: Duration,
        ): AcquireResult = port().acquireOrMarkDirty(repo, token, lockTtl, dirtyTtl)

        override suspend fun extendLock(repo: RepoRef, token: String, ttl: Duration) =
            port().extendLock(repo, token, ttl)

        override suspend fun releaseLockAndTakeDirty(
            repo: RepoRef,
            token: String,
            queuedTtl: Duration?,
        ): ReleaseResult = port().releaseLockAndTakeDirty(repo, token, queuedTtl)

        override suspend fun releaseLock(repo: RepoRef, token: String, queuedTtl: Duration?) =
            port().releaseLock(repo, token, queuedTtl)
    }

    fun startAsync(): Job = scope.launch {
        while (true) {
            try {
                connection.set(connect())
                logger.info("Connected to Valkey; coalescing webhooks before publishing")
                return@launch
            } catch (e: CancellationException) {
                throw e
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("Valkey not ready yet, retrying in {}; publishing without coalescing", retryDelay, e)
                delay(retryDelay)
            }
        }
    }

    /** Clears [repo]'s queued mark in the background, e.g. for a job just moved to the DLQ. */
    fun clearQueuedLater(repo: RepoRef) {
        scope.launch {
            // Otherwise the mark lasts until QUEUED_TTL_SECONDS
            try {
                withTimeoutOrNull(clearTimeout) { coordination.clearQueued(repo) }
                    ?: logger.warn("Timed out clearing the queued mark of dead-lettered {}", repo)
            } catch (e: CancellationException) {
                throw e
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("Could not clear the queued mark of dead-lettered {}", repo, e)
            }
        }
    }

    override fun close() {
        scope.cancel()
        connection.getAndSet(null)?.let { runCatching { it.close() } }
    }

    companion object {
        /** Rejects commands while disconnected rather than queueing them, so ingest never waits on a failover. */
        suspend fun connect(config: ValkeyConfig, ioDispatcher: CoroutineDispatcher = Dispatchers.IO): Connection {
            val client = RedisClient.create(config.toRedisUri())
            client.options = ClientOptions.builder()
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .build()
            try {
                val connection = withContext(ioDispatcher) { client.connect() }
                val coordination = ValkeyRepoCoordinationAdapter(connection, config)
                coordination.preloadScripts()
                val leader = ValkeyLeaderLeaseAdapter(connection, config).apply { preloadScripts() }
                val throttle = ValkeyPlatformThrottleAdapter(connection, config).apply { preloadScripts() }
                val warmWorkers = ValkeyWarmWorkerAdapter(connection, config).apply { preloadScripts() }
                return Connection(coordination, DispatchPorts(leader, throttle, warmWorkers)) {
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
