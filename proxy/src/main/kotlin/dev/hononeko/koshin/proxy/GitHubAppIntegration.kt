package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.RepositoryDiscoveryPort
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * The GitHub App's bot login (`<slug>[bot]`), whose webhooks are ignored. Looked up in the background, retrying, so
 * the proxy starts even while GitHub is unreachable.
 */
class AppBotLogin(
    private val fetch: suspend () -> String,
    private val firstRetry: Duration = 5.seconds,
    private val maxRetry: Duration = 5.minutes,
) {
    private val logger = LoggerFactory.getLogger(AppBotLogin::class.java)
    private val login = AtomicReference<String?>()

    /** The bot login, or `null` until it's known. */
    fun get(): String? = login.get()

    fun start(scope: CoroutineScope): Job = scope.launch {
        var wait = firstRetry
        while (login.get() == null) {
            try {
                login.set(fetch())
                logger.info("Ignoring webhooks sent by the GitHub App's bot {}", login.get())
            } catch (e: CancellationException) {
                throw e
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("Could not look up the GitHub App's bot login; retrying in {}", wait, e)
                delay(wait)
                wait = minOf(wait * 2, maxRetry)
            }
        }
    }
}

/** Repositories from every source, e.g. the GitHub App's installations and the static list (also GitLab's). */
class CompositeRepositoryDiscovery(private val sources: List<RepositoryDiscoveryPort>) : RepositoryDiscoveryPort {
    override suspend fun discover(): List<RepoRef> = sources.flatMap { it.discover() }.distinct()

    companion object {
        fun of(sources: List<RepositoryDiscoveryPort>): RepositoryDiscoveryPort? = when (sources.size) {
            0 -> null
            1 -> sources.single()
            else -> CompositeRepositoryDiscovery(sources)
        }
    }
}
