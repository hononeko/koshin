package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.usecase.IngestObserver
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

/** Logs ingest publishing without coalescing (fail-open), counting how often it happened. */
class LoggingIngestObserver : IngestObserver {
    private val logger = LoggerFactory.getLogger(LoggingIngestObserver::class.java)
    private val failOpenCounter = AtomicLong()

    val failOpenCount: Long
        get() = failOpenCounter.get()

    override fun failedOpen(repo: RepoRef, cause: Throwable?) {
        val count = failOpenCounter.incrementAndGet()
        logger.atWarn().addKeyValue("failOpenCount", count).setCause(cause).log(
            "Valkey {} while coalescing {}; publishing it without coalescing",
            if (cause == null) "timed out" else "failed",
            repo,
        )
    }

    override fun queuedMarkKept(repo: RepoRef, cause: Throwable?) {
        logger.atError().setCause(cause).log(
            "Could not clear the queued mark of {} after its publish failed: its webhooks are dropped until " +
                "the mark expires (QUEUED_TTL_SECONDS)",
            repo,
        )
    }
}
