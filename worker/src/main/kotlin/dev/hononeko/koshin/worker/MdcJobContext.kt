package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobContextPort
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.slf4j.MDC

/** Tags every log line of a job with `repo`, `deliveryId`, `triggerReason` and `deliveryAttempt`. */
object MdcJobContext : JobContextPort {
    override suspend fun within(event: RenovateJobEvent, deliveryAttempt: Int, block: suspend () -> Unit) {
        val context = MDC.getCopyOfContextMap().orEmpty() + mapOf(
            "repo" to event.repo.toString(),
            "deliveryId" to event.deliveryId,
            "triggerReason" to event.triggerReason.name,
            "deliveryAttempt" to deliveryAttempt.toString(),
        )
        withContext(MDCContext(context)) { block() }
    }
}
