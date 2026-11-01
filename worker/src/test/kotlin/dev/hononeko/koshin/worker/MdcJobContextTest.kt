package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.slf4j.MDC

@Suppress("InjectDispatcher")
class MdcJobContextTest {
    private val event = RenovateJobEvent(RepoRef(Platform.GITHUB, "hononeko/koshin"), "d-1", TriggerReason.WEBHOOK)

    @Test
    fun `the job's fields are in the MDC for everything the job does`() = runBlocking {
        val seen = mutableListOf<Map<String, String>?>()

        MdcJobContext.within(event, 2) {
            seen += MDC.getCopyOfContextMap()
            withContext(Dispatchers.IO) { seen += MDC.getCopyOfContextMap() }
            coroutineScope { launch(Dispatchers.Default) { seen += MDC.getCopyOfContextMap() } }
        }

        val expected = mapOf(
            "repo" to "github:hononeko/koshin",
            "deliveryId" to "d-1",
            "triggerReason" to "WEBHOOK",
            "deliveryAttempt" to "2",
        )
        assertEquals(listOf(expected, expected, expected), seen)
        assertNull(MDC.get("repo"), "the MDC is restored afterwards")
    }
}
