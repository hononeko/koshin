package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.QueueDepth
import io.nats.client.Nats
import io.nats.client.api.StorageType
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class NatsQueueDepthIntegrationTest {
    private val config = NatsConfig(
        jobsStreamName = "DEPTH_JOBS",
        dlqStreamName = "DEPTH_DLQ",
        jobsSubjectPrefix = "depth.jobs",
        dlqSubjectPrefix = "depth.dlq",
        storageType = StorageType.Memory,
    )

    @Test
    fun `counts pending and delivered-but-unsettled jobs per credential scope`() = runBlocking {
        Nats.connect(SharedNatsContainer.natsUrl).use { connection ->
            val jsm = connection.jetStreamManagement()
            val depth = NatsQueueDepth(jsm, config)
            val github = CredentialScope.Token(Platform.GITHUB)
            assertEquals(emptyMap<CredentialScope, QueueDepth>(), depth.depths(Platform.entries), "no stream yet")

            NatsJetStreamBootstrap(jsm, config).bootstrap()
            val publisher = NatsJobQueuePublisher(connection, config)
            repeat(3) { i ->
                publisher.publish(
                    RenovateJobEvent(RepoRef(Platform.GITHUB, "hononeko/r$i"), "d-$i", TriggerReason.WEBHOOK),
                )
            }
            NatsJobQueueConsumer(connection, config).use { consumer ->
                checkNotNull(consumer.fetchNext(github, 5.seconds))
                var seen = depth.depths(Platform.entries)[github]
                repeat(50) {
                    if (seen == QueueDepth(2, 1)) return@repeat
                    delay(20.milliseconds)
                    seen = depth.depths(Platform.entries)[github]
                }
                assertEquals(QueueDepth(2, 1), seen)
            }
            assertEquals(emptyMap<CredentialScope, QueueDepth>(), depth.depths(listOf(Platform.GITLAB)), "no GitLab")
            jsm.deleteStream(config.jobsStreamName)
            jsm.deleteStream(config.dlqStreamName)
        }
    }
}
