package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.StorageType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class NatsJetStreamBootstrapIntegrationTest {
    companion object {
        private lateinit var connection: Connection

        @BeforeAll
        @JvmStatic
        fun setUp() {
            connection = Nats.connect(Options.Builder().server(SharedNatsContainer.natsUrl).build())
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            connection.close()
        }
    }

    @Test
    fun `bootstrapper creates streams and consumers idempotently and applies updates`() {
        val jobsStream = "BOOTSTRAP_TEST_JOBS"
        val dlqStream = "BOOTSTRAP_TEST_DLQ"
        val config = NatsConfig(
            jobsStreamName = jobsStream,
            dlqStreamName = dlqStream,
            jobsSubjectPrefix = "bootstrap.jobs",
            dlqSubjectPrefix = "bootstrap.dlq",
            replicas = 1,
            duplicateWindow = 2.minutes,
            storageType = StorageType.Memory,
            ackWait = 30.seconds,
            maxDeliver = 3,
        )

        val jsm = connection.jetStreamManagement()
        val bootstrapper = NatsJetStreamBootstrap(jsm, config)

        // 1. Initial bootstrap
        bootstrapper.bootstrap(listOf(Platform.GITHUB, Platform.GITLAB))

        val jobsInfo = jsm.getStreamInfo(jobsStream)
        assertNotNull(jobsInfo)
        assertEquals(RetentionPolicy.WorkQueue, jobsInfo.configuration.retentionPolicy)
        assertEquals(listOf("bootstrap.jobs.>"), jobsInfo.configuration.subjects)
        assertEquals(Duration.ofMinutes(2), jobsInfo.configuration.duplicateWindow)

        val dlqInfo = jsm.getStreamInfo(dlqStream)
        assertNotNull(dlqInfo)
        assertEquals(RetentionPolicy.Limits, dlqInfo.configuration.retentionPolicy)
        assertEquals(listOf("bootstrap.dlq.>"), dlqInfo.configuration.subjects)

        val ghConsumer = jsm.getConsumerInfo(jobsStream, "koshin-workers-github")
        assertNotNull(ghConsumer)
        assertEquals("bootstrap.jobs.github", ghConsumer.consumerConfiguration.filterSubject)
        assertEquals(3L, ghConsumer.consumerConfiguration.maxDeliver)

        val glConsumer = jsm.getConsumerInfo(jobsStream, "koshin-workers-gitlab")
        assertNotNull(glConsumer)
        assertEquals("bootstrap.jobs.gitlab", glConsumer.consumerConfiguration.filterSubject)

        // 2. Second bootstrap is idempotent
        bootstrapper.bootstrap(listOf(Platform.GITHUB, Platform.GITLAB))
        assertEquals(jobsInfo.createTime, jsm.getStreamInfo(jobsStream).createTime)

        // 3. Update configuration
        val updatedConfig = config.copy(
            duplicateWindow = 5.minutes,
            maxDeliver = 7,
        )
        val updatedBootstrapper = NatsJetStreamBootstrap(jsm, updatedConfig)
        updatedBootstrapper.bootstrap(listOf(Platform.GITHUB, Platform.GITLAB))

        val updatedJobsInfo = jsm.getStreamInfo(jobsStream)
        assertEquals(Duration.ofMinutes(5), updatedJobsInfo.configuration.duplicateWindow)

        val updatedGhConsumer = jsm.getConsumerInfo(jobsStream, "koshin-workers-github")
        assertEquals(7L, updatedGhConsumer.consumerConfiguration.maxDeliver)
    }
}
