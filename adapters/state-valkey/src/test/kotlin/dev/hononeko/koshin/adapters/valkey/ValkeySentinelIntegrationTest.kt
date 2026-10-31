package dev.hononeko.koshin.adapters.valkey

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisConnectionException
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait

/**
 * A password-protected Sentinel, as the official Valkey chart deploys it. Sentinel hands out the master's container IP,
 * which the test JVM can only reach on a Linux Docker host.
 */
class ValkeySentinelIntegrationTest {
    companion object {
        private const val SENTINEL_PASSWORD = "sentinel-secret"
        private val network = Network.newNetwork()
        private val valkey = GenericContainer("valkey/valkey:9.1-alpine")
            .withNetwork(network)
            .withNetworkAliases("valkey")
        private val sentinel = GenericContainer("valkey/valkey:9.1-alpine")
            .withNetwork(network)
            .withExposedPorts(26379)
            .withCommand(
                "sh",
                "-c",
                "printf 'port 26379\\nsentinel resolve-hostnames yes\\nsentinel monitor mymaster valkey 6379 1\\n" +
                    "requirepass $SENTINEL_PASSWORD\\n' > /tmp/sentinel.conf && " +
                    "exec valkey-sentinel /tmp/sentinel.conf",
            )
            .waitingFor(Wait.forLogMessage(".*\\+monitor master mymaster.*", 1))

        private val sentinelUrl: String
            get() = "redis-sentinel://${sentinel.host}:${sentinel.getMappedPort(26379)}?sentinelMasterId=mymaster"

        @BeforeAll
        @JvmStatic
        fun setUp() {
            assumeTrue(System.getProperty("os.name").startsWith("Linux"), "needs a Linux Docker host")
            valkey.start()
            sentinel.start()
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            sentinel.stop()
            valkey.stop()
            network.close()
        }
    }

    @Test
    fun `connects to the master through a password-protected Sentinel`() {
        val config = ValkeyConfig(valkeyUrl = sentinelUrl, sentinelPassword = SENTINEL_PASSWORD)
        val client = RedisClient.create(config.toRedisUri())
        try {
            client.connect().use { connection ->
                connection.sync().set("koshin:sentinel-test", "ok")
                assertEquals("ok", connection.sync().get("koshin:sentinel-test"))
            }
        } finally {
            client.shutdown()
        }
    }

    @Test
    fun `fails without the Sentinel password`() {
        val client = RedisClient.create(ValkeyConfig(valkeyUrl = sentinelUrl).toRedisUri())
        try {
            assertThrows<RedisConnectionException> { client.connect().close() }
        } finally {
            client.shutdown()
        }
    }
}
