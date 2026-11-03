package dev.hononeko.koshin.core.model

import dev.hononeko.koshin.core.ports.JobDelivery
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class DomainModelTest {

    @Test
    fun `RepoRef toString and validation`() {
        val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
        assertEquals("github:hononeko/koshin", repo.toString())

        // Valid repository names including dots
        assertEquals("hononeko/foo..bar", RepoRef(Platform.GITHUB, "hononeko/foo..bar").repository)
        val gitlabRepo = RepoRef(Platform.GITLAB, "gitlab-group/subgroup/project")
        assertEquals("gitlab-group/subgroup/project", gitlabRepo.repository)

        // Blank or whitespace
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "   ") }
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "owner/ repo") }
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "owner/\trepo") }
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "owner/\nrepo") }

        // Slashes
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "/owner/repo") }
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "owner/repo/") }
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "owner//repo") }

        // Path traversal / dot segments
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "../repo") }
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "owner/..") }
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "owner/.") }

        // Platform segment constraints
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "single-segment") }
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITHUB, "owner/repo/extra") }
        assertThrows(IllegalArgumentException::class.java) { RepoRef(Platform.GITLAB, "single-segment") }

        assertEquals(Platform.GITHUB, Platform.fromString("github"))
        assertEquals(Platform.GITLAB, Platform.fromString(" GITLAB "))
        assertThrows(IllegalArgumentException::class.java) { Platform.fromString("bitbucket") }
    }

    @Test
    fun `RenovateJobEvent validation`() {
        val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")

        assertThrows(IllegalArgumentException::class.java) {
            RenovateJobEvent(repo, "   ", TriggerReason.WEBHOOK)
        }

        assertThrows(IllegalArgumentException::class.java) {
            RenovateJobEvent(repo, "id-1", TriggerReason.WEBHOOK, rateLimitAttempt = -1)
        }
        assertThrows(IllegalArgumentException::class.java) {
            RenovateJobEvent(repo, "id-1", TriggerReason.WEBHOOK, installationId = 0)
        }
        assertThrows(IllegalArgumentException::class.java) {
            RenovateJobEvent(RepoRef(Platform.GITLAB, "g/p"), "id-1", TriggerReason.WEBHOOK, installationId = 1)
        }
    }

    @Test
    fun `a job's credential scope is its installation, or else its platform's token`() {
        val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")

        val installed = RenovateJobEvent(repo, "d", TriggerReason.WEBHOOK, installationId = 42)
        assertEquals(CredentialScope.Installation(42), installed.scope)
        assertEquals("github:inst-42", installed.scope.toString())
        assertEquals("inst-42", installed.scope.token)
        assertEquals(Platform.GITHUB, installed.scope.platform)

        val tokenScope = RenovateJobEvent(RepoRef(Platform.GITLAB, "g/p"), "d", TriggerReason.WEBHOOK).scope
        assertEquals(CredentialScope.Token(Platform.GITLAB), tokenScope)
        assertEquals("gitlab:pat", tokenScope.toString())
        assertEquals("pat", tokenScope.token)
    }

    @Test
    fun `an event without an installation reads and writes as before`() {
        val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
        val legacy = """{"repo":{"platform":"GITHUB","repository":"hononeko/koshin"},"deliveryId":"d",""" +
            """"triggerReason":"WEBHOOK"}"""

        val event = Json.decodeFromString(RenovateJobEvent.serializer(), legacy)

        assertEquals(RenovateJobEvent(repo, "d", TriggerReason.WEBHOOK), event)
        assertFalse(Json.encodeToString(RenovateJobEvent.serializer(), event).contains("installationId"))
        assertFalse(Json.encodeToString(RenovateJobEvent.serializer(), event).contains("scope"))
        val installed = event.copy(installationId = 7)
        assertEquals(installed, Json.decodeFromString(RenovateJobEvent.serializer(), Json.encodeToString(installed)))
    }

    @Test
    fun `credential scopes parse back from their tokens`() {
        assertEquals(CredentialScope.Installation(42), CredentialScope.parse(Platform.GITHUB, "inst-42"))
        assertEquals(CredentialScope.Token(Platform.GITLAB), CredentialScope.parse(Platform.GITLAB, "pat"))
        listOf("inst-0", "inst-007", "inst-x", "inst-", "other").forEach { token ->
            assertEquals(null, CredentialScope.parse(Platform.GITHUB, token), token)
        }
        assertEquals(null, CredentialScope.parse(Platform.GITLAB, "inst-42"), "GitLab has no installations")
        assertThrows(IllegalArgumentException::class.java) { CredentialScope.of(Platform.GITLAB, 42) }
        assertThrows(IllegalArgumentException::class.java) { CredentialScope.Installation(-1) }
    }

    @Test
    fun `a repo target's installation is only a GitHub one`() {
        val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
        assertEquals("github:hononeko/koshin (installation 42)", RepoTarget(repo, 42).toString())
        assertEquals("github:hononeko/koshin", RepoTarget(repo).toString())
        assertThrows(IllegalArgumentException::class.java) { RepoTarget(RepoRef(Platform.GITLAB, "g/p"), 1) }
        assertThrows(IllegalArgumentException::class.java) { RepoTarget(repo, 0) }
    }

    @Test
    fun `PlatformCredentials validation`() {
        assertThrows(IllegalArgumentException::class.java) {
            PlatformCredentials("   ")
        }
        assertThrows(IllegalArgumentException::class.java) {
            PlatformCredentials("token", endpoint = " ")
        }
        val creds = PlatformCredentials("s3cr3t", "https://api.github.com")
        assertFalse(creds.toString().contains("s3cr3t"))
        assertTrue(creds.toString().contains("https://api.github.com"))
    }

    @Test
    fun `JobExecutionResult hierarchy`() {
        val success: JobExecutionResult = JobExecutionResult.Success
        val failed: JobExecutionResult = JobExecutionResult.Failed(1)
        val timedOut: JobExecutionResult = JobExecutionResult.TimedOut
        val rateLimited: JobExecutionResult = JobExecutionResult.RateLimited(60.seconds)

        assertEquals(JobExecutionResult.Success, success)
        assertEquals(1, (failed as JobExecutionResult.Failed).exitCode)
        assertEquals(JobExecutionResult.TimedOut, timedOut)
        assertEquals(60.seconds, (rateLimited as JobExecutionResult.RateLimited).retryAfter)
    }

    @Test
    fun `JobDelivery default nack parameter`() = runTest {
        var nackDelay: Duration? = null
        val testDelivery = object : JobDelivery {
            override val event: RenovateJobEvent = RenovateJobEvent(
                RepoRef(Platform.GITHUB, "hononeko/koshin"),
                "del-1",
                TriggerReason.WEBHOOK,
            )
            override val deliveryAttempt: Int = 1
            override suspend fun ack() = Unit
            override suspend fun nack(delay: Duration) {
                nackDelay = delay
            }
            override suspend fun heartbeat() = Unit
        }
        testDelivery.nack()
        assertEquals(Duration.ZERO, nackDelay)
    }
}
