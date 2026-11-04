package dev.hononeko.koshin.worker

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SecretRedactorTest {
    private val redactor = SecretRedactor()

    @Test
    fun `GitHub and GitLab tokens are redacted wherever they appear`() {
        val cases = mapOf(
            "Authorization: token ghs_fakefakefake0000" to "Authorization: token **redacted**",
            "ghp_abc and github_pat_11AB_cd" to "**redacted** and **redacted**",
            "PRIVATE-TOKEN: glpat-a1-B2_c3" to "PRIVATE-TOKEN: **redacted**",
            "remote x-access-token:v1.abc@github.com/o/r" to "remote x-access-token:**redacted**@github.com/o/r",
            "'x-access-token:a' \"x-access-token:b\"" to
                "'x-access-token:**redacted**' \"x-access-token:**redacted**\"",
            "no secrets: ghs_ glpat x-access-token:" to "no secrets: ghs_ glpat x-access-token:",
        )

        cases.forEach { (text, redacted) ->
            assertEquals(redacted, redactor.redact(text))
            assertEquals(text != redacted, redactor.containsSecret(text), text)
        }
    }

    @Test
    fun `a tracked token is redacted until every run using it is over`() {
        val token = "glgroup-0123456789"
        val first = redactor.track(token)
        val second = redactor.track(token)
        assertEquals("oauth2:**redacted**@", redactor.redact("oauth2:$token@"))

        first.close()
        first.close() // closing twice doesn't end the other run's tracking
        assertTrue(redactor.containsSecret(token))
        second.close()

        assertFalse(redactor.containsSecret(token))
        assertEquals(token, redactor.redact(token))
    }

    @Test
    fun `short values are never tracked, and the longest tracked token wins`() {
        redactor.track("short").use {
            redactor.track("token-12345678").use {
                redactor.track("token-12345678-and-more").use {
                    assertEquals("short **redacted**", redactor.redact("short token-12345678-and-more"))
                    assertEquals("**redacted**", redactor.redact("token-12345678"))
                }
            }
        }
    }

    @Test
    fun `the log masker rewrites string values with tokens and leaves the rest alone`() {
        val masker = SecretValueMasker()

        assertEquals("token **redacted**", masker.mask(mockk(), "token ghp_abc"))
        assertNull(masker.mask(mockk(), "nothing to hide"))
        assertNull(masker.mask(mockk(), 42))
    }
}
