package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.ports.JobQueuePublisher
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import io.mockk.mockk
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.deleteExisting
import kotlin.io.path.writeText

class EncryptionKeyTest {

    @TempDir
    lateinit var dir: Path

    private val armored = """
        -----BEGIN PGP PUBLIC KEY BLOCK-----

        mQGNBGcAAAABDADkoshin
        -----END PGP PUBLIC KEY BLOCK-----

    """.trimIndent()

    private fun keyFile(content: String = armored): Path = dir.resolve("public.asc").apply { writeText(content) }

    private fun get(key: EncryptionKey?, check: suspend (HttpStatusCode, String?, String) -> Unit) = testApplication {
        application { proxyModule(ProxyConfig(port = 0), mockk<JobQueuePublisher>(), encryptionKey = key) }
        val response = client.get("/api/v1/encryption-key")
        check(response.status, response.headers[HttpHeaders.ContentType], response.bodyAsText())
    }

    @Test
    fun `serves the armored public key`() = get(EncryptionKey.load(keyFile().toString())) { status, type, body ->
        assertEquals(HttpStatusCode.OK, status)
        assertEquals("application/pgp-keys; charset=utf-8", type)
        assertEquals(armored, body)
    }

    @Test
    fun `serves a key rotated after startup`() {
        val file = keyFile()
        val key = EncryptionKey.load(file.toString())
        val rotated = armored.replace("koshin", "rotated")
        file.writeText(rotated)

        get(key) { status, _, body ->
            assertEquals(HttpStatusCode.OK, status)
            assertEquals(rotated, body)
        }
    }

    @Test
    fun `404 without a key`() = get(null) { status, type, body ->
        assertEquals(HttpStatusCode.NotFound, status)
        assertTrue(type.orEmpty().startsWith("application/json"), "$type")
        assertEquals(ErrorResponse("No encryption key is configured"), Json.decodeFromString<ErrorResponse>(body))
    }

    @Test
    fun `503 when the key is gone`() {
        val file = keyFile()
        val key = EncryptionKey.load(file.toString())
        file.deleteExisting()

        get(key) { status, _, body ->
            assertEquals(HttpStatusCode.ServiceUnavailable, status)
            assertEquals("The encryption key is unavailable", Json.decodeFromString<ErrorResponse>(body).error)
        }
    }

    @Test
    fun `a missing or blank key fails at startup`() {
        val missing = assertThrows<IllegalArgumentException> { EncryptionKey.load(dir.resolve("none.asc").toString()) }
        assertTrue(missing.message!!.startsWith("Cannot read RENOVATE_PUBLIC_KEY_PATH"), missing.message)

        val blank = assertThrows<IllegalArgumentException> { EncryptionKey.load(keyFile(" \n").toString()) }
        assertTrue(blank.message!!.endsWith("is blank"), blank.message)
    }

    @Test
    fun `RENOVATE_PUBLIC_KEY_PATH is optional`() {
        assertNull(ProxyConfig.fromEnv(emptyMap()).publicKeyPath)
        assertNull(ProxyConfig.fromEnv(mapOf("RENOVATE_PUBLIC_KEY_PATH" to " ")).publicKeyPath)
        val configured = ProxyConfig.fromEnv(mapOf("RENOVATE_PUBLIC_KEY_PATH" to "/k/public.asc "))
        assertEquals("/k/public.asc", configured.publicKeyPath)
    }
}
