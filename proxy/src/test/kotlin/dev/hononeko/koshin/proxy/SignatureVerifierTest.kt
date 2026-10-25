package dev.hononeko.koshin.proxy

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.HexFormat
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class SignatureVerifierTest {

    private fun computeGitHubHmac(payload: ByteArray, secret: String): String {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(secret.toByteArray(Charsets.UTF_8), "HmacSHA256"))
        return "sha256=" + HexFormat.of().formatHex(mac.doFinal(payload))
    }

    @Test
    fun `verifyGitHubSignature accepts valid HMAC and rejects invalid or missing`() {
        val body = """{"repository":{"full_name":"hononeko/koshin"}}""".toByteArray(Charsets.UTF_8)
        val secret = "super-secret-webhook-key"
        val validSig = computeGitHubHmac(body, secret)
        val upperHexSig = validSig.uppercase().replace("SHA256=", "sha256=")

        assertTrue(SignatureVerifier.verifyGitHubSignature(body, validSig, secret))
        assertTrue(SignatureVerifier.verifyGitHubSignature(body, upperHexSig, secret))

        assertFalse(SignatureVerifier.verifyGitHubSignature(body, computeGitHubHmac(body, "wrong-secret"), secret))
        assertFalse(SignatureVerifier.verifyGitHubSignature(body, null, secret))
        assertFalse(SignatureVerifier.verifyGitHubSignature(body, validSig, null))
        assertFalse(SignatureVerifier.verifyGitHubSignature(body, validSig, "  "))
        assertFalse(SignatureVerifier.verifyGitHubSignature(body, "sha1=1234", secret))
        assertFalse(SignatureVerifier.verifyGitHubSignature(body, "sha256=short", secret))
        assertFalse(SignatureVerifier.verifyGitHubSignature(body, "sha256=" + "z".repeat(64), secret))
    }

    @Test
    fun `verifyGitLabToken performs constant-time comparison and rejects mismatches`() {
        assertTrue(SignatureVerifier.verifyGitLabToken("gl-secret-123", "gl-secret-123"))
        assertFalse(SignatureVerifier.verifyGitLabToken("wrong", "gl-secret-123"))
        assertFalse(SignatureVerifier.verifyGitLabToken("gl-secret-123-longer", "gl-secret-123"))
        assertFalse(SignatureVerifier.verifyGitLabToken(null, "gl-secret-123"))
        assertFalse(SignatureVerifier.verifyGitLabToken("", "gl-secret-123"))
        assertFalse(SignatureVerifier.verifyGitLabToken("gl-secret-123", null))
        assertFalse(SignatureVerifier.verifyGitLabToken("gl-secret-123", "  "))
    }

    @Test
    fun `verifyBearerToken validates Authorization Bearer header`() {
        assertTrue(SignatureVerifier.verifyBearerToken("Bearer internal-token-xyz", "internal-token-xyz"))
        assertTrue(SignatureVerifier.verifyBearerToken("bearer internal-token-xyz", "internal-token-xyz"))
        assertFalse(SignatureVerifier.verifyBearerToken("Basic internal-token-xyz", "internal-token-xyz"))
        assertFalse(SignatureVerifier.verifyBearerToken("Bearer wrong-token", "internal-token-xyz"))
        assertFalse(SignatureVerifier.verifyBearerToken("Bearer ", "internal-token-xyz"))
        assertFalse(SignatureVerifier.verifyBearerToken(null, "internal-token-xyz"))
        assertFalse(SignatureVerifier.verifyBearerToken("Bearer internal-token-xyz", null))
    }
}
