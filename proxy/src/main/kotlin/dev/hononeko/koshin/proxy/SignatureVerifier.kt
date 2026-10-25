package dev.hononeko.koshin.proxy

import java.security.MessageDigest
import java.util.HexFormat
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

object SignatureVerifier {
    private const val HMAC_SHA256_ALGORITHM = "HmacSHA256"
    private const val SHA256_PREFIX = "sha256="
    private const val SHA256_HEX_LENGTH = 64
    private const val BEARER_PREFIX = "Bearer "

    fun verifyGitHubSignature(payload: ByteArray, signatureHeader: String?, secret: String?): Boolean {
        val secretBytes = secret?.takeIf { it.isNotBlank() }?.toByteArray(Charsets.UTF_8)
        val providedBytes = extractGitHubSignatureBytes(signatureHeader)
        return if (secretBytes == null || providedBytes == null) {
            false
        } else {
            val mac = Mac.getInstance(HMAC_SHA256_ALGORITHM)
            mac.init(SecretKeySpec(secretBytes, HMAC_SHA256_ALGORITHM))
            val expectedBytes = mac.doFinal(payload)
            MessageDigest.isEqual(expectedBytes, providedBytes)
        }
    }

    fun verifyGitLabToken(providedToken: String?, expectedSecret: String?): Boolean =
        constantTimeTokenEquals(providedToken, expectedSecret)

    fun verifyBearerToken(authorizationHeader: String?, expectedToken: String?): Boolean {
        val header = authorizationHeader?.trim()
        val token = if (header != null && header.startsWith(BEARER_PREFIX, ignoreCase = true)) {
            header.substring(BEARER_PREFIX.length).trim()
        } else {
            null
        }
        return constantTimeTokenEquals(token, expectedToken)
    }

    private fun extractGitHubSignatureBytes(signatureHeader: String?): ByteArray? {
        val header = signatureHeader?.trim()
        val hexPart = if (header != null && header.startsWith(SHA256_PREFIX, ignoreCase = true)) {
            header.substring(SHA256_PREFIX.length)
        } else {
            null
        }
        return if (hexPart != null && hexPart.length == SHA256_HEX_LENGTH) {
            decodeHexOrNull(hexPart)
        } else {
            null
        }
    }

    private fun constantTimeTokenEquals(provided: String?, expected: String?): Boolean {
        val expectedBytes = expected?.takeIf { it.isNotBlank() }?.toByteArray(Charsets.UTF_8)
        val providedBytes = provided?.takeIf { it.isNotEmpty() }?.toByteArray(Charsets.UTF_8)
        return if (expectedBytes == null || providedBytes == null) {
            false
        } else {
            val expectedDigest = sha256(expectedBytes)
            val providedDigest = sha256(providedBytes)
            MessageDigest.isEqual(expectedDigest, providedDigest)
        }
    }

    private fun sha256(bytes: ByteArray): ByteArray = MessageDigest.getInstance("SHA-256").digest(bytes)

    private fun decodeHexOrNull(hex: String): ByteArray? = try {
        HexFormat.of().parseHex(hex)
    } catch (_: IllegalArgumentException) {
        null
    }
}
