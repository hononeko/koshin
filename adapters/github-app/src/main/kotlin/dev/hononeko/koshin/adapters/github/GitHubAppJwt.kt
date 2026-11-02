package dev.hononeko.koshin.adapters.github

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import java.math.BigInteger
import java.security.KeyFactory
import java.security.interfaces.RSAPrivateKey
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaInstant

/** Signs the App's JWT (RS256), which authenticates the `/app` endpoints, e.g. minting installation tokens. */
class GitHubAppJwt(private val appId: String, privateKey: RSAPrivateKey, private val clock: Clock = Clock.System) {
    private val algorithm = Algorithm.RSA256(null, privateKey)

    fun create(): String {
        val now = clock.now()
        return JWT.create()
            .withIssuer(appId)
            .withIssuedAt((now - CLOCK_DRIFT).toJavaInstant())
            .withExpiresAt((now + LIFETIME).toJavaInstant())
            .sign(algorithm)
    }

    companion object {
        /** Issued in the past, as GitHub recommends, so a clock slightly ahead of GitHub's still works. */
        val CLOCK_DRIFT: Duration = 60.seconds

        /** GitHub accepts at most 10 minutes between `iat` and `exp`. */
        val LIFETIME: Duration = 9.minutes

        fun fromPem(appId: String, pem: String, clock: Clock = Clock.System) =
            GitHubAppJwt(appId, PemKeys.rsaPrivateKey(pem), clock)
    }
}

/** Reads an RSA private key in PKCS#1 (`BEGIN RSA PRIVATE KEY`, what GitHub hands out) or PKCS#8 PEM. */
internal object PemKeys {
    private val PEM = Regex("-----BEGIN ([A-Z ]+)-----(.+?)-----END \\1-----", RegexOption.DOT_MATCHES_ALL)

    /** `SEQUENCE { OID rsaEncryption, NULL }` */
    private val RSA_ALGORITHM = byteArrayOf(
        0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86.toByte(), 0x48, 0x86.toByte(), 0xf7.toByte(), 0x0d, 0x01, 0x01, 0x01, 0x05,
        0x00,
    )
    private val VERSION_0 = byteArrayOf(0x02, 0x01, 0x00)
    private const val SEQUENCE = 0x30
    private const val OCTET_STRING = 0x04
    private const val SHORT_LENGTH_LIMIT = 0x80

    fun rsaPrivateKey(pem: String): RSAPrivateKey {
        val match = requireNotNull(PEM.find(pem)) { "No PEM private key found" }
        val der = Base64.getMimeDecoder().decode(match.groupValues[2].trim())
        val pkcs8 = when (val type = match.groupValues[1]) {
            "PRIVATE KEY" -> der
            "RSA PRIVATE KEY" -> pkcs1ToPkcs8(der)
            else -> throw IllegalArgumentException("Unsupported PEM type '$type': expected an RSA private key")
        }
        return KeyFactory.getInstance("RSA").generatePrivate(PKCS8EncodedKeySpec(pkcs8)) as RSAPrivateKey
    }

    /** PKCS#8 `PrivateKeyInfo { version 0, rsaEncryption, OCTET STRING { PKCS#1 key } }` */
    fun pkcs1ToPkcs8(pkcs1: ByteArray): ByteArray = der(SEQUENCE, VERSION_0 + RSA_ALGORITHM + der(OCTET_STRING, pkcs1))

    private fun der(tag: Int, content: ByteArray): ByteArray {
        val header = byteArrayOf(tag.toByte()) + length(content.size)
        return header + content
    }

    private fun length(size: Int): ByteArray = if (size < SHORT_LENGTH_LIMIT) {
        byteArrayOf(size.toByte())
    } else {
        val bytes = BigInteger.valueOf(size.toLong()).toByteArray().dropWhile { it == 0.toByte() }
        byteArrayOf((SHORT_LENGTH_LIMIT or bytes.size).toByte()) + bytes
    }
}
