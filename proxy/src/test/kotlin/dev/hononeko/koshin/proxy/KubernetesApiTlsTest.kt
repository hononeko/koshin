package dev.hononeko.koshin.proxy

import com.sun.net.httpserver.HttpsConfigurator
import com.sun.net.httpserver.HttpsServer
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.file.Path
import java.security.KeyStore
import java.util.concurrent.TimeUnit
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import kotlin.io.path.Path
import kotlin.io.path.inputStream
import kotlin.io.path.writeText

/** The in-cluster client against a real TLS server addressed by IP, like the API server (`KUBERNETES_SERVICE_HOST`). */
class KubernetesApiTlsTest {
    private val password = "changeit".toCharArray()

    /** A self-signed certificate for 127.0.0.1 only, made with the running JDK's keytool. */
    private fun keytool(vararg args: String) {
        val java = Path(ProcessHandle.current().info().command().orElseThrow())
        val process = ProcessBuilder(listOf(java.resolveSibling("keytool").toString()) + args)
            .redirectErrorStream(true)
            .start()
        check(process.waitFor(60, TimeUnit.SECONDS) && process.exitValue() == 0) {
            process.inputStream.bufferedReader().readText()
        }
    }

    private fun certificate(dir: Path, name: String): Path {
        val keyStore = dir.resolve("$name.p12")
        keytool(
            "-genkeypair", "-alias", name, "-keyalg", "EC", "-groupname", "secp256r1", "-validity", "2",
            "-dname", "CN=$name", "-ext", "san=ip:127.0.0.1", "-storetype", "PKCS12",
            "-keystore", keyStore.toString(), "-storepass", String(password),
        )
        keytool(
            "-exportcert", "-rfc", "-alias", name, "-keystore", keyStore.toString(), "-storepass", String(password),
            "-file", dir.resolve("$name.crt").toString(),
        )
        return keyStore
    }

    private fun server(keyStore: Path): HttpsServer {
        val keys = KeyStore.getInstance("PKCS12").apply { keyStore.inputStream().use { load(it, password) } }
        val keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
            .apply { init(keys, password) }
            .keyManagers
        val tls = SSLContext.getInstance("TLS").apply { init(keyManagers, null, null) }
        return HttpsServer.create(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0).apply {
            httpsConfigurator = HttpsConfigurator(tls)
            createContext("/api/v1/namespaces/koshin/podtemplates/koshin-worker") { exchange ->
                val auth = exchange.requestHeaders.getFirst("Authorization").orEmpty()
                val body = """{"metadata":{"uid":"u-1"},"auth":"$auth"}""".toByteArray()
                exchange.responseHeaders.add("Content-Type", "application/json")
                exchange.sendResponseHeaders(200, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            }
            start()
        }
    }

    @Test
    fun `trusts the cluster CA for an API server addressed by IP, and nothing else`(@TempDir dir: Path) {
        val keyStore = certificate(dir, "apiserver")
        certificate(dir, "other")
        val server = server(keyStore)
        try {
            val env = mapOf(
                "KUBERNETES_SERVICE_HOST" to "127.0.0.1",
                "KUBERNETES_SERVICE_PORT" to server.address.port.toString(),
            )
            val serviceAccount = dir.resolve("sa").toFile().apply { mkdirs() }.toPath()
            dir.resolve("apiserver.crt").toFile().copyTo(serviceAccount.resolve("ca.crt").toFile())
            serviceAccount.resolve("token").writeText("sa-token\n")

            KubernetesApi.inCluster(env, serviceAccount).use { api ->
                val template = runBlocking { api.get("/api/v1/namespaces/koshin/podtemplates/koshin-worker") }
                assertEquals("Bearer sa-token", template["auth"]!!.jsonPrimitive.content)
            }

            // A CA that didn't sign the server's certificate
            val untrusting = dir.resolve("untrusting").toFile().apply { mkdirs() }.toPath()
            dir.resolve("other.crt").toFile().copyTo(untrusting.resolve("ca.crt").toFile())
            untrusting.resolve("token").writeText("sa-token")
            KubernetesApi.inCluster(env, untrusting).use { api ->
                assertThrows<Exception> {
                    runBlocking { api.get("/api/v1/namespaces/koshin/podtemplates/koshin-worker") }
                }
            }
        } finally {
            server.stop(0)
        }
    }
}
