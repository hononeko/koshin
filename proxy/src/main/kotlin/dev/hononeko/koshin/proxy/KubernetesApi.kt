package dev.hononeko.koshin.proxy

import io.ktor.client.HttpClient
import io.ktor.client.engine.java.Java
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.bearerAuth
import io.ktor.client.request.get
import io.ktor.client.request.parameter
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import java.io.IOException
import java.nio.file.Path
import java.security.KeyStore
import java.security.cert.CertificateFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager
import kotlin.io.path.Path
import kotlin.io.path.inputStream
import kotlin.io.path.readText

class KubernetesApiException(val status: Int, message: String) : IOException(message)

/**
 * Just enough of the Kubernetes API for the worker dispatcher: JSON in and out, authenticated with the ServiceAccount
 * token, read afresh for every request since the kubelet rotates it.
 */
class KubernetesApi(private val client: HttpClient, private val baseUrl: String, private val token: () -> String) :
    AutoCloseable {

    suspend fun get(path: String, query: Map<String, String> = emptyMap()): JsonObject = client.get("$baseUrl$path") {
        authenticate()
        query.forEach { (name, value) -> parameter(name, value) }
    }.json()

    suspend fun post(path: String, body: JsonObject): JsonObject = client.post("$baseUrl$path") {
        authenticate()
        contentType(ContentType.Application.Json)
        setBody(body.toString())
    }.json()

    private fun HttpRequestBuilder.authenticate() = bearerAuth(token())

    private suspend fun HttpResponse.json(): JsonObject {
        val text = bodyAsText()
        if (!status.isSuccess()) {
            val request = "${call.request.method.value} ${call.request.url}"
            throw KubernetesApiException(status.value, "$request: $status $text")
        }
        return ProxyJson.parseToJsonElement(text).jsonObject
    }

    override fun close() = client.close()

    companion object {
        const val SERVICE_ACCOUNT_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
        private const val REQUEST_TIMEOUT_MILLIS = 10_000L

        /** The API server of the cluster this pod runs in, trusting its CA, as the pod's ServiceAccount. */
        fun inCluster(
            env: Map<String, String> = System.getenv(),
            serviceAccountDir: Path = Path(SERVICE_ACCOUNT_DIR),
        ): KubernetesApi {
            val host = requireNotNull(env["KUBERNETES_SERVICE_HOST"]?.takeIf { it.isNotBlank() }) {
                "KUBERNETES_SERVICE_HOST is not set: the worker dispatcher only runs in a Kubernetes pod"
            }
            val port = env["KUBERNETES_SERVICE_PORT"]?.takeIf { it.isNotBlank() } ?: "443"
            val tls = SSLContext.getInstance("TLS").apply {
                init(null, arrayOf(trustManagerFor(serviceAccountDir.resolve("ca.crt"))), null)
            }
            // The JDK's TLS verifies the API server's certificate against the IP in KUBERNETES_SERVICE_HOST
            val client = HttpClient(Java) {
                engine { config { sslContext(tls) } }
                install(HttpTimeout) { requestTimeoutMillis = REQUEST_TIMEOUT_MILLIS }
            }
            val tokenFile = serviceAccountDir.resolve("token")
            return KubernetesApi(client, "https://${bracketed(host)}:$port") { tokenFile.readText().trim() }
        }

        /** The namespace of this pod's ServiceAccount. */
        fun inClusterNamespace(serviceAccountDir: Path = Path(SERVICE_ACCOUNT_DIR)): String =
            serviceAccountDir.resolve("namespace").readText().trim()

        private fun bracketed(host: String) = if (':' in host) "[$host]" else host

        internal fun trustManagerFor(caFile: Path): X509TrustManager {
            val factory = CertificateFactory.getInstance("X.509")
            val certificates = caFile.inputStream().use { factory.generateCertificates(it) }
            require(certificates.isNotEmpty()) { "No certificate in $caFile" }
            val keyStore = KeyStore.getInstance(KeyStore.getDefaultType()).apply {
                load(null, null)
                certificates.forEachIndexed { i, certificate -> setCertificateEntry("ca-$i", certificate) }
            }
            val trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            trust.init(keyStore)
            return trust.trustManagers.filterIsInstance<X509TrustManager>().single()
        }
    }
}
