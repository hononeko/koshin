package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.CliExecutorPort
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.security.KeyStore
import java.security.cert.CertificateFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import kotlin.io.path.Path
import kotlin.io.path.inputStream
import kotlin.io.path.readText
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

fun interface PodDeletionCostUpdater {
    suspend fun update(cost: Int)
}

/** Patches `controller.kubernetes.io/pod-deletion-cost` on this pod through the in-cluster API. */
class KubernetesPodDeletionCostUpdater(
    podName: String,
    namespace: String,
    private val apiServer: URI = URI(DEFAULT_API_SERVER),
    private val tokenPath: Path = Path(SERVICE_ACCOUNT_DIR, "token"),
    private val httpClient: HttpClient = inClusterHttpClient(Path(SERVICE_ACCOUNT_DIR, "ca.crt")),
) : PodDeletionCostUpdater {

    private val podPath = "/api/v1/namespaces/${namespace.urlEncoded()}/pods/${podName.urlEncoded()}"

    override suspend fun update(cost: Int) {
        val body = """{"metadata":{"annotations":{"$ANNOTATION":"$cost"}}}"""
        val request = HttpRequest.newBuilder(apiServer.resolve(podPath))
            .timeout(REQUEST_TIMEOUT)
            .header("Authorization", "Bearer ${tokenPath.readText().trim()}")
            .header("Content-Type", "application/merge-patch+json")
            .method("PATCH", HttpRequest.BodyPublishers.ofString(body))
            .build()
        val response = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
        if (response.statusCode() !in SUCCESS_RANGE) {
            throw IOException("Pod patch failed with HTTP ${response.statusCode()}: ${response.body().take(MAX_ERROR)}")
        }
    }

    companion object {
        const val ANNOTATION = "controller.kubernetes.io/pod-deletion-cost"
        const val DEFAULT_API_SERVER = "https://kubernetes.default.svc"
        const val SERVICE_ACCOUNT_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
        private val REQUEST_TIMEOUT = 5.seconds.toJavaDuration()
        private val SUCCESS_RANGE = 200..299
        private const val MAX_ERROR = 200

        private fun String.urlEncoded(): String = URLEncoder.encode(this, Charsets.UTF_8)

        fun inClusterHttpClient(caPath: Path): HttpClient {
            val keyStore = KeyStore.getInstance(KeyStore.getDefaultType()).apply { load(null, null) }
            caPath.inputStream().use { input ->
                CertificateFactory.getInstance("X.509").generateCertificates(input).forEachIndexed { i, cert ->
                    keyStore.setCertificateEntry("k8s-ca-$i", cert)
                }
            }
            val trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                .apply { init(keyStore) }
                .trustManagers
            val sslContext = SSLContext.getInstance("TLS").apply { init(null, trustManagers, null) }
            return HttpClient.newBuilder().sslContext(sslContext).connectTimeout(REQUEST_TIMEOUT).build()
        }
    }
}

/** Marks the pod expensive to delete while Renovate runs, so KEDA scale-in prefers idle pods. Fails soft. */
class PodDeletionCostCliExecutor(
    private val delegate: CliExecutorPort,
    private val updater: PodDeletionCostUpdater,
    private val busyCost: Int = DEFAULT_BUSY_COST,
) : CliExecutorPort {

    private val logger = LoggerFactory.getLogger(PodDeletionCostCliExecutor::class.java)

    override suspend fun execute(repo: RepoRef, creds: PlatformCredentials, timeout: Duration): JobExecutionResult {
        trySet(busyCost)
        try {
            return delegate.execute(repo, creds, timeout)
        } finally {
            withContext(NonCancellable) { trySet(IDLE_COST) }
        }
    }

    private suspend fun trySet(cost: Int) {
        try {
            updater.update(cost)
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            logger.warn("Failed to set pod-deletion-cost to {}", cost, e)
        }
    }

    companion object {
        const val DEFAULT_BUSY_COST = 1000
        const val IDLE_COST = 0
    }
}
