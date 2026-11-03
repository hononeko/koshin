package dev.hononeko.koshin.proxy

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.TextContent
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.io.path.copyTo
import kotlin.io.path.writeText
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

class KubernetesBurstWorkersTest {
    private val podTemplate = """
        {"apiVersion":"v1","kind":"PodTemplate",
         "metadata":{"name":"koshin-worker","namespace":"koshin","uid":"pt-uid-1","labels":{"app.kubernetes.io/component":"worker"}},
         "template":{
           "metadata":{"labels":{"app.kubernetes.io/component":"worker","app.kubernetes.io/instance":"koshin"},
                       "annotations":{"checksum/config":"abc"}},
           "spec":{"restartPolicy":"Never","serviceAccountName":"koshin-worker","automountServiceAccountToken":false,
                   "containers":[{"name":"worker","image":"ghcr.io/hononeko/koshin-worker:1.0.0",
                                  "env":[{"name":"WORKER_IDLE_TIMEOUT_SECONDS","value":"10"}]}]}}}
    """.trimIndent()

    private val jobs = """
        {"kind":"JobList","items":[
          {"metadata":{"name":"running"},"status":{"active":1}},
          {"metadata":{"name":"starting"}},
          {"metadata":{"name":"pending"},"status":{}},
          {"metadata":{"name":"done"},"status":{"succeeded":1}},
          {"metadata":{"name":"failed"},"status":{"failed":1}}]}
    """.trimIndent()

    private val requests = CopyOnWriteArrayList<HttpRequestData>()
    private val created = CopyOnWriteArrayList<JsonObject>()

    private fun MockRequestHandleScope.json(
        body: String,
        status: HttpStatusCode = HttpStatusCode.OK,
    ): HttpResponseData = respond(body, status, headersOf(HttpHeaders.ContentType, "application/json"))

    private fun api(handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData) = KubernetesApi(
        HttpClient(
            MockEngine { request ->
                requests += request
                handler(request)
            },
        ),
        "https://10.0.0.1:443",
    ) { "token-${requests.size}" }

    private val cluster = api { request ->
        when {
            request.url.encodedPath.endsWith("/podtemplates/koshin-worker") -> json(podTemplate)

            request.method == HttpMethod.Get -> json(jobs)

            else -> {
                val body = ProxyJson.parseToJsonElement((request.body as TextContent).text).jsonObject
                created += body
                json(body.toString(), HttpStatusCode.Created)
            }
        }
    }

    private val burstWorkers = KubernetesBurstWorkers(cluster, "koshin", "koshin-worker", 6.hours, 60.seconds)

    @Test
    fun `counts the template's Jobs that neither succeeded nor failed`() = runTest {
        assertEquals(3, burstWorkers.active())

        val list = requests.last()
        assertEquals("/apis/batch/v1/namespaces/koshin/jobs", list.url.encodedPath)
        assertEquals("koshin.hononeko.dev/pod-template-uid=pt-uid-1", list.url.parameters["labelSelector"])
        assertEquals("Bearer token-0", requests.first().headers[HttpHeaders.Authorization])
        assertEquals("Bearer token-1", list.headers[HttpHeaders.Authorization], "the token is read for every request")
    }

    @Test
    fun `starts Jobs that embed the PodTemplate's pod`() = runTest {
        burstWorkers.active()
        burstWorkers.start(2)

        assertEquals(2, created.size)
        assertEquals(2, requests.count { it.method == HttpMethod.Post })
        val job = created.first()
        assertEquals("batch/v1", job["apiVersion"]!!.jsonPrimitive.content)
        assertEquals("Job", job["kind"]!!.jsonPrimitive.content)
        val metadata = job["metadata"]!!.jsonObject
        assertEquals("koshin-worker-", metadata["generateName"]!!.jsonPrimitive.content)
        val labels = metadata["labels"]!!.jsonObject
        assertEquals("burst", labels["koshin.hononeko.dev/worker-mode"]!!.jsonPrimitive.content)
        assertEquals("pt-uid-1", labels["koshin.hononeko.dev/pod-template-uid"]!!.jsonPrimitive.content)
        assertEquals("koshin", labels["app.kubernetes.io/instance"]!!.jsonPrimitive.content)
        val owner = metadata["ownerReferences"]!!.jsonArray.single().jsonObject
        assertEquals("PodTemplate", owner["kind"]!!.jsonPrimitive.content)
        assertEquals("pt-uid-1", owner["uid"]!!.jsonPrimitive.content)

        val spec = job["spec"]!!.jsonObject
        assertEquals(0, spec["backoffLimit"]!!.jsonPrimitive.int)
        assertEquals(60L, spec["ttlSecondsAfterFinished"]!!.jsonPrimitive.long)
        assertEquals(21600L, spec["activeDeadlineSeconds"]!!.jsonPrimitive.long)
        val pod = spec["template"]!!.jsonObject
        assertEquals(labels, pod["metadata"]!!.jsonObject["labels"])
        val annotations = pod["metadata"]!!.jsonObject["annotations"]!!.jsonObject
        assertEquals("abc", annotations["checksum/config"]!!.jsonPrimitive.content)
        val template = ProxyJson.parseToJsonElement(podTemplate).jsonObject["template"]!!.jsonObject
        assertEquals(template["spec"], pod["spec"], "the pod spec is the PodTemplate's, unchanged")
    }

    @Test
    fun `starting without a count to start makes no call, and a template without a restart policy gets Never`() =
        runTest {
            burstWorkers.start(0)
            assertTrue(requests.isEmpty())

            val bare = ProxyJson.parseToJsonElement(
                """{"metadata":{"uid":"u"},"template":{"spec":{"containers":[]}}}""",
            ).jsonObject
            val job = burstWorkers.jobFor(bare)
            val podSpec = job["spec"]!!.jsonObject["template"]!!.jsonObject["spec"]!!.jsonObject
            assertEquals("Never", podSpec["restartPolicy"]!!.jsonPrimitive.content)
            assertThrows<IllegalArgumentException> { burstWorkers.jobFor(JsonObject(emptyMap())) }
        }

    @Test
    fun `a long template name keeps Job names within 63 characters`() {
        val name = "a".repeat(70)
        val workers = KubernetesBurstWorkers(cluster, "koshin", name, 6.hours, 60.seconds)
        val bare = ProxyJson.parseToJsonElement("""{"metadata":{"uid":"u"}}""").jsonObject
        val generateName = workers.jobFor(bare)["metadata"]!!.jsonObject["generateName"]!!.jsonPrimitive.content
        assertEquals(58, generateName.length)
    }

    @Test
    fun `API errors carry the status`() = runTest {
        val forbidden = KubernetesBurstWorkers(
            api { json("""{"kind":"Status","reason":"Forbidden"}""", HttpStatusCode.Forbidden) },
            "koshin",
            "koshin-worker",
            6.hours,
            60.seconds,
        )
        val error = assertThrows<KubernetesApiException> { forbidden.active() }
        assertEquals(403, error.status)
    }

    @Test
    fun `invalid settings are rejected`() {
        assertThrows<IllegalArgumentException> { KubernetesBurstWorkers(cluster, " ", "t", 1.hours, 1.seconds) }
        assertThrows<IllegalArgumentException> { KubernetesBurstWorkers(cluster, "ns", " ", 1.hours, 1.seconds) }
        assertThrows<IllegalArgumentException> { KubernetesBurstWorkers(cluster, "ns", "t", 0.seconds, 1.seconds) }
        assertThrows<IllegalArgumentException> {
            KubernetesBurstWorkers(cluster, "ns", "t", 1.hours, (-1).seconds)
        }
    }

    @Test
    fun `the in-cluster client trusts the cluster CA and reads the ServiceAccount's namespace`(@TempDir dir: Path) {
        Path.of(checkNotNull(javaClass.getResource("/kubernetes/ca.crt")).toURI()).copyTo(dir.resolve("ca.crt"))
        dir.resolve("token").writeText("t\n")
        dir.resolve("namespace").writeText("koshin\n")

        KubernetesApi.inCluster(mapOf("KUBERNETES_SERVICE_HOST" to "fd00::1"), dir).close()
        assertEquals("koshin", KubernetesApi.inClusterNamespace(dir))
        assertEquals(1, KubernetesApi.trustManagerFor(dir.resolve("ca.crt")).acceptedIssuers.size)
        assertThrows<IllegalArgumentException> { KubernetesApi.inCluster(emptyMap(), dir) }
        dir.resolve("empty.crt").writeText("")
        assertThrows<IllegalArgumentException> { KubernetesApi.trustManagerFor(dir.resolve("empty.crt")) }
    }
}
