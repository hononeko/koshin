package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.ports.BurstWorkerPort
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import kotlin.time.Duration

/**
 * Burst workers as Kubernetes `batch/v1` `Job`s cloned from the chart-rendered `v1/PodTemplate` [podTemplateName], so
 * every pod setting stays in Helm or Kustomize. Each `Job` runs one pod once (`backoffLimit: 0`), is deleted
 * [ttlAfterFinished] after it finishes, and belongs to the `PodTemplate`, so uninstalling removes it.
 */
class KubernetesBurstWorkers(
    private val api: KubernetesApi,
    private val namespace: String,
    private val podTemplateName: String,
    private val activeDeadline: Duration,
    private val ttlAfterFinished: Duration,
) : BurstWorkerPort {
    init {
        require(namespace.isNotBlank()) { "namespace cannot be blank" }
        require(podTemplateName.isNotBlank()) { "podTemplateName cannot be blank" }
        require(activeDeadline.inWholeSeconds >= 1) { "activeDeadline must be at least 1s: $activeDeadline" }
        require(!ttlAfterFinished.isNegative()) { "ttlAfterFinished cannot be negative: $ttlAfterFinished" }
    }

    private val jobsPath = "/apis/batch/v1/namespaces/$namespace/jobs"

    /** The `PodTemplate` seen by the last [active], reused by the [start] that follows it. */
    @Volatile
    private var lastTemplate: JsonObject? = null

    override suspend fun active(): Int {
        val template = podTemplate()
        val jobs = api.get(jobsPath, mapOf("labelSelector" to "$TEMPLATE_UID_LABEL=${template.uid}"))
        return jobs["items"]?.jsonArray.orEmpty().count { isActive(it.jsonObject) }
    }

    override suspend fun start(count: Int) {
        if (count <= 0) return
        val template = lastTemplate ?: podTemplate()
        repeat(count) { api.post(jobsPath, jobFor(template)) }
    }

    private suspend fun podTemplate(): JsonObject =
        api.get("/api/v1/namespaces/$namespace/podtemplates/$podTemplateName").also { lastTemplate = it }

    /** The `Job` running [podTemplate]'s pod once, labelled as a burst worker of that template. */
    internal fun jobFor(podTemplate: JsonObject): JsonObject {
        val uid = podTemplate.uid
        val template = podTemplate["template"]?.jsonObject ?: JsonObject(emptyMap())
        val podMetadata = template["metadata"]?.jsonObject ?: JsonObject(emptyMap())
        val labels = JsonObject(
            podMetadata["labels"]?.jsonObject.orEmpty() + mapOf(
                WORKER_MODE_LABEL to JsonPrimitive(BURST),
                TEMPLATE_UID_LABEL to JsonPrimitive(uid),
            ),
        )
        val podSpec = template["spec"]?.jsonObject.orEmpty()
        return buildJsonObject {
            put("apiVersion", "batch/v1")
            put("kind", "Job")
            putJsonObject("metadata") {
                put("generateName", "${podTemplateName.take(MAX_GENERATE_NAME_PREFIX).trimEnd('-')}-")
                put("labels", labels)
                putJsonArray("ownerReferences") {
                    addJsonObject {
                        put("apiVersion", "v1")
                        put("kind", "PodTemplate")
                        put("name", podTemplateName)
                        put("uid", uid)
                    }
                }
            }
            putJsonObject("spec") {
                put("backoffLimit", 0)
                put("ttlSecondsAfterFinished", ttlAfterFinished.inWholeSeconds)
                put("activeDeadlineSeconds", activeDeadline.inWholeSeconds)
                putJsonObject("template") {
                    put("metadata", JsonObject(podMetadata + ("labels" to labels)))
                    // A Job's pod must not restart in place; it exits 0 once drained
                    put("spec", JsonObject(podSpec + ("restartPolicy" to JsonPrimitive("Never"))))
                }
            }
        }
    }

    private val JsonObject.uid: String
        get() = requireNotNull(this["metadata"]?.jsonObject?.get("uid")?.jsonPrimitive?.contentOrNull) {
            "PodTemplate $podTemplateName has no metadata.uid"
        }

    companion object {
        const val WORKER_MODE_LABEL = "koshin.hononeko.dev/worker-mode"
        const val TEMPLATE_UID_LABEL = "koshin.hononeko.dev/pod-template-uid"
        const val BURST = "burst"

        /** Job names stay within 63 characters, with 5 random ones added. */
        private const val MAX_GENERATE_NAME_PREFIX = 57

        /** Neither succeeded nor failed yet. */
        internal fun isActive(job: JsonObject): Boolean {
            val status = job["status"]?.jsonObject ?: return true
            return status.count("succeeded") == 0L && status.count("failed") == 0L
        }

        private fun JsonObject.count(field: String): Long = (this[field] as? JsonPrimitive)?.longOrNull ?: 0L
    }
}
