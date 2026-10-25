package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.RepositoryDiscoveryPort
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path

class StaticFileRepositoryDiscovery(
    private val filePath: Path?,
    private val defaultPlatform: Platform = Platform.GITHUB,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : RepositoryDiscoveryPort {

    constructor(
        filePath: String?,
        defaultPlatform: Platform = Platform.GITHUB,
        json: Json = Json { ignoreUnknownKeys = true },
        ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    ) : this(
        filePath = filePath?.takeIf { it.isNotBlank() }?.let { Path.of(it) },
        defaultPlatform = defaultPlatform,
        json = json,
        ioDispatcher = ioDispatcher,
    )

    override suspend fun discover(): List<RepoRef> {
        val path = filePath
        return if (path == null) {
            emptyList()
        } else {
            val content = withContext(ioDispatcher) {
                require(Files.exists(path)) { "Sweep repositories file does not exist: $path" }
                Files.readString(path, Charsets.UTF_8)
            }.trim()

            when {
                content.isEmpty() -> emptyList()
                content.startsWith("[") -> parseJsonArray(content)
                else -> parseLineDelimited(content)
            }
        }
    }

    private fun parseJsonArray(content: String): List<RepoRef> {
        val element = json.parseToJsonElement(content)
        require(element is JsonArray) { "SWEEP_REPOSITORIES_FILE JSON content must be an array" }
        return element.map { parseJsonElement(it) }
    }

    private fun parseJsonElement(item: JsonElement): RepoRef = when (item) {
        is JsonPrimitive -> {
            val raw = requireNotNull(item.contentOrNull) {
                "Invalid string entry in SWEEP_REPOSITORIES_FILE"
            }
            parseSpecString(raw, defaultPlatform)
        }

        is JsonObject -> {
            val platformRaw = item["platform"]?.jsonPrimitive?.contentOrNull
            val platform = if (platformRaw.isNullOrBlank()) {
                defaultPlatform
            } else {
                parsePlatform(platformRaw)
            }
            val repository = requireNotNull(item["repository"]?.jsonPrimitive?.contentOrNull) {
                "Missing 'repository' field in SWEEP_REPOSITORIES_FILE object"
            }
            RepoRef(platform, repository.trim())
        }

        else -> throw IllegalArgumentException("Unsupported JSON element in SWEEP_REPOSITORIES_FILE: $item")
    }

    private fun parseLineDelimited(content: String): List<RepoRef> = content.lineSequence()
        .map { line -> line.substringBefore('#').trim() }
        .filter { it.isNotEmpty() }
        .map { line -> parseSpecString(line, defaultPlatform) }
        .toList()

    companion object {
        fun parseSpecString(spec: String, defaultPlatform: Platform = Platform.GITHUB): RepoRef {
            val trimmed = spec.trim()
            require(trimmed.isNotEmpty()) { "Repository specification cannot be blank" }
            return if (':' in trimmed) {
                val platformPart = trimmed.substringBefore(':').trim()
                val repoPart = trimmed.substringAfter(':').trim()
                RepoRef(parsePlatform(platformPart), repoPart)
            } else {
                RepoRef(defaultPlatform, trimmed)
            }
        }

        fun parsePlatform(raw: String): Platform {
            val cleaned = raw.trim()
            return requireNotNull(
                Platform.entries.firstOrNull { it.name.equals(cleaned, ignoreCase = true) },
            ) { "Unknown platform: '$raw'" }
        }
    }
}
