package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.ExecConfig
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.MountableFile
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.util.Base64
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.io.path.readText

/**
 * Renovate's encrypted config as koshin runs it (KSH-25): the Renovate version the chart pins, with the environment
 * [RenovateProcessExecutor] builds, against a real Git platform (Gitea). The keypairs are made, and the secrets
 * encrypted, with the README's `gpg` and `jq` commands.
 */
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class RenovateDecryptionIntegrationTest {
    companion object {
        private const val ADMIN = "koshin"

        // The throwaway Gitea's admin: made up for each run
        private val password = UUID.randomUUID().toString()
        private const val KEYS = "/tmp/keys"
        private val DECRYPTED = Regex("""Decrypted \w+ in \S+""")
        private val network = Network.newNetwork()
        private val gitea = GenericContainer("gitea/gitea:1.26-rootless")
            .withNetwork(network)
            .withNetworkAliases("gitea")
            .withEnv("GITEA__security__INSTALL_LOCK", "true")
            .withEnv("GITEA__server__ROOT_URL", "http://gitea:3000/")
            .withExposedPorts(3000)
            .waitingFor(Wait.forHttp("/api/v1/version").forPort(3000))
        private val renovate = GenericContainer(renovateImage())
            .withNetwork(network)
            .withCreateContainerCmdModifier { it.withEntrypoint("sleep") }
            .withCommand("infinity")
            .withCopyFileToContainer(MountableFile.forClasspathResource("renovate-encryption/", 0b111_101_101), "/it/")
        private lateinit var log: List<JsonObject>

        /** Everything the run printed, JSON or not, on stdout and stderr. */
        private lateinit var output: String

        /** The chart's Renovate version, in the slim image: decrypting needs none of the full image's tools. */
        private fun renovateImage(): String {
            val values = Path.of("../charts/koshin/values.yaml").readText()
            val tag = Regex("""repository: renovatebot/renovate\n(?:\s*#[^\n]*\n)*\s*tag: (\S+)""").find(values)
                ?.groupValues?.get(1)
            checkNotNull(tag) { "No Renovate tag in $values" }
            return "ghcr.io/renovatebot/renovate:${tag.removeSuffix("-full")}"
        }

        @BeforeAll
        @JvmStatic
        fun setUp() {
            gitea.start()
            renovate.start()
            gitea.run(
                listOf("gitea", "admin", "user", "create", "--admin", "--username", ADMIN, "--password", password) +
                    listOf("--email", "koshin@example.test", "--must-change-password=false"),
            )
            listOf("current", "old").forEach { renovate.run("/it/generate-key.sh", it) }
            val api = GiteaApi("http://${gitea.host}:${gitea.getMappedPort(3000)}/api/v1")
            seed(api)
            output = runRenovate(api.token(), listOf("acme/app", "evil/app", "evil/preset"))
            log = output.lines().filter { it.startsWith("{") }.map { Json.parseToJsonElement(it).jsonObject }
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            renovate.stop()
            gitea.stop()
            network.close()
        }

        private fun encrypt(key: String, org: String, repo: String?, secret: String) =
            renovate.run("/it/encrypt.sh", key, org, repo.orEmpty(), secret)

        /**
         * `acme` keeps an org-wide registry token in its `acme/renovate-config` preset and a repo-scoped one in
         * `acme/app`, plus one still encrypted for the previous key. `evil` replays acme's repo-scoped secret in a repo
         * of the same name, and extends acme's preset.
         */
        private fun seed(api: GiteaApi) {
            val orgWide = encrypt("current", "acme", null, "acme-org-token")
            val repoScoped = encrypt("current", "acme", "app", "acme-app-password")
            val previousKey = encrypt("old", "acme", null, "acme-old-token")
            listOf("acme", "evil").forEach { api.post("/orgs", buildJsonObject { put("username", it) }) }
            val orgPreset = hostRules(hostRule("npm.pkg.github.com", "token", orgWide))
            api.commit("acme/renovate-config", "default.json", orgPreset)
            api.commit(
                "acme/app",
                "renovate.json",
                hostRules(
                    hostRule("maven.pkg.github.com", "password", repoScoped),
                    hostRule("registry.example.com", "token", previousKey),
                    extends = "local>acme/renovate-config",
                ),
            )
            api.commit("evil/app", "renovate.json", hostRules(hostRule("maven.pkg.github.com", "password", repoScoped)))
            api.commit("evil/preset", "renovate.json", hostRules(extends = "local>acme/renovate-config"))
        }

        private fun hostRule(host: String, field: String, encrypted: String) = buildJsonObject {
            put("matchHost", host)
            putJsonObject("encrypted") { put(field, encrypted) }
        }

        private fun hostRules(vararg rules: JsonObject, extends: String? = null) = buildJsonObject {
            extends?.let { putJsonArray("extends") { add(it) } }
            if (rules.isNotEmpty()) put("hostRules", buildJsonArray { rules.forEach { add(it) } })
        }.toString()

        /** One Renovate run over [repositories], in dry-run, with what the worker would hand it; its output. */
        private fun runRenovate(token: String, repositories: List<String>): String {
            val env = RenovateProcessExecutor(
                baseDir = Path.of("/tmp/renovate"),
                inheritedEnv = mapOf(
                    "RENOVATE_PRIVATE_KEY_PATH" to "$KEYS/current-private.asc",
                    "RENOVATE_PRIVATE_KEY_PATH_OLD" to "$KEYS/old-private.asc",
                    // Renovate prefers it to RENOVATE_PRIVATE_KEY_PATH: a secret for the current key decrypting at
                    // all shows the worker strips it
                    "RENOVATE_PRIVATE_KEY" to renovate.run("cat", "$KEYS/old-private.asc"),
                    "RENOVATE_DRY_RUN" to "extract",
                    "RENOVATE_ONBOARDING" to "false",
                    "RENOVATE_REQUIRE_CONFIG" to "required",
                    "RENOVATE_LOG_LEVEL" to "debug",
                ),
            ).buildEnvironment(
                RepoRef(Platform.GITHUB, repositories.first()),
                PlatformCredentials(token, endpoint = "http://gitea:3000/api/v1"),
            ) + ("RENOVATE_PLATFORM" to "gitea")
            return renovate.run(listOf("renovate") + repositories, env, withStderr = true)
        }

        private fun GenericContainer<*>.run(vararg command: String) = run(command.toList())

        private fun GenericContainer<*>.run(
            command: List<String>,
            env: Map<String, String> = emptyMap(),
            withStderr: Boolean = false,
        ): String {
            val result = execInContainer(ExecConfig.builder().command(command.toTypedArray()).envVars(env).build())
            check(result.exitCode == 0) { "$command exited with ${result.exitCode}: ${result.stderr}" }
            return if (withStderr) result.stdout + "\n" + result.stderr else result.stdout
        }
    }

    private fun JsonObject.string(key: String) = this[key]?.jsonPrimitive?.contentOrNull

    private fun messages(repository: String) = log.filter { it.string("repository") == repository }

    /** Which secrets Renovate decrypted for [repository], in order: `Decrypted <field> in <path>`. */
    private fun decrypted(messages: List<String>) = messages.filter { DECRYPTED.matches(it) }

    private fun finished(repository: String) = messages(repository).single { it.string("msg") == "Repository finished" }

    private fun rejection(repository: String) = messages(repository)
        .single { it.string("msg") == "Could not parse decrypted string" }["err"]?.jsonObject?.string("validationError")

    @Test
    fun `a tenant's repository decrypts its repo-scoped and org-wide secrets, including its org preset's`() {
        assertEquals(
            listOf(
                "Decrypted password in $.hostRules[0].encrypted",
                "Decrypted token in $.hostRules[1].encrypted",
                // From acme/renovate-config, decrypted for acme/app
                "Decrypted token in $.hostRules[0].encrypted",
            ),
            decrypted(messages("acme/app").mapNotNull { it.string("msg") }),
        )
        assertNull(finished("acme/app").string("result"), "no config error")
    }

    @Test
    fun `a secret for the previous key still decrypts while rotating keys`() {
        val messages = messages("acme/app").mapNotNull { it.string("msg") }
        val oldKey = messages.indexOf("Trying to decrypt with old private key")

        assertTrue(oldKey >= 0, "the old key was tried")
        assertEquals("Decrypted token in $.hostRules[1].encrypted", decrypted(messages.drop(oldKey)).first())
    }

    @Test
    fun `another tenant can't replay a repo-scoped secret`() {
        assertEquals("Encrypted secret is scoped to a different repository: \"ACME/APP\".", rejection("evil/app"))
        assertEquals("config-validation", finished("evil/app").string("result"))
    }

    @Test
    fun `another tenant extending the org's preset can't decrypt its secrets`() {
        assertEquals("Encrypted secret is scoped to a different org: \"ACME/\".", rejection("evil/preset"))
        assertEquals("config-validation", finished("evil/preset").string("result"))
    }

    @Test
    fun `Renovate runs with no way for a repository to run code next to the key`() {
        val config = log.single { it.string("msg") == "Env config" }["config"]?.jsonObject

        assertEquals(
            mapOf(
                "allowScripts" to "false",
                "allowedUnsafeExecutions" to "[]",
                "allowPlugins" to "false",
                "allowCustomCrateGitRegistries" to "false",
                "allowedCommands" to "[]",
                "allowShellExecutorForPostUpgradeCommands" to "false",
            ),
            listOf(
                "allowScripts",
                "allowedUnsafeExecutions",
                "allowPlugins",
                "allowCustomCrateGitRegistries",
                "allowedCommands",
                "allowShellExecutorForPostUpgradeCommands",
            ).associateWith { config?.get(it).toString() },
        )
    }

    @Test
    fun `decrypted secrets stay out of everything Renovate prints`() {
        assertTrue(log.size > 100, "the debug log is there: ${log.size} lines")
        listOf("acme-org-token", "acme-app-password", "acme-old-token").forEach { secret ->
            assertFalse(secret in output, secret)
        }
    }

    /** Gitea's API, as its admin. */
    private class GiteaApi(private val base: String) {
        private val http = HttpClient.newHttpClient()
        private val auth = "Basic " + Base64.getEncoder().encodeToString("$ADMIN:$password".toByteArray())

        fun post(path: String, body: JsonObject): JsonObject {
            val request = HttpRequest.newBuilder(URI("$base$path"))
                .header("Authorization", auth)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build()
            val response = http.send(request, HttpResponse.BodyHandlers.ofString())
            check(response.statusCode() in 200..299) { "POST $path: ${response.statusCode()} ${response.body()}" }
            return Json.parseToJsonElement(response.body()).jsonObject
        }

        fun token(): String = post(
            "/users/$ADMIN/tokens",
            buildJsonObject {
                put("name", "renovate")
                putJsonArray("scopes") {
                    listOf("repository", "user", "organization", "issue", "misc").forEach { add("write:$it") }
                }
            },
        ).string("sha1")

        /** Commits [content] as [file] to [repository], creating it. */
        fun commit(repository: String, file: String, content: String) {
            val (owner, name) = repository.split('/')
            post(
                "/orgs/$owner/repos",
                buildJsonObject {
                    put("name", name)
                    put("auto_init", true)
                    put("default_branch", "main")
                },
            )
            post(
                "/repos/$repository/contents/$file",
                buildJsonObject {
                    put("content", Base64.getEncoder().encodeToString(content.toByteArray()))
                    put("message", "Add $file")
                },
            )
        }

        private fun JsonObject.string(key: String) = checkNotNull(this[key]?.jsonPrimitive?.contentOrNull) { key }
    }
}
