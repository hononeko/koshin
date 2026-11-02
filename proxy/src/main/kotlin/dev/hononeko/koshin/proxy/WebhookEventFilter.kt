package dev.hononeko.koshin.proxy

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull

/** The GitHub account a webhook is about: the repository owner, which is also its App installation's account. */
data class WebhookAccount(val id: Long, val login: String) {
    override fun toString(): String = "$login ($id)"
}

sealed interface WebhookDecision {
    /** [account] is the GitHub repository owner, checked against `GITHUB_APP_ALLOWED_ACCOUNTS`. */
    data class Enqueue(val repository: String, val account: WebhookAccount? = null) : WebhookDecision

    /**
     * The GitHub App was installed on these repositories: run each once, as a scheduled run. [newInstallation] when it
     * was installed on an account just now (`installation` created) rather than given more repositories.
     */
    data class EnqueueInstalled(
        val repositories: List<String>,
        val account: WebhookAccount? = null,
        val installationId: Long? = null,
        val newInstallation: Boolean = false,
    ) : WebhookDecision

    /** The GitHub App installation was deleted or suspended: its tokens are no good any more. */
    data class InstallationGone(val installationId: Long, val reason: String) : WebhookDecision

    data class Ignore(val reason: String) : WebhookDecision
    data class Malformed(val reason: String) : WebhookDecision
}

/**
 * Decides which webhooks start a Renovate run. Pushes and events by a bot login ([botLogins], plus [extraBotLogins],
 * e.g. the GitHub App's bot once it's known) are ignored, so Renovate's own changes don't re-trigger it.
 */
class WebhookEventFilter(
    botLogins: Set<String> = ProxyConfig.DEFAULT_BOT_LOGINS,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val extraBotLogins: () -> Collection<String> = { emptyList() },
) {
    private val normalizedBotLogins: Set<String> = botLogins.map { it.trim().lowercase() }.toSet()

    fun evaluateGitHub(eventHeader: String?, rawPayload: String): WebhookDecision {
        val eventType = eventHeader?.trim()?.lowercase()
        return when {
            eventType.isNullOrEmpty() -> WebhookDecision.Ignore("missing X-GitHub-Event header")
            eventType == "ping" -> WebhookDecision.Ignore("github ping event")
            eventType in INSTALLATION_EVENTS -> evaluateInstallation(eventType, rawPayload)
            else -> evaluateGitHubJson(eventType, rawPayload)
        }
    }

    private fun evaluateGitHubJson(eventType: String, rawPayload: String): WebhookDecision {
        val root = parseJsonObject(rawPayload)
            ?: return WebhookDecision.Malformed("invalid JSON payload")

        val repoObj = root["repository"] as? JsonObject
        val fullName = repoObj?.stringField("full_name")?.takeIf { it.isNotBlank() }

        return if (repoObj == null || fullName == null) {
            WebhookDecision.Ignore("missing repository.full_name")
        } else {
            val decision = when (eventType) {
                "push" -> evaluateGitHubPush(root, repoObj, fullName)
                "pull_request" -> evaluateGitHubPullRequest(root, fullName)
                "issues" -> evaluateGitHubIssues(root, fullName)
                else -> WebhookDecision.Ignore("unhandled GitHub event '$eventType'")
            }
            if (decision is WebhookDecision.Enqueue) decision.copy(account = repoObj.account("owner")) else decision
        }
    }

    private fun JsonObject.account(field: String): WebhookAccount? {
        val obj = this[field] as? JsonObject
        val id = obj?.get("id")?.jsonPrimitive?.longOrNull
        return id?.let { WebhookAccount(it, obj.stringField("login").orEmpty()) }
    }

    /** `installation` (created) and `installation_repositories` (added) list the repositories the App now covers. */
    private fun evaluateInstallation(eventType: String, rawPayload: String): WebhookDecision {
        val root = parseJsonObject(rawPayload)
            ?: return WebhookDecision.Malformed("invalid JSON payload")
        val action = root.stringField("action")?.lowercase()
        val installationId = (root["installation"] as? JsonObject)?.get("id")?.jsonPrimitive?.longOrNull
        val added = when {
            eventType == "installation" && action == "created" -> root["repositories"]
            eventType == "installation_repositories" && action == "added" -> root["repositories_added"]
            else -> null
        }
        val repositories = (added as? JsonArray).orEmpty().mapNotNull { item ->
            (item as? JsonObject)?.stringField("full_name")?.takeIf { it.isNotBlank() }
        }
        return when {
            eventType == "installation" && action in GONE_ACTIONS && installationId != null ->
                WebhookDecision.InstallationGone(installationId, "installation $action")

            added == null -> WebhookDecision.Ignore("$eventType action '$action' ignored")

            repositories.isEmpty() -> WebhookDecision.Ignore("$eventType $action without repositories")

            else -> WebhookDecision.EnqueueInstalled(
                repositories,
                (root["installation"] as? JsonObject)?.account("account"),
                installationId,
                newInstallation = eventType == "installation",
            )
        }
    }

    fun evaluateGitLab(eventHeader: String?, rawPayload: String): WebhookDecision {
        val root = parseJsonObject(rawPayload)
            ?: return WebhookDecision.Malformed("invalid JSON payload")

        val projectObj = root["project"] as? JsonObject
        val pathWithNamespace = projectObj?.stringField("path_with_namespace")?.takeIf { it.isNotBlank() }

        return if (projectObj == null || pathWithNamespace == null) {
            WebhookDecision.Ignore("missing project.path_with_namespace")
        } else {
            val objectKind = root.stringField("object_kind")?.lowercase()
                ?: root.stringField("event_name")?.lowercase()
            val headerNormalized = eventHeader?.trim()?.lowercase()

            when {
                objectKind == "push" || headerNormalized == "push hook" ->
                    evaluateGitLabPush(root, projectObj, pathWithNamespace)

                objectKind == "merge_request" || headerNormalized == "merge request hook" ->
                    evaluateGitLabMergeRequest(root, pathWithNamespace)

                objectKind == "issue" || headerNormalized == "issue hook" ->
                    evaluateGitLabIssue(root, pathWithNamespace)

                else -> WebhookDecision.Ignore(
                    "unhandled GitLab event header='${eventHeader.orEmpty()}' kind='${objectKind.orEmpty()}'",
                )
            }
        }
    }

    private fun evaluateGitHubPush(root: JsonObject, repoObj: JsonObject, fullName: String): WebhookDecision {
        val deleted = root["deleted"]?.jsonPrimitive?.booleanOrNull ?: false
        val after = root.stringField("after")
        val ref = root.stringField("ref")
        val defaultBranch = repoObj.stringField("default_branch")?.takeIf { it.isNotBlank() } ?: DEFAULT_BRANCH
        val expectedRef = "refs/heads/$defaultBranch"

        val senderObj = root["sender"] as? JsonObject
        val senderLogin = senderObj?.stringField("login")
        val senderType = senderObj?.stringField("type")
        val pusherName = (root["pusher"] as? JsonObject)?.stringField("name")

        return when {
            deleted || after == ZERO_COMMIT_SHA -> WebhookDecision.Ignore("branch deletion push")

            ref == null -> WebhookDecision.Ignore("push missing ref")

            ref.startsWith(RENOVATE_BRANCH_REF_PREFIX) -> WebhookDecision.Ignore("renovate branch push ($ref)")

            ref != expectedRef -> WebhookDecision.Ignore("non-default branch push ($ref != $expectedRef)")

            isBotActor(senderLogin, senderType) || isBotActor(pusherName, null) ->
                WebhookDecision.Ignore("self-triggered push by bot (${senderLogin ?: pusherName})")

            else -> WebhookDecision.Enqueue(fullName)
        }
    }

    private fun evaluateGitHubPullRequest(root: JsonObject, fullName: String): WebhookDecision {
        val senderObj = root["sender"] as? JsonObject
        val senderLogin = senderObj?.stringField("login")
        val senderType = senderObj?.stringField("type")
        val action = root.stringField("action")?.lowercase()

        return when {
            isBotActor(senderLogin, senderType) ->
                WebhookDecision.Ignore("self-triggered pull_request by bot ($senderLogin)")

            action == null -> WebhookDecision.Ignore("pull_request missing action")

            action == "closed" -> WebhookDecision.Enqueue(fullName)

            action == "edited" -> {
                val prObj = root["pull_request"] as? JsonObject
                val currentBody = prObj?.stringField("body")
                val previousBody = ((root["changes"] as? JsonObject)?.get("body") as? JsonObject)
                    ?.stringField("from")
                if (hasCheckboxToggled(previousBody, currentBody)) {
                    WebhookDecision.Enqueue(fullName)
                } else {
                    WebhookDecision.Ignore("pull_request edited without checkbox toggle")
                }
            }

            else -> WebhookDecision.Ignore("pull_request action '$action' ignored")
        }
    }

    private fun evaluateGitHubIssues(root: JsonObject, fullName: String): WebhookDecision {
        val senderObj = root["sender"] as? JsonObject
        val senderLogin = senderObj?.stringField("login")
        val senderType = senderObj?.stringField("type")
        val action = root.stringField("action")?.lowercase()
        val issueObj = root["issue"] as? JsonObject
        val title = issueObj?.stringField("title").orEmpty()
        val currentBody = issueObj?.stringField("body")
        val previousBody = ((root["changes"] as? JsonObject)?.get("body") as? JsonObject)
            ?.stringField("from")

        return when {
            isBotActor(senderLogin, senderType) ->
                WebhookDecision.Ignore("self-triggered issue event by bot ($senderLogin)")

            action == null -> WebhookDecision.Ignore("issues missing action")

            action != "edited" -> WebhookDecision.Ignore("issues action '$action' ignored")

            issueObj == null -> WebhookDecision.Ignore("issues missing issue object")

            !title.contains(DEPENDENCY_DASHBOARD_TITLE, ignoreCase = true) ->
                WebhookDecision.Ignore("issue is not Dependency Dashboard")

            hasCheckboxToggled(previousBody, currentBody) -> WebhookDecision.Enqueue(fullName)

            else -> WebhookDecision.Ignore("Dependency Dashboard edited without checkbox toggle")
        }
    }

    private fun evaluateGitLabPush(
        root: JsonObject,
        projectObj: JsonObject,
        pathWithNamespace: String,
    ): WebhookDecision {
        val after = root.stringField("after")
        val ref = root.stringField("ref")
        val defaultBranch = projectObj.stringField("default_branch")?.takeIf { it.isNotBlank() } ?: DEFAULT_BRANCH
        val expectedRef = "refs/heads/$defaultBranch"
        val username = extractGitLabUsername(root)

        return when {
            after == ZERO_COMMIT_SHA -> WebhookDecision.Ignore("GitLab branch deletion push")
            ref == null -> WebhookDecision.Ignore("GitLab push missing ref")
            ref.startsWith(RENOVATE_BRANCH_REF_PREFIX) -> WebhookDecision.Ignore("GitLab renovate branch push ($ref)")
            ref != expectedRef -> WebhookDecision.Ignore("GitLab non-default branch push ($ref != $expectedRef)")
            isBotActor(username, null) -> WebhookDecision.Ignore("GitLab self-triggered push by bot ($username)")
            else -> WebhookDecision.Enqueue(pathWithNamespace)
        }
    }

    private fun evaluateGitLabMergeRequest(root: JsonObject, pathWithNamespace: String): WebhookDecision {
        val username = extractGitLabUsername(root)
        val attrs = root["object_attributes"] as? JsonObject
        val action = attrs?.stringField("action")?.lowercase()

        return when {
            isBotActor(username, null) ->
                WebhookDecision.Ignore("GitLab self-triggered merge_request by bot ($username)")

            attrs == null -> WebhookDecision.Ignore("GitLab merge_request missing object_attributes")

            action == null -> WebhookDecision.Ignore("GitLab merge_request missing action")

            action == "close" || action == "merge" -> WebhookDecision.Enqueue(pathWithNamespace)

            action == "update" -> {
                val descChange = (root["changes"] as? JsonObject)?.get("description") as? JsonObject
                val previous = descChange?.stringField("previous")
                val current = descChange?.stringField("current") ?: attrs.stringField("description")
                if (hasCheckboxToggled(previous, current)) {
                    WebhookDecision.Enqueue(pathWithNamespace)
                } else {
                    WebhookDecision.Ignore("GitLab merge_request updated without checkbox toggle")
                }
            }

            else -> WebhookDecision.Ignore("GitLab merge_request action '$action' ignored")
        }
    }

    private fun evaluateGitLabIssue(root: JsonObject, pathWithNamespace: String): WebhookDecision {
        val username = extractGitLabUsername(root)
        val attrs = root["object_attributes"] as? JsonObject
        val action = attrs?.stringField("action")?.lowercase()
        val title = attrs?.stringField("title").orEmpty()
        val descChange = (root["changes"] as? JsonObject)?.get("description") as? JsonObject
        val previous = descChange?.stringField("previous")
        val current = descChange?.stringField("current") ?: attrs?.stringField("description")

        return when {
            isBotActor(username, null) ->
                WebhookDecision.Ignore("GitLab self-triggered issue event by bot ($username)")

            attrs == null -> WebhookDecision.Ignore("GitLab issue missing object_attributes")

            action == null -> WebhookDecision.Ignore("GitLab issue missing action")

            action != "update" -> WebhookDecision.Ignore("GitLab issue action '$action' ignored")

            !title.contains(DEPENDENCY_DASHBOARD_TITLE, ignoreCase = true) ->
                WebhookDecision.Ignore("GitLab issue is not Dependency Dashboard")

            hasCheckboxToggled(previous, current) -> WebhookDecision.Enqueue(pathWithNamespace)

            else -> WebhookDecision.Ignore("GitLab Dependency Dashboard updated without checkbox toggle")
        }
    }

    private fun extractGitLabUsername(root: JsonObject): String? =
        root.stringField("user_username") ?: (root["user"] as? JsonObject)?.stringField("username")

    private fun isBotActor(login: String?, senderType: String?): Boolean {
        val normalized = login?.trim()?.lowercase()?.takeIf { it.isNotEmpty() } ?: return false
        return normalized in normalizedBotLogins ||
            extraBotLogins().any { it.trim().lowercase() == normalized } ||
            (senderType.equals("Bot", ignoreCase = true) && normalized.contains("renovate"))
    }

    private fun parseJsonObject(rawPayload: String): JsonObject? = try {
        json.parseToJsonElement(rawPayload).jsonObject
    } catch (_: Exception) {
        null
    }

    private fun JsonObject.stringField(key: String): String? = this[key]?.jsonPrimitive?.contentOrNull

    companion object {
        private val INSTALLATION_EVENTS = setOf("installation", "installation_repositories")
        private val GONE_ACTIONS = setOf("deleted", "suspend")
        private const val DEFAULT_BRANCH = "main"
        private const val RENOVATE_BRANCH_REF_PREFIX = "refs/heads/renovate/"
        private const val ZERO_COMMIT_SHA = "0000000000000000000000000000000000000000"
        private const val DEPENDENCY_DASHBOARD_TITLE = "Dependency Dashboard"
        private const val CHECKBOX_MARKER_LENGTH = 6
        private val CHECKED_BOX_REGEX = Regex("""(?m)^\s*[-*]\s+\[[xX]]""")
        private val UNCHECKED_BOX_REGEX = Regex("""(?m)^\s*[-*]\s+\[ ]""")

        fun hasCheckboxToggled(previousBody: String?, currentBody: String?): Boolean {
            if (previousBody == null || currentBody == null || previousBody == currentBody) {
                return false
            }
            val prevChecked = CHECKED_BOX_REGEX.findAll(previousBody).count()
            val currChecked = CHECKED_BOX_REGEX.findAll(currentBody).count()
            val prevUnchecked = UNCHECKED_BOX_REGEX.findAll(previousBody).count()
            val currUnchecked = UNCHECKED_BOX_REGEX.findAll(currentBody).count()

            val countChangedWithSameTotal =
                prevChecked != currChecked && (prevChecked + prevUnchecked) == (currChecked + currUnchecked)

            return countChangedWithSameTotal || hasPerItemToggle(previousBody, currentBody)
        }

        private fun hasPerItemToggle(previousBody: String, currentBody: String): Boolean {
            val prevLines = extractCheckboxStates(previousBody)
            val currLines = extractCheckboxStates(currentBody)
            return prevLines.isNotEmpty() &&
                currLines.isNotEmpty() &&
                prevLines.any { (label, wasChecked) ->
                    currLines[label]?.let { isNowChecked -> isNowChecked != wasChecked } == true
                }
        }

        private fun extractCheckboxStates(markdown: String): Map<String, Boolean> {
            val result = mutableMapOf<String, Boolean>()
            for (line in markdown.lineSequence()) {
                val trimmed = line.trimStart()
                when {
                    trimmed.startsWith("- [ ] ") || trimmed.startsWith("* [ ] ") ->
                        result[trimmed.substring(CHECKBOX_MARKER_LENGTH).trim()] = false

                    trimmed.startsWith("- [x] ") ||
                        trimmed.startsWith("- [X] ") ||
                        trimmed.startsWith("* [x] ") ||
                        trimmed.startsWith("* [X] ") ->
                        result[trimmed.substring(CHECKBOX_MARKER_LENGTH).trim()] = true
                }
            }
            return result
        }
    }
}
