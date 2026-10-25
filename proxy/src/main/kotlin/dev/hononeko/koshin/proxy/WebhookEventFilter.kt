package dev.hononeko.koshin.proxy

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

sealed interface WebhookDecision {
    data class Enqueue(val repository: String) : WebhookDecision
    data class Ignore(val reason: String) : WebhookDecision
    data class Malformed(val reason: String) : WebhookDecision
}

class WebhookEventFilter(
    botLogins: Set<String> = ProxyConfig.DEFAULT_BOT_LOGINS,
    private val json: Json = Json { ignoreUnknownKeys = true },
) {
    private val normalizedBotLogins: Set<String> = botLogins.map { it.trim().lowercase() }.toSet()

    fun evaluateGitHub(eventHeader: String?, rawPayload: String): WebhookDecision {
        val eventType = eventHeader?.trim()?.lowercase()
        return when {
            eventType.isNullOrEmpty() -> WebhookDecision.Ignore("missing X-GitHub-Event header")
            eventType == "ping" -> WebhookDecision.Ignore("github ping event")
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
            when (eventType) {
                "push" -> evaluateGitHubPush(root, repoObj, fullName)
                "pull_request" -> evaluateGitHubPullRequest(root, fullName)
                "issues" -> evaluateGitHubIssues(root, fullName)
                else -> WebhookDecision.Ignore("unhandled GitHub event '$eventType'")
            }
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
        val action = root.stringField("action")?.lowercase()
            ?: return WebhookDecision.Ignore("pull_request missing action")

        return when (action) {
            "closed" -> WebhookDecision.Enqueue(fullName)

            "edited" -> {
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
        val action = root.stringField("action")?.lowercase()
        val issueObj = root["issue"] as? JsonObject
        val title = issueObj?.stringField("title").orEmpty()
        val currentBody = issueObj?.stringField("body")
        val previousBody = ((root["changes"] as? JsonObject)?.get("body") as? JsonObject)
            ?.stringField("from")

        return when {
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
        val username = root.stringField("user_username")
            ?: (root["user"] as? JsonObject)?.stringField("username")

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
        val attrs = root["object_attributes"] as? JsonObject
        val action = attrs?.stringField("action")?.lowercase()

        return when {
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
        val attrs = root["object_attributes"] as? JsonObject
        val action = attrs?.stringField("action")?.lowercase()
        val title = attrs?.stringField("title").orEmpty()
        val descChange = (root["changes"] as? JsonObject)?.get("description") as? JsonObject
        val previous = descChange?.stringField("previous")
        val current = descChange?.stringField("current") ?: attrs?.stringField("description")

        return when {
            attrs == null -> WebhookDecision.Ignore("GitLab issue missing object_attributes")

            action == null -> WebhookDecision.Ignore("GitLab issue missing action")

            action != "update" -> WebhookDecision.Ignore("GitLab issue action '$action' ignored")

            !title.contains(DEPENDENCY_DASHBOARD_TITLE, ignoreCase = true) ->
                WebhookDecision.Ignore("GitLab issue is not Dependency Dashboard")

            hasCheckboxToggled(previous, current) -> WebhookDecision.Enqueue(pathWithNamespace)

            else -> WebhookDecision.Ignore("GitLab Dependency Dashboard updated without checkbox toggle")
        }
    }

    private fun isBotActor(login: String?, senderType: String?): Boolean {
        val normalized = login?.trim()?.lowercase()?.takeIf { it.isNotEmpty() } ?: return false
        return normalized in normalizedBotLogins ||
            normalized.endsWith("[bot]") ||
            (senderType.equals("Bot", ignoreCase = true) && normalized.contains("renovate"))
    }

    private fun parseJsonObject(rawPayload: String): JsonObject? = try {
        json.parseToJsonElement(rawPayload).jsonObject
    } catch (_: Exception) {
        null
    }

    private fun JsonObject.stringField(key: String): String? = this[key]?.jsonPrimitive?.contentOrNull

    companion object {
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
