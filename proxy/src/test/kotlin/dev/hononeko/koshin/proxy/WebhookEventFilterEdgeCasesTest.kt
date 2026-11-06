package dev.hononeko.koshin.proxy

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WebhookEventFilterEdgeCasesTest {
    private val filter = WebhookEventFilter(
        botLogins = setOf("renovate[bot]", "renovate", "renovate-bot", "koshin-bot"),
    )

    @Test
    fun `GitHub event header whitespace and malformed payloads`() {
        assertTrue(filter.evaluateGitHub("   ", "{}") is WebhookDecision.Ignore)
        assertTrue(filter.evaluateGitHub("push", "not-json") is WebhookDecision.Malformed)
        assertTrue(filter.evaluateGitHub("push", "{}") is WebhookDecision.Ignore)
        val missingRepo = """{"sender":{"login":"octocat"}}"""
        assertTrue(filter.evaluateGitHub("push", missingRepo) is WebhookDecision.Ignore)
        val blankRepo = """{"repository":{"full_name":"   "}}"""
        assertTrue(filter.evaluateGitHub("push", blankRepo) is WebhookDecision.Ignore)
    }

    @Test
    fun `GitHub push edge cases for missing ref and default branch fallback`() {
        val missingRef = """
            {
              "repository": { "full_name": "hononeko/koshin" },
              "sender": { "login": "octocat" }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Ignore("push missing ref"), filter.evaluateGitHub("push", missingRef))

        val blankDefaultBranch = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin", "default_branch": "   " },
              "sender": { "login": "octocat" }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Enqueue("hononeko/koshin"), filter.evaluateGitHub("push", blankDefaultBranch))

        val pusherBot = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin" },
              "sender": {},
              "pusher": { "name": "renovate[bot]" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("push", pusherBot) is WebhookDecision.Ignore)

        val pushDeleted = """
            {
              "ref": "refs/heads/main",
              "deleted": true,
              "repository": { "full_name": "hononeko/koshin" },
              "sender": { "login": "octocat" }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Ignore("branch deletion push"), filter.evaluateGitHub("push", pushDeleted))
    }

    @Test
    fun `GitHub pull request and issue edge cases`() {
        val prMissingAction = """{"repository":{"full_name":"hononeko/koshin"}}"""
        assertEquals(
            WebhookDecision.Ignore("pull_request missing action"),
            filter.evaluateGitHub("pull_request", prMissingAction),
        )

        val prEditedWithoutChanges = """
            {
              "action": "edited",
              "repository": { "full_name": "hononeko/koshin" },
              "pull_request": { "body": "some body" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("pull_request", prEditedWithoutChanges) is WebhookDecision.Ignore)

        val issuesMissingAction = """{"repository":{"full_name":"hononeko/koshin"}}"""
        assertEquals(
            WebhookDecision.Ignore("issues missing action"),
            filter.evaluateGitHub("issues", issuesMissingAction),
        )

        val issuesMissingObject = """{"action":"edited","repository":{"full_name":"hononeko/koshin"}}"""
        assertEquals(
            WebhookDecision.Ignore("issues missing issue object"),
            filter.evaluateGitHub("issues", issuesMissingObject),
        )
    }

    @Test
    fun `GitHub installation events edge cases`() {
        val installNoAction = """{"installation":{"id":42}}"""
        assertTrue(filter.evaluateGitHub("installation", installNoAction) is WebhookDecision.Ignore)

        val installDeletedNoId = """{"action":"deleted"}"""
        assertTrue(filter.evaluateGitHub("installation", installDeletedNoId) is WebhookDecision.Ignore)

        val installReposEmpty = """
            {
              "action": "added",
              "installation": { "id": 42 },
              "repositories_added": []
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("installation_repositories", installReposEmpty) is WebhookDecision.Ignore)

        val accountWithSlug = """
            {
              "action": "created",
              "installation": {
                "id": 42,
                "account": { "id": 100, "slug": "slug-org" }
              },
              "repositories": [{ "full_name": "hononeko/repo" }]
            }
        """.trimIndent()
        val decisionSlug = filter.evaluateGitHub("installation", accountWithSlug)
        assertTrue(decisionSlug is WebhookDecision.EnqueueInstalled)
        assertEquals(WebhookAccount(100, "slug-org"), (decisionSlug as WebhookDecision.EnqueueInstalled).account)

        val accountNoLoginNoSlug = """
            {
              "action": "created",
              "installation": {
                "id": 42,
                "account": { "id": 100 }
              },
              "repositories": [{ "full_name": "hononeko/repo" }]
            }
        """.trimIndent()
        val decisionEmpty = filter.evaluateGitHub("installation", accountNoLoginNoSlug)
        assertTrue(decisionEmpty is WebhookDecision.EnqueueInstalled)
        assertEquals(WebhookAccount(100, ""), (decisionEmpty as WebhookDecision.EnqueueInstalled).account)
    }

    @Test
    fun `GitLab events header and object kind variations`() {
        val eventNamePayload = """
            {
              "event_name": "push",
              "ref": "refs/heads/main",
              "project": { "path_with_namespace": "gitlab-org/project" }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Enqueue("gitlab-org/project"), filter.evaluateGitLab(null, eventNamePayload))

        assertTrue(filter.evaluateGitLab("Push Hook", """{"object_kind":"push"}""") is WebhookDecision.Ignore)
        val blankProject = """{"object_kind":"push","project":{"path_with_namespace":"   "}}"""
        assertTrue(filter.evaluateGitLab("Push Hook", blankProject) is WebhookDecision.Ignore)

        val pushNoRef = """{"object_kind":"push","project":{"path_with_namespace":"a/b"}}"""
        assertEquals(WebhookDecision.Ignore("GitLab push missing ref"), filter.evaluateGitLab("Push Hook", pushNoRef))

        val mrClose = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "a/b" },
              "object_attributes": { "action": "close" }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Enqueue("a/b"), filter.evaluateGitLab("Merge Request Hook", mrClose))

        val mrMerge = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "a/b" },
              "object_attributes": { "action": "merge" }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Enqueue("a/b"), filter.evaluateGitLab("Merge Request Hook", mrMerge))
    }

    @Test
    fun `GitLab merge request and issue error conditions`() {
        val mrNoAttrs = """{"object_kind":"merge_request","project":{"path_with_namespace":"a/b"}}"""
        assertEquals(
            WebhookDecision.Ignore("GitLab merge_request missing object_attributes"),
            filter.evaluateGitLab(null, mrNoAttrs),
        )

        val mrNoAction = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "a/b" },
              "object_attributes": {}
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Ignore("GitLab merge_request missing action"),
            filter.evaluateGitLab(null, mrNoAction),
        )

        val issueNoAttrs = """{"object_kind":"issue","project":{"path_with_namespace":"a/b"}}"""
        assertEquals(
            WebhookDecision.Ignore("GitLab issue missing object_attributes"),
            filter.evaluateGitLab(null, issueNoAttrs),
        )

        val issueNoAction = """
            {
              "object_kind": "issue",
              "project": { "path_with_namespace": "a/b" },
              "object_attributes": {}
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Ignore("GitLab issue missing action"),
            filter.evaluateGitLab(null, issueNoAction),
        )

        val issueOpen = """
            {
              "object_kind": "issue",
              "project": { "path_with_namespace": "a/b" },
              "object_attributes": { "action": "open" }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Ignore("GitLab issue action 'open' ignored"),
            filter.evaluateGitLab(null, issueOpen),
        )

        val unhandled = """{"project":{"path_with_namespace":"a/b"}}"""
        assertTrue(filter.evaluateGitLab("custom_header", unhandled) is WebhookDecision.Ignore)
    }

    @Test
    fun `checkbox parsing supports diverse markdown formats and rejects invalid transitions`() {
        assertTrue(WebhookEventFilter.hasCheckboxToggled("- [ ] Task", "- [X] Task"))
        assertTrue(WebhookEventFilter.hasCheckboxToggled("* [ ] Task", "* [x] Task"))
        assertTrue(WebhookEventFilter.hasCheckboxToggled("* [ ] Task", "* [X] Task"))

        assertTrue(WebhookEventFilter.hasCheckboxToggled("- [ ] Task A", "- [x] Task A\n- [ ] Task B"))
        assertFalse(WebhookEventFilter.hasCheckboxToggled("- [ ] Task A", "- [ ] Task A\n- [ ] Task B"))
        assertFalse(WebhookEventFilter.hasCheckboxToggled("- [ ] Task A", "- [ ] Task B\n- [ ] Task C"))

        assertFalse(WebhookEventFilter.hasCheckboxToggled("- [x] Task A", "- [x] Task A\n- [ ] Task B"))
    }

    @Test
    fun `GitHub header null and non-positive installation id handling`() {
        assertEquals(WebhookDecision.Ignore("missing X-GitHub-Event header"), filter.evaluateGitHub(null, "{}"))

        val pushZeroInstall = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin", "owner": { "id": 10, "login": "hononeko" } },
              "installation": { "id": 0 }
            }
        """.trimIndent()
        val decisionZero = filter.evaluateGitHub("push", pushZeroInstall)
        assertTrue(decisionZero is WebhookDecision.Enqueue)
        assertNull((decisionZero as WebhookDecision.Enqueue).installationId)
        assertEquals(WebhookAccount(10, "hononeko"), decisionZero.account)

        val pushNegativeInstall = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin" },
              "installation": { "id": -5 }
            }
        """.trimIndent()
        val decisionNegative = filter.evaluateGitHub("push", pushNegativeInstall)
        assertTrue(decisionNegative is WebhookDecision.Enqueue)
        assertNull((decisionNegative as WebhookDecision.Enqueue).installationId)
        assertNull(decisionNegative.account)

        val prEditedNoObj = """
            {
              "action": "edited",
              "repository": { "full_name": "hononeko/koshin" }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Ignore("pull_request edited without checkbox toggle"),
            filter.evaluateGitHub("pull_request", prEditedNoObj),
        )
    }

    @Test
    fun `GitLab description change fallback to object attributes`() {
        val mrUpdateWithFallback = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "a/b" },
              "object_attributes": { "action": "update", "description": "- [x] Task" },
              "changes": {
                "description": { "previous": "- [ ] Task" }
              }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Enqueue("a/b"), filter.evaluateGitLab(null, mrUpdateWithFallback))

        val issueUpdateWithFallback = """
            {
              "object_kind": "issue",
              "project": { "path_with_namespace": "a/b" },
              "object_attributes": {
                "action": "update",
                "title": "Dependency Dashboard",
                "description": "* [x] Item"
              },
              "changes": {
                "description": { "previous": "* [ ] Item" }
              }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Enqueue("a/b"), filter.evaluateGitLab(null, issueUpdateWithFallback))

        val mrUpdateNoToggle = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "a/b" },
              "object_attributes": { "action": "update", "description": "same" },
              "changes": { "description": { "previous": "same" } }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Ignore("GitLab merge_request updated without checkbox toggle"),
            filter.evaluateGitLab(null, mrUpdateNoToggle),
        )
    }

    @Test
    fun `bot actor detection with extra bots and type heuristics`() {
        val filterWithExtra = WebhookEventFilter(
            botLogins = setOf("default-bot"),
            extraBotLogins = { listOf("dynamic-bot") },
        )
        val pushDynamicBot = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin" },
              "sender": { "login": "DYNAMIC-BOT", "type": "User" }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Ignore("self-triggered push by bot (DYNAMIC-BOT)"),
            filterWithExtra.evaluateGitHub("push", pushDynamicBot),
        )

        val pushBlankSender = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin" },
              "sender": { "login": "   ", "type": "Bot" }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Enqueue("hononeko/koshin"), filter.evaluateGitHub("push", pushBlankSender))

        val pushBotOther = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin" },
              "sender": { "login": "dependabot[bot]", "type": "Bot" }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Enqueue("hononeko/koshin"), filter.evaluateGitHub("push", pushBotOther))

        val pushRenovateSubstringUser = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin" },
              "sender": { "login": "renovate-fan", "type": "User" }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Enqueue("hononeko/koshin"),
            filter.evaluateGitHub("push", pushRenovateSubstringUser),
        )
    }

    @Test
    fun `checkbox toggle with nulls or empty transitions`() {
        assertFalse(WebhookEventFilter.hasCheckboxToggled(null, "- [x] A"))
        assertFalse(WebhookEventFilter.hasCheckboxToggled("- [ ] A", null))
        assertFalse(WebhookEventFilter.hasCheckboxToggled("same", "same"))
        assertFalse(WebhookEventFilter.hasCheckboxToggled("no checkboxes", "still none"))
        assertFalse(WebhookEventFilter.hasCheckboxToggled("- [ ] Task", "plain text without checkbox"))
        assertFalse(WebhookEventFilter.hasCheckboxToggled("plain text", "- [ ] Task"))
    }
}
