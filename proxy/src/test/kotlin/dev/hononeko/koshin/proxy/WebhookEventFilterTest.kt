package dev.hononeko.koshin.proxy

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WebhookEventFilterTest {

    private val filter = WebhookEventFilter(botLogins = setOf("renovate[bot]", "renovate", "koshin-bot"))

    @Test
    fun `GitHub push to default branch by human is enqueued`() {
        val payload = """
            {
              "ref": "refs/heads/main",
              "deleted": false,
              "after": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
              "repository": {
                "full_name": "hononeko/koshin",
                "default_branch": "main"
              },
              "sender": {
                "login": "octocat",
                "type": "User"
              },
              "pusher": {
                "name": "octocat"
              }
            }
        """.trimIndent()

        val decision = filter.evaluateGitHub("push", payload)
        assertEquals(WebhookDecision.Enqueue("hononeko/koshin"), decision)
    }

    @Test
    fun `GitHub push filters out non-default branch, renovate branch, and deleted branch`() {
        val nonDefaultBranch = """
            {
              "ref": "refs/heads/feature/foo",
              "repository": { "full_name": "hononeko/koshin", "default_branch": "main" },
              "sender": { "login": "octocat", "type": "User" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("push", nonDefaultBranch) is WebhookDecision.Ignore)

        val renovateBranch = """
            {
              "ref": "refs/heads/renovate/kotlin-monorepo",
              "repository": { "full_name": "hononeko/koshin", "default_branch": "main" },
              "sender": { "login": "octocat", "type": "User" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("push", renovateBranch) is WebhookDecision.Ignore)

        val deletedBranch = """
            {
              "ref": "refs/heads/main",
              "deleted": true,
              "repository": { "full_name": "hononeko/koshin", "default_branch": "main" },
              "sender": { "login": "octocat", "type": "User" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("push", deletedBranch) is WebhookDecision.Ignore)

        val zeroShaPush = """
            {
              "ref": "refs/heads/main",
              "after": "0000000000000000000000000000000000000000",
              "repository": { "full_name": "hononeko/koshin" },
              "sender": { "login": "octocat", "type": "User" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("push", zeroShaPush) is WebhookDecision.Ignore)
    }

    @Test
    fun `GitHub push filters out bot pushes`() {
        val renovateBotPush = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin", "default_branch": "main" },
              "sender": { "login": "renovate[bot]", "type": "Bot" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("push", renovateBotPush) is WebhookDecision.Ignore)

        val customBotPusher = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin", "default_branch": "main" },
              "sender": { "login": "octocat", "type": "User" },
              "pusher": { "name": "koshin-bot" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("push", customBotPusher) is WebhookDecision.Ignore)

        val botTypeRenovate = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "hononeko/koshin", "default_branch": "main" },
              "sender": { "login": "self-hosted-renovate-runner", "type": "Bot" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("push", botTypeRenovate) is WebhookDecision.Ignore)
    }

    @Test
    fun `GitHub pull_request allows closed and edited with checkbox toggle only`() {
        val prClosed = """
            {
              "action": "closed",
              "repository": { "full_name": "hononeko/koshin" },
              "pull_request": { "body": "PR description" }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Enqueue("hononeko/koshin"), filter.evaluateGitHub("pull_request", prClosed))

        val prEditedWithCheckboxToggle = """
            {
              "action": "edited",
              "repository": { "full_name": "hononeko/koshin" },
              "pull_request": {
                "body": "If you want to rebase/retry this PR, check this box\n- [x] <!-- rebase-check -->Rebase"
              },
              "changes": {
                "body": {
                  "from": "If you want to rebase/retry this PR, check this box\n- [ ] <!-- rebase-check -->Rebase"
                }
              }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Enqueue("hononeko/koshin"),
            filter.evaluateGitHub("pull_request", prEditedWithCheckboxToggle),
        )

        val prEditedWithoutCheckboxToggle = """
            {
              "action": "edited",
              "repository": { "full_name": "hononeko/koshin" },
              "pull_request": {
                "body": "Updated text only\n- [ ] <!-- rebase-check -->Rebase"
              },
              "changes": {
                "body": {
                  "from": "Original text\n- [ ] <!-- rebase-check -->Rebase"
                }
              }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("pull_request", prEditedWithoutCheckboxToggle) is WebhookDecision.Ignore)

        val prOpened = """
            {
              "action": "opened",
              "repository": { "full_name": "hononeko/koshin" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("pull_request", prOpened) is WebhookDecision.Ignore)
    }

    @Test
    fun `GitHub issues allows edited Dependency Dashboard with checkbox toggle only`() {
        val dashboardToggled = """
            {
              "action": "edited",
              "repository": { "full_name": "hononeko/koshin" },
              "issue": {
                "title": "Dependency Dashboard",
                "body": "## Pending Approval\n- [X] Approve all"
              },
              "changes": {
                "body": {
                  "from": "## Pending Approval\n- [ ] Approve all"
                }
              }
            }
        """.trimIndent()
        assertEquals(WebhookDecision.Enqueue("hononeko/koshin"), filter.evaluateGitHub("issues", dashboardToggled))

        val otherIssueEdited = """
            {
              "action": "edited",
              "repository": { "full_name": "hononeko/koshin" },
              "issue": {
                "title": "Bug report",
                "body": "- [x] Reproduce"
              },
              "changes": {
                "body": {
                  "from": "- [ ] Reproduce"
                }
              }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("issues", otherIssueEdited) is WebhookDecision.Ignore)

        val dashboardOpened = """
            {
              "action": "opened",
              "repository": { "full_name": "hononeko/koshin" },
              "issue": { "title": "Dependency Dashboard" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("issues", dashboardOpened) is WebhookDecision.Ignore)

        val dashboardEditedNoToggle = """
            {
              "action": "edited",
              "repository": { "full_name": "hononeko/koshin" },
              "issue": {
                "title": "Dependency Dashboard",
                "body": "## Pending Approval (updated text)\n- [ ] Approve all"
              },
              "changes": {
                "body": {
                  "from": "## Pending Approval\n- [ ] Approve all"
                }
              }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitHub("issues", dashboardEditedNoToggle) is WebhookDecision.Ignore)
    }

    @Test
    fun `GitHub ping, unknown events, missing headers, and malformed JSON are handled cleanly`() {
        val repoPayload = """{"repository":{"full_name":"hononeko/koshin"}}"""
        assertTrue(filter.evaluateGitHub("ping", "{}") is WebhookDecision.Ignore)
        assertTrue(filter.evaluateGitHub(null, "{}") is WebhookDecision.Ignore)
        assertTrue(filter.evaluateGitHub("star", repoPayload) is WebhookDecision.Ignore)
        assertTrue(filter.evaluateGitHub("push", """{"ref":"refs/heads/main"}""") is WebhookDecision.Ignore)
        assertTrue(filter.evaluateGitHub("push", "not-json") is WebhookDecision.Malformed)
    }

    @Test
    fun `GitLab push events enqueue default branch human pushes and ignore bot or renovate pushes`() {
        val validGitLabPush = """
            {
              "object_kind": "push",
              "ref": "refs/heads/main",
              "after": "1234567890abcdef1234567890abcdef12345678",
              "user_username": "developer",
              "project": {
                "path_with_namespace": "gitlab-org/subgroup/koshin",
                "default_branch": "main"
              }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Enqueue("gitlab-org/subgroup/koshin"),
            filter.evaluateGitLab("Push Hook", validGitLabPush),
        )

        val gitlabBotPush = """
            {
              "object_kind": "push",
              "ref": "refs/heads/main",
              "after": "1234567890abcdef1234567890abcdef12345678",
              "user_username": "renovate",
              "project": { "path_with_namespace": "gitlab-org/koshin", "default_branch": "main" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitLab("Push Hook", gitlabBotPush) is WebhookDecision.Ignore)

        val gitlabRenovateBranchPush = """
            {
              "object_kind": "push",
              "ref": "refs/heads/renovate/all-minor",
              "after": "1234567890abcdef1234567890abcdef12345678",
              "user_username": "developer",
              "project": { "path_with_namespace": "gitlab-org/koshin", "default_branch": "main" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitLab("Push Hook", gitlabRenovateBranchPush) is WebhookDecision.Ignore)
    }

    @Test
    fun `GitLab push events ignore branch deletions and non-default branches`() {
        val gitlabDeletePush = """
            {
              "object_kind": "push",
              "ref": "refs/heads/main",
              "after": "0000000000000000000000000000000000000000",
              "user_username": "developer",
              "project": { "path_with_namespace": "gitlab-org/koshin", "default_branch": "main" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitLab("Push Hook", gitlabDeletePush) is WebhookDecision.Ignore)

        val gitlabNonDefaultBranch = """
            {
              "object_kind": "push",
              "ref": "refs/heads/develop",
              "after": "1234567890abcdef1234567890abcdef12345678",
              "user_username": "developer",
              "project": { "path_with_namespace": "gitlab-org/koshin", "default_branch": "main" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitLab("Push Hook", gitlabNonDefaultBranch) is WebhookDecision.Ignore)
    }

    @Test
    fun `GitLab merge_request events are filtered properly`() {
        val gitlabMrMerged = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "gitlab-org/koshin" },
              "object_attributes": { "action": "merge" }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Enqueue("gitlab-org/koshin"),
            filter.evaluateGitLab("Merge Request Hook", gitlabMrMerged),
        )

        val gitlabMrClosed = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "gitlab-org/koshin" },
              "object_attributes": { "action": "close" }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Enqueue("gitlab-org/koshin"),
            filter.evaluateGitLab("Merge Request Hook", gitlabMrClosed),
        )

        val gitlabMrCheckboxUpdate = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "gitlab-org/koshin" },
              "object_attributes": { "action": "update" },
              "changes": { "description": { "previous": "- [ ] Rebase MR", "current": "- [x] Rebase MR" } }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Enqueue("gitlab-org/koshin"),
            filter.evaluateGitLab("Merge Request Hook", gitlabMrCheckboxUpdate),
        )

        val gitlabMrNonCheckboxUpdate = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "gitlab-org/koshin" },
              "object_attributes": { "action": "update" },
              "changes": { "description": { "previous": "Old description", "current": "New description" } }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitLab("Merge Request Hook", gitlabMrNonCheckboxUpdate) is WebhookDecision.Ignore)

        val gitlabMrOpen = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "gitlab-org/koshin" },
              "object_attributes": { "action": "open" }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitLab("Merge Request Hook", gitlabMrOpen) is WebhookDecision.Ignore)
    }

    @Test
    fun `GitLab issue and unrecognized events are filtered properly`() {
        val gitlabDashboardIssueUpdate = """
            {
              "object_kind": "issue",
              "project": { "path_with_namespace": "gitlab-org/koshin" },
              "object_attributes": {
                "action": "update",
                "title": "Dependency Dashboard"
              },
              "changes": {
                "description": {
                  "previous": "- [ ] Trigger manual run",
                  "current": "- [x] Trigger manual run"
                }
              }
            }
        """.trimIndent()
        assertEquals(
            WebhookDecision.Enqueue("gitlab-org/koshin"),
            filter.evaluateGitLab("Issue Hook", gitlabDashboardIssueUpdate),
        )

        val gitlabOtherIssueUpdate = """
            {
              "object_kind": "issue",
              "project": { "path_with_namespace": "gitlab-org/koshin" },
              "object_attributes": {
                "action": "update",
                "title": "Normal Issue"
              },
              "changes": {
                "description": {
                  "previous": "- [ ] Item",
                  "current": "- [x] Item"
                }
              }
            }
        """.trimIndent()
        assertTrue(filter.evaluateGitLab("Issue Hook", gitlabOtherIssueUpdate) is WebhookDecision.Ignore)

        val notePayload = """{"object_kind":"note","project":{"path_with_namespace":"a/b"}}"""
        assertTrue(filter.evaluateGitLab("Note Hook", notePayload) is WebhookDecision.Ignore)
        assertTrue(filter.evaluateGitLab("Push Hook", "{}") is WebhookDecision.Ignore)
        assertTrue(filter.evaluateGitLab("Push Hook", "invalid-json") is WebhookDecision.Malformed)
    }

    @Test
    fun `hasCheckboxToggled detects checkbox state changes across markdown styles`() {
        assertTrue(WebhookEventFilter.hasCheckboxToggled("- [ ] Task 1\n- [ ] Task 2", "- [x] Task 1\n- [ ] Task 2"))
        assertTrue(WebhookEventFilter.hasCheckboxToggled("* [x] Task 1", "* [ ] Task 1"))
        assertTrue(
            WebhookEventFilter.hasCheckboxToggled(
                "- [x] Task 1\n- [ ] Task 2",
                "- [ ] Task 1\n- [x] Task 2",
            ),
        )
        assertFalse(WebhookEventFilter.hasCheckboxToggled(null, "- [x] Task 1"))
        assertFalse(WebhookEventFilter.hasCheckboxToggled("- [x] Task 1", null))
        assertFalse(WebhookEventFilter.hasCheckboxToggled("- [ ] Task 1", "- [ ] Task 1"))
        assertFalse(WebhookEventFilter.hasCheckboxToggled("Plain text", "Updated plain text"))
    }
}
