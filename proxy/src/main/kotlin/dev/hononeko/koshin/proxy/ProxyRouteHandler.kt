package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.RepositoryDiscoveryPort
import dev.hononeko.koshin.core.usecase.IngestWebhookUseCase
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.header
import io.ktor.server.response.respond
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.SerializationException
import org.slf4j.LoggerFactory

internal class ProxyRouteHandler(
    private val config: ProxyConfig,
    private val eventFilter: WebhookEventFilter,
    private val ingestUseCase: IngestWebhookUseCase,
    private val discoveryPort: RepositoryDiscoveryPort?,
) {
    private val logger = LoggerFactory.getLogger(ProxyRouteHandler::class.java)

    suspend fun handleGitHubWebhook(call: ApplicationCall) {
        if (!config.isPlatformEnabled(Platform.GITHUB)) {
            call.respond(HttpStatusCode.Forbidden, ErrorResponse("GitHub platform is not enabled"))
            return
        }

        val payloadBytes = call.receiveBoundedBytes(config.maxRequestBodyBytes) ?: return
        val signatureHeader = call.request.header("X-Hub-Signature-256")
        if (!SignatureVerifier.verifyGitHubSignature(payloadBytes, signatureHeader, config.githubWebhookSecret)) {
            logger.warn("Rejected GitHub webhook due to invalid or missing X-Hub-Signature-256")
            call.respond(HttpStatusCode.Forbidden, ErrorResponse("Invalid webhook signature"))
        } else {
            val eventHeader = call.request.header("X-GitHub-Event")
            val deliveryIdHeader = call.request.header("X-GitHub-Delivery")
            val rawPayload = payloadBytes.toString(Charsets.UTF_8)

            processWebhookDecision(
                call = call,
                platform = Platform.GITHUB,
                deliveryId = deliveryIdHeader,
                decision = eventFilter.evaluateGitHub(eventHeader, rawPayload),
            )
        }
    }

    suspend fun handleGitLabWebhook(call: ApplicationCall) {
        if (!config.isPlatformEnabled(Platform.GITLAB)) {
            call.respond(HttpStatusCode.Forbidden, ErrorResponse("GitLab platform is not enabled"))
            return
        }

        val tokenHeader = call.request.header("X-Gitlab-Token")
        if (!SignatureVerifier.verifyGitLabToken(tokenHeader, config.gitlabWebhookSecret)) {
            logger.warn("Rejected GitLab webhook due to invalid or missing X-Gitlab-Token")
            call.respond(HttpStatusCode.Forbidden, ErrorResponse("Invalid webhook token"))
        } else {
            val payloadBytes = call.receiveBoundedBytes(config.maxRequestBodyBytes) ?: return
            val eventHeader = call.request.header("X-Gitlab-Event")
            val deliveryIdHeader = call.request.header("X-Gitlab-Event-UUID")
            val rawPayload = payloadBytes.toString(Charsets.UTF_8)

            processWebhookDecision(
                call = call,
                platform = Platform.GITLAB,
                deliveryId = deliveryIdHeader,
                decision = eventFilter.evaluateGitLab(eventHeader, rawPayload),
            )
        }
    }

    suspend fun handleTriggerRequest(call: ApplicationCall) {
        val authHeader = call.request.header("Authorization")
        if (!SignatureVerifier.verifyBearerToken(authHeader, config.internalTriggerToken)) {
            logger.warn("Rejected /api/v1/trigger request due to invalid or missing Bearer token")
            call.respond(HttpStatusCode.Unauthorized, ErrorResponse("Unauthorized"))
            return
        }

        val bodyBytes = call.receiveBoundedBytes(config.maxRequestBodyBytes) ?: return
        val rawBody = bodyBytes.toString(Charsets.UTF_8)
        val targetRepos = resolveTriggerRepositories(call, rawBody)
        if (targetRepos != null) {
            enqueueScheduledRepositories(call, targetRepos)
        }
    }

    private suspend fun processWebhookDecision(
        call: ApplicationCall,
        platform: Platform,
        deliveryId: String?,
        decision: WebhookDecision,
    ) {
        when (decision) {
            is WebhookDecision.Malformed -> {
                call.respond(HttpStatusCode.BadRequest, ErrorResponse(decision.reason))
            }

            is WebhookDecision.Ignore -> {
                logger.debug("Filtered {} webhook (delivery={}): {}", platform, deliveryId, decision.reason)
                call.respond(
                    HttpStatusCode.Accepted,
                    WebhookResponse(status = "ignored", enqueued = false, reason = decision.reason),
                )
            }

            is WebhookDecision.Enqueue -> {
                enqueueWebhookEvent(call, platform, decision.repository, deliveryId)
            }
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun enqueueWebhookEvent(
        call: ApplicationCall,
        platform: Platform,
        repository: String,
        deliveryId: String?,
    ) {
        val validatedRepo = try {
            RepoRef(platform, repository.trim())
        } catch (e: IllegalArgumentException) {
            logger.warn("Invalid {} repository format '{}': {}", platform, repository, e.message)
            call.respond(HttpStatusCode.BadRequest, ErrorResponse(e.message ?: "Invalid repository"))
            return
        }

        try {
            val event = ingestUseCase.execute(
                platform = validatedRepo.platform,
                repository = validatedRepo.repository,
                deliveryId = deliveryId,
                triggerReason = TriggerReason.WEBHOOK,
            )
            call.respond(
                HttpStatusCode.Accepted,
                WebhookResponse(status = "accepted", enqueued = true, deliveryId = event.deliveryId),
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.error("Failed to publish {} webhook job for {}", platform, validatedRepo.repository, e)
            call.respond(HttpStatusCode.ServiceUnavailable, ErrorResponse("Queue unavailable"))
        }
    }

    private suspend fun resolveTriggerRepositories(call: ApplicationCall, rawBody: String): List<RepoRef>? {
        val parsedRequest = parseAndValidateTriggerRequest(call, rawBody) ?: return null
        return if (!parsedRequest.discover) {
            parsedRequest.explicitRepos.distinct()
        } else {
            val discovered = discoverEnabledRepositories(call)
            discovered?.let { (parsedRequest.explicitRepos + it).distinct() }
        }
    }

    private data class ValidatedTriggerInput(val explicitRepos: List<RepoRef>, val discover: Boolean)

    private suspend fun parseAndValidateTriggerRequest(
        call: ApplicationCall,
        rawBody: String,
    ): ValidatedTriggerInput? = try {
        val triggerRequest = ProxyJson.decodeFromString(TriggerRequest.serializer(), rawBody)
        require(triggerRequest.repositories.isNotEmpty() || triggerRequest.discover) {
            "Trigger request must specify non-empty 'repositories' or 'discover: true'"
        }
        val explicitRepos = triggerRequest.repositories.map { item ->
            val platform = Platform.fromString(item.platform)
            require(config.isPlatformEnabled(platform)) {
                "Platform '${platform.name.lowercase()}' is not enabled"
            }
            RepoRef(platform, item.repository.trim())
        }
        ValidatedTriggerInput(explicitRepos, triggerRequest.discover)
    } catch (e: SerializationException) {
        logger.warn("Malformed /api/v1/trigger JSON payload: {}", e.message)
        call.respond(HttpStatusCode.BadRequest, ErrorResponse("Malformed trigger request payload"))
        null
    } catch (e: IllegalArgumentException) {
        logger.warn("Invalid /api/v1/trigger request: {}", e.message)
        call.respond(HttpStatusCode.BadRequest, ErrorResponse(e.message ?: "Invalid trigger request"))
        null
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun discoverEnabledRepositories(call: ApplicationCall): List<RepoRef>? {
        val port = discoveryPort
        if (port == null) {
            logger.warn("Rejected /api/v1/trigger discover=true because repository discovery is not configured")
            call.respond(
                HttpStatusCode.ServiceUnavailable,
                ErrorResponse("Repository discovery is not configured"),
            )
            return null
        }
        return try {
            port.discover().filter { config.isPlatformEnabled(it.platform) }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.error("Repository discovery failed during /api/v1/trigger", e)
            call.respond(
                HttpStatusCode.InternalServerError,
                ErrorResponse("Repository discovery failed"),
            )
            null
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun enqueueScheduledRepositories(call: ApplicationCall, targetRepos: List<RepoRef>) {
        try {
            for (repoRef in targetRepos) {
                ingestUseCase.execute(
                    platform = repoRef.platform,
                    repository = repoRef.repository,
                    deliveryId = null,
                    triggerReason = TriggerReason.SCHEDULE,
                )
            }
            call.respond(
                HttpStatusCode.Accepted,
                TriggerResponse(status = "accepted", enqueued = targetRepos.size),
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.error("Failed to publish scheduled trigger jobs", e)
            call.respond(HttpStatusCode.ServiceUnavailable, ErrorResponse("Queue unavailable"))
        }
    }
}
