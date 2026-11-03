package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import io.nats.client.JetStream
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.PublishOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.ConsumerInfo
import io.nats.client.api.MessageInfo
import io.nats.client.api.StreamInfoOptions
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * The work queue's consumers, one per credential scope (KSH-24): `koshin-workers-<platform>-<scope>`, filtering
 * `renovate.jobs.<platform>.<scope>`. Publishers create their scope's consumer as they publish ([ensure]); the proxy's
 * housekeeping creates any still missing for stored jobs ([ensureForStoredJobs]), deletes idle ones ([collectIdle])
 * and moves jobs off the per-platform queue of earlier versions ([migrateLegacy]).
 */
class NatsScopeConsumers(
    private val jsm: JetStreamManagement,
    private val config: NatsConfig,
    /** Needed only to [migrateLegacy]. */
    private val js: JetStream? = null,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val now: () -> Instant = Instant::now,
) {
    private val logger = LoggerFactory.getLogger(NatsScopeConsumers::class.java)

    /** Creates [scope]'s consumer, or updates it to the current settings. */
    fun ensure(scope: CredentialScope): ConsumerInfo {
        val name = config.consumerNameFor(scope)
        val consumerConfig = ConsumerConfiguration.builder()
            .durable(name)
            .name(name)
            .filterSubject(config.jobsSubjectFor(scope))
            .ackPolicy(AckPolicy.Explicit)
            .ackWait(config.ackWait.toJavaDuration())
            .maxDeliver(config.maxDeliver)
            .build()
        return jsm.addOrUpdateConsumer(config.jobsStreamName, consumerConfig)
    }

    /** The consumers of [platforms]' scopes, by scope. */
    fun list(platforms: Collection<Platform> = Platform.entries): Map<CredentialScope, ConsumerInfo> =
        jsm.getConsumers(config.jobsStreamName)
            .mapNotNull { info -> scopeOf(info)?.takeIf { it.platform in platforms }?.let { it to info } }
            .toMap()

    /** The scope [info] is the consumer of, or `null` for any other consumer (e.g. a legacy per-platform one). */
    private fun scopeOf(info: ConsumerInfo): CredentialScope? = info.consumerConfiguration.filterSubject
        ?.let(config::scopeOf)
        ?.takeIf { config.consumerNameFor(it) == info.name }

    /**
     * Creates the consumer of every scope that has jobs stored but none, e.g. one [collectIdle] deleted just as a job
     * was published to it. Returns how many it created.
     */
    fun ensureForStoredJobs(): Int {
        val existing = jsm.getConsumerNames(config.jobsStreamName).toSet()
        val missing = storedJobsBySubject().keys.mapNotNull(config::scopeOf)
            .distinct()
            .filter { config.consumerNameFor(it) !in existing }
        missing.forEach { scope ->
            logger.info("Creating the missing consumer of {}, which has jobs queued", scope)
            ensure(scope)
        }
        return missing.size
    }

    /** How many jobs each subject of the work queue holds, delivered or not. */
    fun storedJobsBySubject(): Map<String, Long> =
        jsm.getStreamInfo(config.jobsStreamName, StreamInfoOptions.filterSubjects(config.jobsSubject))
            .streamState
            .subjectMap

    /**
     * Deletes the scope consumers that have no job, no fetch waiting and no delivery, ACK or creation for [idleTtl], so
     * uninstalled tenants leave nothing behind. Returns how many it deleted. A job published just as its consumer is
     * deleted waits for [ensureForStoredJobs].
     */
    fun collectIdle(): Int = collectIdle(config.consumerIdleTtl)

    /** [collectIdle] with another TTL. */
    fun collectIdle(idleTtl: Duration): Int {
        if (!idleTtl.isPositive()) return 0
        val cutoff = now().minus(idleTtl.toJavaDuration())
        return list().count { (scope, info) ->
            isIdle(info, cutoff) &&
                delete(info.name).also { deleted ->
                    if (deleted) logger.info("Deleted the consumer of {}: no job for {}", scope, idleTtl)
                }
        }
    }

    private fun isIdle(info: ConsumerInfo, cutoff: Instant): Boolean {
        val lastActive = listOfNotNull(info.creationTime, info.delivered.lastActive, info.ackFloor.lastActive)
            .maxOfOrNull { it.toInstant() }
        return info.numPending == 0L &&
            info.numAckPending == 0L &&
            info.numWaiting == 0L &&
            lastActive != null &&
            lastActive.isBefore(cutoff)
    }

    /**
     * Moves the jobs of [platforms] on the per-platform subjects of earlier versions (`renovate.jobs.<platform>`) to
     * their scope's subject, after deleting the per-platform consumers so no worker is still handed them. Runs on every
     * pass, since a worker or proxy of the earlier version can still publish there during a rolling upgrade. A job
     * an earlier worker is running then runs again, or is coalesced on its lock. Returns how many it moved.
     */
    fun migrateLegacy(platforms: Collection<Platform> = Platform.entries): Int = platforms.sumOf { platform ->
        if (delete(config.legacyConsumerNameFor(platform))) {
            logger.info("Deleted the per-platform consumer {}: jobs are queued per credential scope", platform)
        }
        moveLegacyJobs(platform)
    }

    /** Another proxy replica may move the same job: the message ID dedupes its copy, and its delete is a no-op. */
    private fun moveLegacyJobs(platform: Platform): Int {
        val subject = config.legacyJobsSubjectFor(platform)
        val ensured = mutableSetOf<CredentialScope>()
        var moved = 0
        var next = nextStored(1L, subject)
        while (next != null) {
            val scope = scopeOfJob(next) ?: CredentialScope.Token(platform)
            if (ensured.add(scope)) ensure(scope)
            // A new message ID: the original one may still be in the duplicate window
            val options = PublishOptions.builder().messageId("migrated:${config.jobsStreamName}:${next.seq}").build()
            val js = checkNotNull(js) { "Moving jobs needs a JetStream context" }
            js.publish(config.jobsSubjectFor(scope), next.data, options)
            deleteMessage(next.seq)
            moved++
            next = nextStored(next.seq + 1, subject)
        }
        if (moved > 0) logger.info("Moved {} job(s) from {} to their credential scope's queue", moved, subject)
        return moved
    }

    /** An unreadable job goes to the platform's token scope, whose worker dead-letters it. */
    private fun scopeOfJob(message: MessageInfo): CredentialScope? = try {
        json.decodeFromString(RenovateJobEvent.serializer(), message.data?.decodeToString().orEmpty()).scope
    } catch (_: SerializationException) {
        null
    } catch (_: IllegalArgumentException) {
        null
    }

    private fun nextStored(fromSeq: Long, subject: String): MessageInfo? = try {
        jsm.getNextMessage(config.jobsStreamName, fromSeq, subject)?.takeIf { it.isMessage && it.data != null }
    } catch (e: JetStreamApiException) {
        if (!e.isNotFound()) throw e
        null
    }

    private fun deleteMessage(seq: Long) {
        try {
            jsm.deleteMessage(config.jobsStreamName, seq)
        } catch (e: JetStreamApiException) {
            if (!e.isNotFound()) throw e
        }
    }

    /** False if it was already gone. */
    private fun delete(consumer: String): Boolean = try {
        jsm.deleteConsumer(config.jobsStreamName, consumer)
    } catch (e: JetStreamApiException) {
        if (!e.isNotFound()) throw e
        false
    }

    private fun JetStreamApiException.isNotFound() = apiErrorCode in NOT_FOUND_CODES || errorCode in NOT_FOUND_CODES

    private companion object {
        /** No message, no consumer, no stream. */
        val NOT_FOUND_CODES = setOf(10037, 10014, 10059)
    }
}
