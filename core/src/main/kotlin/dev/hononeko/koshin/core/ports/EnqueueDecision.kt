package dev.hononeko.koshin.core.ports

/** What ingest does with an event (KSH-30): publish it, or coalesce it into a queued or running job. */
enum class EnqueueDecision {
    /** The repo was idle and is now marked queued: publish the event. */
    ENQUEUE,

    /** A job for the repo is already queued and will see the change. */
    ALREADY_QUEUED,

    /** A run is in progress: its dirty flag now schedules one follow-up run. */
    MARKED_DIRTY,
}
