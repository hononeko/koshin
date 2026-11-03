package dev.hononeko.koshin.core.ports

/** Workers started on demand that drain the queue and exit (KSH-32), e.g. Kubernetes `Job`s. */
interface BurstWorkerPort {
    /** Burst workers started and not finished yet, including ones still starting. */
    suspend fun active(): Int

    suspend fun start(count: Int)
}
