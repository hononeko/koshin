package dev.hononeko.koshin.core.model

import kotlin.time.Duration

sealed interface JobExecutionResult {
    data object Success : JobExecutionResult
    data class Failed(val exitCode: Int) : JobExecutionResult
    data object TimedOut : JobExecutionResult
    data class RateLimited(val retryAfter: Duration? = null) : JobExecutionResult
}
