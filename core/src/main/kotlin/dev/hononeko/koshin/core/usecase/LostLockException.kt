package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.RepoRef
import kotlinx.coroutines.CancellationException

class LostLockException(val repo: RepoRef, message: String = "Lost lock ownership for repository: $repo") :
    CancellationException(message)
