package dev.hononeko.koshin.proxy

import io.ktor.http.ContentType
import io.ktor.http.HeaderValueParam
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

/**
 * koshin's armored OpenPGP public key (`RENOVATE_PUBLIC_KEY_PATH`), served at `GET /api/v1/encryption-key` so tenants
 * can encrypt their registry credentials for Renovate's `encrypted` config. Read on each request, so a key
 * rotated in a mounted Secret is served without a restart; [load] checks the file once at startup.
 */
class EncryptionKey(private val path: Path, private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO) {

    /** Throws an [IOException] if the key can't be read or is blank. */
    suspend fun armored(): String = withContext(ioDispatcher) { read() }

    private fun read(): String = Files.readString(path).also { key ->
        if (key.isBlank()) throw IOException("$path is blank")
    }

    companion object {
        val CONTENT_TYPE = ContentType("application", "pgp-keys", listOf(HeaderValueParam("charset", "utf-8")))

        /** The key at [path], if it's readable and not blank. */
        fun load(path: String): EncryptionKey = EncryptionKey(Path.of(path)).also { key ->
            try {
                key.read()
            } catch (e: IOException) {
                throw IllegalArgumentException("Cannot read RENOVATE_PUBLIC_KEY_PATH $path: ${e.message}", e)
            }
        }
    }
}
