package dev.hononeko.koshin.worker

internal fun Map<String, String>.nonBlank(key: String): String? = this[key]?.trim()?.takeIf { it.isNotEmpty() }

internal fun Map<String, String>.boolean(key: String): Boolean? = nonBlank(key)?.let { raw ->
    requireNotNull(raw.lowercase().toBooleanStrictOrNull()) { "Invalid boolean for $key: '$raw'" }
}

internal fun Map<String, String>.long(key: String): Long? = nonBlank(key)?.let { raw ->
    requireNotNull(raw.toLongOrNull()) { "Invalid integer for $key: '$raw'" }
}

internal fun Map<String, String>.double(key: String): Double? = nonBlank(key)?.let { raw ->
    requireNotNull(raw.toDoubleOrNull()) { "Invalid number for $key: '$raw'" }
}
