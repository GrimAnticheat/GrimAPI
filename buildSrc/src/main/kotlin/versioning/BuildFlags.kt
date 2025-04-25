package versioning

import org.gradle.api.Project

/**
 * Singleton that exposes build-time flags (release, shadePE, relocate, …)
 * and can read them from:
 *   ① System property   ( ‑Dflag=value )
 *   ② Gradle/project    ( ‑Pflag=value or gradle.properties )
 *   ③ Environment var   ( FLAG=value )
 *
 * Initialise once from the root build script with  BuildFlags.init(project)
 */
object BuildFlags {

    // we inject the root project from build.gradle.kts
    private lateinit var root: Project
    fun init(project: Project) { root = project }

    /* unified getter ----------------------------------------------------- */
    private fun raw(key: String): String? =
        System.getProperty(key)                       // ① JVM   (-D)
            ?: root.findProperty(key)?.toString()     // ② Gradle (-P or gradle.properties)
            ?: System.getenv(key.uppercase())         // ③ ENV

    private fun bool(key: String, default: Boolean) =
        raw(key)?.toBooleanStrictOrNull() ?: default

    /* public, strongly typed flags --------------------------------------- */
    val release  get() = bool("release" , default = false)
    val shadePE  get() = bool("shadePE" , default = true)
    val relocate get() = bool("relocate", default = true)
}