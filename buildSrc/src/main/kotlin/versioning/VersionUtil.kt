package versioning

import java.io.ByteArrayOutputStream

/**
 * Builds a version string using:
 *   • base semver
 *   • git branch + short commit (via indra.git)
 *   • BuildFlags.release
 */
object VersionUtil {

    fun compute(baseVersion: String): String {
        val branch = getGitBranch()
        val commit = getGitCommitHash()

        return when {
            BuildFlags.release                      -> baseVersion
            branch == "main" || branch == "master"  -> "$baseVersion-$commit"
            else                                    -> "$baseVersion-$branch-$commit"
        }
    }
}


/**
 * Retrieves the current Git commit as a short hash.
 */
private fun getGitCommitHash(): String {
    val stdout = ByteArrayOutputStream()
    ProcessBuilder("git", "rev-parse", "--short", "HEAD")
        .redirectErrorStream(true)
        .start()
        .apply { waitFor() }
        .inputStream
        .use { stdout.writeBytes(it.readAllBytes()) }
    return stdout.toString().trim()
}

/**
 * Returns the current Git branch, sanitised for use in file names.
 * If the branch is "main" or "2.0", returns null.
 *
 * Any slash (/) in the branch name is replaced with an underscore (_)
 * to avoid filesystem issues.
 */
private fun getGitBranch(): String? {
    val stdout = ByteArrayOutputStream()

    ProcessBuilder("git", "rev-parse", "--abbrev-ref", "HEAD")
        .redirectErrorStream(true)
        .start()
        .apply { waitFor() }
        .inputStream.use { stdout.writeBytes(it.readAllBytes()) }

    val branch = stdout.toString().trim()

    return when (branch) {
        "main", "2.0" -> null                    // ← ignore these branches
        else           -> branch.replace("/", "_")
    }
}