package versioning

import org.gradle.api.Project

/**
 * Builds a version string using:
 *   • base epoch versioning
 *   • git branch + short commit (via providers.exec for config-cache compat)
 *   • BuildFlags.release
 */
object VersionUtil {

    fun compute(project: Project, baseVersion: String): String {
        val branch = getGitBranch(project)
        val commit = getGitCommitHash(project)

        return when {
            BuildFlags.release                      -> baseVersion
            branch == "main" || branch == "master"  -> "$baseVersion-$commit"
            else                                    -> "$baseVersion-$branch-$commit"
        }
    }
}


private fun getGitCommitHash(project: Project): String {
    return try {
        project.providers.exec {
            commandLine("git", "rev-parse", "--short", "HEAD")
            workingDir(project.projectDir)
            isIgnoreExitValue = true
        }.standardOutput.asText.get().trim()
    } catch (e: Exception) {
        "unknown"
    }
}

private fun getGitBranch(project: Project): String? {
    val branch = try {
        project.providers.exec {
            commandLine("git", "rev-parse", "--abbrev-ref", "HEAD")
            workingDir(project.projectDir)
            isIgnoreExitValue = true
        }.standardOutput.asText.get().trim()
    } catch (e: Exception) {
        return null
    }

    return when (branch) {
        "main", "2.0" -> null
        else -> branch.replace("/", "_")
    }
}