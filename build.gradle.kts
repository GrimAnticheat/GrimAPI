import versioning.BuildFlags // Assuming these helpers are compatible
import versioning.VersionUtil // Assuming these helpers are compatible
import org.gradle.api.JavaVersion
import java.util.Properties

plugins {
    `java-library`
    `maven-publish`
    id("net.kyori.indra.git") version "3.1.3" // Keep for Git info access
}

BuildFlags.init(project) // Initialize your build flags helper

/**
 * ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
 * ┃               GrimAPI Epoch Versioning Scheme                ┃
 * ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
 *
 * Base versioning uses a 4-part Epoch Versioning scheme, similar to
 * Semantic Versioning but with an added Epoch component:
 *
 *     val baseVersion = "EPOCH.MAJOR.MINOR.PATCH" // e.g., "1.0.0.0"
 *
 * Version Components:
 *  - EPOCH: Incremented for massive, foundational, backward-incompatible
 *           changes that represent a fundamental shift in the project's
 *           architecture or core philosophy (e.g., moving from a platform-
 *           specific API to platform-independent). RESETS Major, Minor,
 *           and Patch to 0. (Example: 0.5.1.2 -> 1.0.0.0)
 *  - MAJOR: Incremented for backward-incompatible API changes within
 *           the current EPOCH. RESETS Minor and Patch to 0.
 *           (Example: 1.2.3.4 -> 1.3.0.0)
 *  - MINOR: Incremented for adding backward-compatible functionality
 *           within the current EPOCH.MAJOR. RESETS Patch to 0.
 *           (Example: 1.3.4.5 -> 1.3.5.0)
 *  - PATCH: Incremented for backward-compatible bug fixes within the
 *           current EPOCH.MAJOR.MINOR. (Example: 1.3.5.0 -> 1.3.5.1)
 *
 * The base EPOCH.MAJOR.MINOR.PATCH version is manually updated in this
 * file for new releases according to the rules above.
 *
 * --- Build Metadata & Release Flag ---
 *
 * A single boolean flag – `release` – decides what the final
 * `project.version` looks like at build time. For non-release builds,
 * build metadata (commit hash and optionally branch name) is appended
 * to the base version for identification.
 *
 *  ┌─────────────────────────┬────────────────────────────────────┐
 *  │ release flag value      │ produced version string            │
 *  ├─────────────────────────┼────────────────────────────────────┤
 *  │ true                    │ 1.0.0.0                            │ // Example Release
 *  │ false + on main/master  │ 1.0.0.0-<10-char-commit>           │ // Example Dev on Main
 *  │ false + on any branch   │ 1.0.0.0-<branch>-<10-char-commit>  │ // Example Dev on Branch
 *  └─────────────────────────┴────────────────────────────────────┘
 *
 * Ways to set the flag
 *  • Gradle project prop : ./gradlew build -Prelease=true
 *  • JVM/system property : ./gradlew build -Drelease=true
 *  • Environment var     : RELEASE=true ./gradlew build
 *
 * Examples (assuming baseVersion = "1.0.0.0")
 *  • `./gradlew build -Prelease=true`
 *        → 1.0.0.0
 *  • On main branch, no flag
 *        → 1.0.0.0-3e1c92a1f4
 *  • On branch `feat/new-api`, no flag
 *        → 1.0.0.0-feat_new-api-3e1c92a1f4
 *
 * Compatibility Rules:
 *  - Different EPOCH numbers are fundamentally incompatible.
 *  - Within the same EPOCH:
 *      - Different MAJOR numbers indicate backward-incompatible API changes.
 *      - Same MAJOR, different MINOR indicate backward-compatible additions.
 *      - Same MAJOR and MINOR, different PATCH indicate backward-compatible fixes.
 *  - Development builds (with -<commit> suffix) offer no stability guarantees.
 */
val baseVersion = project.findProperty("version") as String

// --- Standard Project Configuration ---
group = "ac.grim.grimac" // Or your desired group ID
// VersionUtil appends commit/branch metadata if not a release build
version = VersionUtil.compute(baseVersion)
description = "GrimAPI"

println("⚙️  Build flags     → release=${BuildFlags.release}")
println("📦 Project version → $version")

// --- Java Configuration ---
java {
    targetCompatibility = JavaVersion.VERSION_1_8
    sourceCompatibility = JavaVersion.VERSION_1_8
    withSourcesJar()
    withJavadocJar()
    // disableAutoTargetJvm() // Keep if needed
}

// --- Environment Variable Loading for Publishing ---
val envProperties = Properties()
file(".env").takeIf { it.exists() }?.reader(Charsets.UTF_8)?.use(envProperties::load)
fun getEnvVar(name: String): String? =
    System.getenv(name) ?: envProperties.getProperty(name)

// --- Publishing Configuration ---
allprojects {
    // Ensure the plugin is applied so we can access the 'publishing' extension
    apply(plugin = "maven-publish")

    publishing {
        repositories {
            mavenLocal()
            getEnvVar("MAVEN_REPO_URL")?.let { repoUrl ->
                maven {
                    name = getEnvVar("MAVEN_REPO_NAME") ?: "CustomMaven"
                    url = uri(repoUrl)
                    credentials {
                        username = getEnvVar("MAVEN_USERNAME") ?: ""
                        password = getEnvVar("MAVEN_PASSWORD") ?: ""
                    }
                }
            }
        }
    }
}

publishing {
    publications.create<MavenPublication>("maven") {
        // Use the calculated version (EPOCH.MAJOR.MINOR.PATCH + optional metadata)
        version = project.version.toString()
        from(components["java"])
    }
}


// --- Repositories for Dependencies ---
repositories {
    mavenLocal()
    maven("https://jitpack.io/")
    maven("https://repo.viaversion.com")
    mavenCentral()
}

dependencies {
    compileOnly(libs.annotations)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)
}


tasks.test {
    useJUnitPlatform()
}