import versioning.BuildFlags
import versioning.VersionUtil
import org.gradle.api.JavaVersion
import java.util.Properties

plugins {
    java
    `maven-publish`
    id("net.kyori.indra.git") version "3.1.3"
}

BuildFlags.init(project)

/**
 * ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
 * ┃                        GrimAPI Versioning                    ┃
 * ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
 *
 * Base semver lives in this build script:
 *
 *     val baseVersion = "MAJOR.MINOR.PATCH"
 *
 * A single boolean flag – `release` – decides what the final
 * `project.version` looks like at build time.
 *
 *  ┌─────────────────────────┬────────────────────────────────────┐
 *  │ release flag value      │ produced version string            │
 *  ├─────────────────────────┼────────────────────────────────────┤
 *  │ true                    │ 1.4.2                              │
 *  │ false + on main/master  │ 1.4.2-<10-char-commit>             │
 *  │ false + on any branch   │ 1.4.2-<branch>-<10-char-commit>    │
 *  └─────────────────────────┴────────────────────────────────────┘
 *
 * Ways to set the flag
 *  • Gradle project prop : ./gradlew build -Prelease=true
 *  • JVM/system property : ./gradlew build -Drelease=true
 *  • Environment var     : RELEASE=true ./gradlew build
 *
 * Examples
 *  • `./gradlew build -Prelease=true`
 *        → 1.4.2
 *  • On main branch, no flag
 *        → 1.4.2-3e1c92a1f4
 *  • On branch `fix/login`, no flag
 *        → 1.4.2-fix_login-3e1c92a1f4
 *
 * Semantic compatibility
 *  • Patching within the same MINOR (1.4.x) is binary-compatible.
 *  • Next MINOR may deprecate; next MAJOR may remove APIs.
 */
val baseVersion = "1.0.0"
group = "ac.grim.grimac"
version = VersionUtil.compute(baseVersion)
description = "GrimAPI"

println("⚙️  build flags  →  release=${BuildFlags.release}")
println("📦 project vers →  $version")

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    toolchain.languageVersion.set(JavaLanguageVersion.of(17))
    withSourcesJar()
    withJavadocJar()
    disableAutoTargetJvm()
}

val envProperties = Properties()
file(".env").takeIf { it.exists() }?.reader(Charsets.UTF_8)?.use(envProperties::load)
fun getEnvVar(name: String): String? =
    System.getenv(name) ?: envProperties.getProperty(name)

publishing {
    repositories {
        mavenLocal()
        getEnvVar("MAVEN_REPO_URL")?.let { repoUrl ->
            maven {
                name = getEnvVar("MAVEN_REPO_NAME") ?: ""
                url = uri(repoUrl)
                credentials {
                    username = getEnvVar("MAVEN_USERNAME") ?: ""
                    password = getEnvVar("MAVEN_PASSWORD") ?: ""
                }
            }
        }
    }
    publications.create<MavenPublication>("maven") {
        version = project.version.toString()
        from(components["java"])
    }
}

repositories {
    mavenLocal()
    maven("https://jitpack.io/")
    maven("https://repo.viaversion.com")
    mavenCentral()
}

dependencies {
    compileOnly("org.jetbrains:annotations:24.1.0")
    compileOnly("org.projectlombok:lombok:1.18.36")
    annotationProcessor("org.projectlombok:lombok:1.18.36")
    testCompileOnly("org.projectlombok:lombok:1.18.36")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.36")
}

tasks.test { useJUnitPlatform() }