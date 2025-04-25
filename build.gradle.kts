import java.util.Properties

plugins {
    java
    `maven-publish`
    id("net.kyori.indra.git") version "3.1.3"
}

group = "ac.grim.grimac"
version = "1.0"
description = "GrimAPI"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    toolchain.languageVersion.set(JavaLanguageVersion.of(17))
    withSourcesJar()
    withJavadocJar()
    disableAutoTargetJvm()
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
    options.encoding = "UTF-8"
}

// Load environment variables from .env file
val envProperties = Properties()
val envFile = file(".env")
if (envFile.exists()) {
    envFile.reader(Charsets.UTF_8).use { reader ->
        envProperties.load(reader)
    }
}

fun getEnvVar(name: String): String? =
    System.getenv(name) ?: envProperties.getProperty(name)

publishing {
    repositories {
        mavenLocal()

        val repoUrl = getEnvVar("MAVEN_REPO_URL")
        if (repoUrl != null) {
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

    publications {
        create<MavenPublication>("maven") {
            val commitName = "any-" + (project.extensions.getByType(net.kyori.indra.git.IndraGitExtension::class)
                .commit()?.name()?.substring(0, 10) ?: "unknown")
            version = commitName
            from(components["java"])
        }
    }
}

repositories {
    mavenLocal()
    maven("https://hub.spigotmc.org/nexus/content/repositories/snapshots/")
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

tasks.test {
    useJUnitPlatform()
}