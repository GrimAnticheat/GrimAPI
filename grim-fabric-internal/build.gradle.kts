plugins {
    `java-library`
    `maven-publish`
}

// Inherit metadata
group = rootProject.group
version = rootProject.version
description = "GrimAPI-Fabric-Internal"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api(project(":"))
    api(project(":grim-internal"))

    // turns out JVM lazy loading means we're fine on Fabric, and org.bukkit.entity.Player not existing doesn't cause a CNF
    // Uncomment and include the shim in the future if necessary
//    api(project(":grim-internal-shims"))

    compileOnly(libs.annotations)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
    options.encoding = "UTF-8"
}

// Publishing for the Legacy Module
publishing {
    repositories {
        mavenLocal()
    }
    publications.create<MavenPublication>("maven") {
        artifactId = "grim-fabric-internal"
        version = project.version.toString()

        from(components["java"])
    }
}

repositories {
    mavenLocal()
    mavenCentral()
}