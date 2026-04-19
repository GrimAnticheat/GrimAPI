plugins {
    `java-library`
    `maven-publish`
}

// Inherit metadata
group = rootProject.group
version = rootProject.version
description = "GrimAPI-Internal"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api(project(":"))

    compileOnly(libs.annotations)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    // SQLite reference backend (Layer 2).
    compileOnly(libs.sqliteJdbc)

    // Test framework (JUnit 5).
    testImplementation(libs.junitJupiter)
    testImplementation(libs.sqliteJdbc)
    testCompileOnly(libs.annotations)
    testRuntimeOnly(libs.junitPlatformLauncher)
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
    options.encoding = "UTF-8"
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

// Publishing for the Legacy Module
publishing {
    repositories {
        mavenLocal()
    }
    publications.create<MavenPublication>("maven") {
        artifactId = "grim-internal"
        version = project.version.toString()

        from(components["java"])
    }
}

repositories {
    mavenLocal()
    mavenCentral()
}
