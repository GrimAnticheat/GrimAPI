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

    // SQLite reference backend.
    compileOnly(libs.sqliteJdbc)

    // Optional plugin-side backends — compileOnly so the shaded plugin jar
    // picks which drivers to bundle. Each backend's init() throws a clear
    // error when its driver isn't on the runtime classpath.
    compileOnly(libs.mysqlJdbc)
    compileOnly(libs.postgresJdbc)
    compileOnly(libs.mongoDriverSync)
    compileOnly(libs.jedis)

    // com.lmax.* to avoid Log4j collisions on the classpath?
    api(libs.disruptor)

    testImplementation(libs.junitApi)
    testImplementation(libs.junitParams)
    testRuntimeOnly(libs.junitEngine)
    testRuntimeOnly(libs.junitLauncher)
    testImplementation(libs.annotations)
    testImplementation(libs.sqliteJdbc)
    testImplementation(libs.mysqlJdbc)
    testImplementation(libs.postgresJdbc)
    testImplementation(libs.mongoDriverSync)
    testImplementation(libs.jedis)
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
    options.encoding = "UTF-8"
}

tasks.withType<Test>().configureEach {
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
