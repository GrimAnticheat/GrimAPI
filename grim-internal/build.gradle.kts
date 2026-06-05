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
    compileOnly(libs.hikaricp)

    // com.lmax.* ring-buffer writer path used by the datastore. Shaded +
    // relocated in prod builds; version pinned to match Paper's bundled
    // 3.4.4 so no-relocate debug builds don't double up on the classpath.
    api(libs.disruptor)

    testImplementation(libs.annotations)
    testImplementation(libs.junitJupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation(libs.sqliteJdbc)
    testImplementation(libs.mysqlJdbc)
    testImplementation(libs.postgresJdbc)
    testImplementation(libs.mongoDriverSync)
    testImplementation(libs.jedis)
    testImplementation(libs.hikaricp)
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
    options.encoding = "UTF-8"
}

tasks.test {
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

val codecBindingsDir = layout.buildDirectory.dir("generated/codec-bindings")
val generateCodecBindings = tasks.register<JavaExec>("generateCodecBindings") {
    group = "build"
    description = "Capture v2 codec bindings for builtin persistent records"
    dependsOn(tasks.named("compileJava"))
    classpath = sourceSets["main"].output.classesDirs + sourceSets["main"].compileClasspath
    mainClass.set("ac.grim.grimac.internal.storage.codec.gen.CodecBindingCaptureTool")
    val outFile = codecBindingsDir.get().file("META-INF/grim/codec-bindings.tsv").asFile
    args(outFile.absolutePath)
    outputs.file(outFile)
    outputs.upToDateWhen { false }
}

sourceSets["main"].output.dir(mapOf("builtBy" to generateCodecBindings), codecBindingsDir)
