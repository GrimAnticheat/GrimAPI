rootProject.name = "GrimAPI"

// Local developer overrides, including private Maven credentials.
run {
    val userProps = rootDir.resolve("gradle.user.properties")
    if (userProps.isFile) {
        val loaded = java.util.Properties()
        userProps.inputStream().use { loaded.load(it) }
        for ((key, value) in loaded) {
            System.setProperty(key.toString(), value.toString())
        }
    }
}

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            from(files("libs.versions.toml"))
        }
    }
}

include(":grim-internal")
include(":grim-internal-shims")
include(":grim-bukkit-internal")
include(":grim-fabric-internal")

if (file("workspace.gradle.kts").exists()) apply(from = "workspace.gradle.kts")
