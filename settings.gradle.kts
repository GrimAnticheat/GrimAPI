rootProject.name = "GrimAPI"

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            from(files("libs.versions.toml"))
        }
    }
}

include(":grim-bukkit-api")
include(":grim-bukkit-internal")