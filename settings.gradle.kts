rootProject.name = "GrimAPI"

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            from(files("libs.versions.toml"))
        }
    }
}

include(":grim-internal")
include(":grim-bukkit-internal")