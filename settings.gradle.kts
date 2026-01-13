rootProject.name = "GrimAPI"

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