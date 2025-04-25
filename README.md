# GrimAPI
A work in progress API for [Grim Anticheat](https://github.com/GrimAnticheat/Grim)

You can find the latest version here: https://repo.grim.ac/#/

Gradle:
```gradle
repositories {
    maven {
        name = "grimacSnapshots"
        url = uri("https://repo.grim.ac/snapshots")
    }
}

dependencies {
    // replace <VERSION> with api version
    compileOnly 'ac.grim.grimac:GrimAPI:<VERSION>'
}
```

Maven:
```xml
<repository>
    <id>grimac-snapshots</id>
    <name>GrimAC Repository</name>
    <url>https://repo.grim.ac/snapshots</url>
</repository>

<!-- replace <VERSION> with api version -->
<dependency>
    <groupId>ac.grim.grimac</groupId>
    <artifactId>GrimAPI</artifactId>
    <version>VERSION</version>
</dependency>
```

Make sure to depend or softdepend on GrimAC in your plugin's `plugin.yml`:
```yml
softdepend: [GrimAC]
```

Obtaining an instance of the API:
```java
RegisteredServiceProvider<GrimAbstractAPI> provider = Bukkit.getServicesManager().getRegistration(GrimAbstractAPI.class);
if (provider != null) {
    GrimAbstractAPI api = provider.getProvider();
}
```

