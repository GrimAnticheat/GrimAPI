# GrimAPI
A work in progress API for [Grim Anticheat](https://github.com/GrimAnticheat/Grim)

You can find the latest version here: https://jitpack.io/#GrimAnticheat/GrimAPI

Gradle:
```gradle
repositories {
    maven("https://jitpack.io/") { // Grim API
        content {
            includeGroup("com.github.grimanticheat")
        }
    }
}

dependencies {
    compileOnly 'com.github.grimanticheat:grimapi:VERSION'
}
```

Maven:
```xml
<repository>
   <id>jitpack.io</id>
   <url>https://jitpack.io</url>
</repository>
  
<dependency>
   <groupId>com.github.grimanticheat</groupId>
   <artifactId>GrimAPI</artifactId>
   <version>VERSION</version>
   <scope>provided</scope>
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

