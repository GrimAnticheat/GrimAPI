# GrimAPI
A work in progress API for [Grim Anticheat](https://github.com/MWHunter/Grim)

You can find the latest version here: https://jitpack.io/#MWHunter/GrimAPI

Gradle:
```gradle
repositories {
    maven { url = 'https://jitpack.io/' }
}

dependencies {
    compileOnly 'com.github.MWHunter:GrimAPI:VERSION'
}
```

Maven:
```xml
<repository>
   <id>jitpack.io</id>
   <url>https://jitpack.io</url>
</repository>
  
<dependency>
   <groupId>com.github.MWHunter</groupId>
   <artifactId>GrimAPI</artifactId>
   <version>VERSION</version>
   <scope>provided</scope>
</dependency>
```

Obtaining an instance of the API:
```java
RegisteredServiceProvider<GrimAbstractAPI> provider = Bukkit.getServicesManager().getRegistration(GrimAbstractAPI.class);
if (provider != null) {
    GrimAbstractAPI api = provider.getProvider();
}
```

