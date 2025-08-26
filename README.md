<div align="center">
 <h1>GrimAPI</h1>
    
 <div>
  <a href="https://github.com/GrimAnticheat/GrimAPI/actions/workflows/gradle-publish.yml">
   <img alt="Workflow" src="https://github.com/GrimAnticheat/GrimAPI/actions/workflows/gradle-publish.yml/badge.svg" />
  </a>
  <a href="https://repo.grim.ac">
   <img alt="Maven repository" src="https://repo.grim.ac/api/badge/latest/snapshots/ac/grim/grimac/GrimAPI?name=Version&style=flat">
  </a>
  <a href="https://discord.grim.ac">
   <img alt="Discord" src="https://img.shields.io/discord/811396969670901800?style=flat&label=Discord&logo=discord">
  </a>
 </div>
 
 <br>
 <div>
  <a href="https://grim.ac">Website</a>
  |
  <a href="https://github.com/GrimAnticheat/Grim/wiki">Wiki</a>
  |
  <a href="https://repo.grim.ac/">Maven</a>
  |
  <a href="https://github.com/GrimAnticheat/Grim">GrimAC</a>
 </div>

 <br>
 <div>
The official developer plugin API for GrimAnticheat
 </div>

</div>

### **Requirements**:
- Java 17 or higher
- A supported environment listed [here](https://github.com/GrimAnticheat/Grim/wiki/Supported-environments)

### **Gradle**:
```gradle
repositories {
    maven {
        name = "grimacSnapshots"
        url = uri("https://repo.grim.ac/snapshots")
    }
}

dependencies {
    // replace %VERSION% with the latest api version
    compileOnly 'ac.grim.grimac:GrimAPI:%VERSION%'
}
```

### **Maven**:
```xml
<repository>
    <id>grimac-snapshots</id>
    <name>GrimAC Repository</name>
    <url>https://repo.grim.ac/snapshots</url>
</repository>

<!-- replace %VERSION% with the latest api version -->
<dependency>
    <groupId>ac.grim.grimac</groupId>
    <artifactId>GrimAPI</artifactId>
    <version>%VERSION%</version>
    <scope>provided</scope>
</dependency>
```

### **Plugin usage**:

> *These examples are assuming you are using a bukkit environment*

Configure your plugin to depend or softdepend on `GrimAC` in your plugin's `plugin.yml`:
```yml
softdepend: [GrimAC]
```

Example of obtaining an instance of the API via bukkit and subscribing to an event:
```java
public class TestPlugin extends JavaPlugin {

    @Override
    public void onEnable() {
        // check if GrimAC is loaded
        if (Bukkit.getPluginManager().isPluginEnabled("GrimAC")) {
            // create a GrimPlugin instance from this plugin
            GrimPlugin plugin = new BasicGrimPlugin(
                    this.getLogger(),
                    this.getDataFolder(),
                    this.getDescription().getVersion(),
                    this.getDescription().getDescription(),
                    this.getDescription().getAuthors()
            );
            // get the provider
            RegisteredServiceProvider<GrimAbstractAPI> provider = Bukkit.getServicesManager().getRegistration(GrimAbstractAPI.class);
            if (provider != null) {
                GrimAbstractAPI api = provider.getProvider();
                // use the event bus to subscribe to FlagEvent
                api.getEventBus().subscribe(plugin, FlagEvent.class, event -> {
                    // broadcast to all players when a player flags a check
                    Bukkit.broadcast(Component.text(
                            event.getPlayer().getName() + " flagged " + event.getCheck().getCheckName()));
                });
            }
        }
    }
}
```
