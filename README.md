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
  <a href="https://github.com/GrimAnticheat/Grim/wiki/Developer-API">Wiki</a>
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


### Wiki
You can find more documentation and examples of how to use the API on the [wiki](https://github.com/GrimAnticheat/Grim/wiki/Developer-API).

### **Gradle**:
```kt
repositories {
    maven {
        name = "grimacSnapshots"
        url = uri("https://repo.grim.ac/snapshots")
    }
}
dependencies {
    // replace %VERSION% with the latest API version
    compileOnly("ac.grim.grimac:GrimAPI:%VERSION%")
}
```

### **Maven**:
```xml
<repository>
  <id>grimac-snapshots</id>
  <name>GrimAC's Maven Repository</name>
  <url>https://repo.grim.ac/snapshots</url>
</repository>
<!-- replace %VERSION% with the latest API version -->
<dependency>
  <groupId>ac.grim.grimac</groupId>
  <artifactId>GrimAPI</artifactId>
  <version>%VERSION%</version>
  <scope>provided</scope>
</dependency>
```

### **Subscribing to events** (1.3+)

The current API dispatches each event through its own `EventChannel`. Grab the
channel via `bus.get(EventClass.class)` and subscribe with its fluent
`on…(…)` method. Handlers take the event's fields as positional parameters
— no pooled event object, no class-keyed registry lookup on the hot path.

Every subscribe takes a `GrimPlugin` as the first argument. The bus tracks
subscriptions by that plugin and sweeps them automatically on plugin disable
via `bus.unregisterAllListeners(plugin)` — same lifecycle model as Bukkit's
`HandlerList`. Resolve the `GrimPlugin` once at plugin enable and reuse it:

```java
GrimAbstractAPI api = GrimAPIProvider.get();
EventBus bus = api.getEventBus();
GrimPlugin grim = api.getGrimPlugin(this);   // resolve once; `this` can be a Bukkit
                                             // JavaPlugin, a Fabric ModContainer, etc.

// Non-cancellable — observational
bus.get(GrimTransactionSendEvent.class)
   .onTransactionSend(grim, (user, id, ts) -> {
       getLogger().info("tx-send " + id + " for " + user.getName());
   });

// Cancellable — returns the new cancelled state (true = cancel)
bus.get(FlagEvent.class)
   .onFlag(grim, (user, check, verbose, cancelled) -> {
       if (verbose.contains("safe")) return false; // un-cancel
       return cancelled;
   });

// Priority + ignoreCancelled overloads
bus.get(FlagEvent.class)
   .onFlag(grim, (u, c, v, cancelled) -> true, /*priority*/ 10, /*ignoreCancelled*/ false);
```

`api.getGrimPlugin(...)` is the modern replacement for examples that manually
created `BasicGrimPlugin`. Pass the native owner object from your platform and
let Grim construct the wrapper. Supported contexts include Bukkit/Paper
`JavaPlugin` / `Plugin`, Fabric `ModInitializer`, Fabric `ModContainer`, a
Fabric mod id string, or a `Class<?>` from your plugin/mod.

For cross-platform plugins, keep the Grim registration code in a shared class
and call it from each platform bootstrap:

```java
public final class GrimHooks {
    public static void register(Object platformOwner) {
        GrimAbstractAPI api = GrimAPIProvider.get();
        GrimPlugin grim = api.getGrimPlugin(platformOwner);
        EventBus bus = api.getEventBus();

        bus.get(FlagEvent.class).onFlag(grim, (user, check, verbose, cancelled) -> {
            grim.getLogger().info(user.getName() + " flagged " + check.getCheckName());
            return cancelled;
        });
    }
}
```

`BasicGrimPlugin` remains available for unusual integrations without a native
platform owner, but it should not be used in ordinary Bukkit, Paper, Fabric, or
cross-platform plugin setup.

If you really don't want to hold a `GrimPlugin` reference, every `onX(...)`
has a `@Deprecated` `onX(Object pluginContext, Handler, …)` overload that
resolves the context through the bus's plugin resolver:

```java
// Works but warns. Call api.getGrimPlugin(this) once and use the
// non-deprecated overload instead.
bus.get(FlagEvent.class).onFlag((Object) this, (u, c, v, cancelled) -> cancelled);
```

**Cache the channel for hot paths.** The `bus.get(...)` lookup is a
ConcurrentHashMap hit; storing the channel in a `static final` lets the JIT
fold the lookup away entirely:

```java
public final class MyHotPathClass {
    private static final FlagEvent.Channel FLAG =
            GrimAPIProvider.get().getEventBus().get(FlagEvent.class);

    public void process() {
        // No map lookup, no event-object allocation.
        FLAG.fire(user, check, verbose);
    }
}
```

**Priority ordering.** Lower priority fires first; higher priority gets the
final say on cancellation. This matches Bukkit's `EventPriority`
convention — a handler at a high priority can observe the settled cancelled
state after lower-priority handlers have run (useful for monitoring) or,
when registered with `ignoreCancelled = true`, override a lower-priority
handler's cancellation by returning `false`.

> **Note:** This is a direction flip from pre-1.3 Grim, which sorted
> highest-first. Plugins migrating from 1.2.x that use explicit priority
> numbers should re-evaluate what "early" and "late" mean for their logic.

**Addon events.** Plugins that define their own `GrimEvent<Channel>`
subclass register it once at enable with
`bus.register(MyEvent.class, new MyEvent.Channel())`.

**Plugin-disable cleanup.** `bus.unregisterAllListeners(grim)` sweeps every
handler registered with that `GrimPlugin` — both the typed
`bus.get(E.class).onX(grim, handler)` path and the legacy
`bus.subscribe(ctx, E.class, listener)` / `@GrimEventHandler` paths. In
`onDisable()`, pass either your `GrimPlugin` or the same `Object` context
you registered with:

```java
@Override
public void onDisable() {
    EventBus bus = GrimAPIProvider.get().getEventBus();
    bus.unregisterAllListeners(this);  // or: bus.unregisterAllListeners(grim)
}
```

**Legacy API.** `bus.post(event)`, `bus.subscribe(ctx, EventClass.class, listener)`
and `@GrimEventHandler` reflective registration are preserved for source
compatibility with 1.2.4.0 callers, route through the same channels, and are
marked `@Deprecated` with doc pointing at `bus.get(E.class).on…(grim, handler)`.
