package ac.grim.grimac.internal.event;

import ac.grim.grimac.api.event.EventBus;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.event.GrimEventHandler;
import ac.grim.grimac.api.event.GrimEventListener;
import ac.grim.grimac.api.event.events.CommandExecuteEvent;
import ac.grim.grimac.api.event.events.CompletePredictionEvent;
import ac.grim.grimac.api.event.events.FlagEvent;
import ac.grim.grimac.api.event.events.GrimCheckEvent;
import ac.grim.grimac.api.event.events.GrimJoinEvent;
import ac.grim.grimac.api.event.events.GrimPlayerSetbackEvent;
import ac.grim.grimac.api.event.events.GrimQuitEvent;
import ac.grim.grimac.api.event.events.GrimReloadEvent;
import ac.grim.grimac.api.event.events.GrimSetbackEvent;
import ac.grim.grimac.api.event.events.GrimTeleportEvent;
import ac.grim.grimac.api.event.events.GrimTransactionReceivedEvent;
import ac.grim.grimac.api.event.events.GrimTransactionSendEvent;
import ac.grim.grimac.api.event.events.GrimVehicleSetbackEvent;
import ac.grim.grimac.api.event.events.GrimVerboseCheckEvent;
import ac.grim.grimac.api.plugin.GrimPlugin;
import ac.grim.grimac.internal.plugin.resolver.GrimExtensionManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Channel-backed event bus implementation.
 *
 * <p>All Grim built-in channels are registered explicitly at construction
 * time — no reflection, obfuscation-safe. Addons with their own events can
 * register via {@link #register(Class, EventChannel)}.
 *
 * <p>Legacy APIs ({@link #post(GrimEvent)}, class-keyed {@code subscribe},
 * reflective {@code registerAnnotatedListeners}) are retained for source
 * compatibility with 1.2.4.0 callers. All three route through the same
 * channel objects as {@link #get(Class)} + the channel's typed
 * {@code on…(…)} methods — a single post therefore reaches typed handlers
 * as well as legacy listeners via each channel's
 * {@link EventChannel#dispatchLegacy(GrimEvent)}.
 */
public class OptimizedEventBus implements EventBus {
    private final MethodHandles.Lookup lookup = MethodHandles.lookup();
    private final GrimExtensionManager extensionManager;
    private final ConcurrentMap<Class<?>, EventChannel<?, ?>> channels = new ConcurrentHashMap<>();

    /**
     * Side map of reflective/class-keyed registrations keyed by identity of
     * (pluginContext, listenerInstanceOrClass). Used by
     * {@link #unregisterListeners(Object, Object)} and friends to find the
     * specific entries to remove — Entry itself doesn't carry the originating
     * listener instance. Modifications are guarded by {@code this}.
     */
    private final Map<Object, Map<Object, List<Registration>>> instanceRegistrations = new IdentityHashMap<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    public OptimizedEventBus(GrimExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
        // Built-in channels — direct compile-time references.
        // Renaming any event's nested Channel breaks these lines immediately.

        // Concrete channels.
        FlagEvent.Channel flagCh                           = new FlagEvent.Channel();
        CommandExecuteEvent.Channel commandCh              = new CommandExecuteEvent.Channel();
        CompletePredictionEvent.Channel completeCh         = new CompletePredictionEvent.Channel();
        GrimJoinEvent.Channel joinCh                       = new GrimJoinEvent.Channel();
        GrimQuitEvent.Channel quitCh                       = new GrimQuitEvent.Channel();
        GrimReloadEvent.Channel reloadCh                   = new GrimReloadEvent.Channel();
        GrimTeleportEvent.Channel teleportCh               = new GrimTeleportEvent.Channel();
        GrimTransactionSendEvent.Channel txSendCh          = new GrimTransactionSendEvent.Channel();
        GrimTransactionReceivedEvent.Channel txRecvCh      = new GrimTransactionReceivedEvent.Channel();
        GrimPlayerSetbackEvent.Channel playerSetbackCh     = new GrimPlayerSetbackEvent.Channel();
        GrimVehicleSetbackEvent.Channel vehicleSetbackCh   = new GrimVehicleSetbackEvent.Channel();
        installChannel(FlagEvent.class,                    flagCh);
        installChannel(CommandExecuteEvent.class,          commandCh);
        installChannel(CompletePredictionEvent.class,      completeCh);
        installChannel(GrimJoinEvent.class,                joinCh);
        installChannel(GrimQuitEvent.class,                quitCh);
        installChannel(GrimReloadEvent.class,              reloadCh);
        installChannel(GrimTeleportEvent.class,            teleportCh);
        installChannel(GrimTransactionSendEvent.class,     txSendCh);
        installChannel(GrimTransactionReceivedEvent.class, txRecvCh);
        installChannel(GrimPlayerSetbackEvent.class,       playerSetbackCh);
        installChannel(GrimVehicleSetbackEvent.class,      vehicleSetbackCh);

        // Abstract channels. Their Class<GrimCheckEvent<?>> keys are raw-cast
        // because GrimCheckEvent's CHANNEL type parameter is erased at .class.
        GrimEvent.Channel anyCh                            = new GrimEvent.Channel();
        GrimCheckEvent.Channel checkCh                     = new GrimCheckEvent.Channel();
        GrimVerboseCheckEvent.Channel verboseCheckCh       = new GrimVerboseCheckEvent.Channel();
        GrimSetbackEvent.Channel setbackCh                 = new GrimSetbackEvent.Channel();
        installAbstractChannel(GrimEvent.class,             anyCh);
        installAbstractChannel(GrimCheckEvent.class,        checkCh);
        installAbstractChannel(GrimVerboseCheckEvent.class, verboseCheckCh);
        installAbstractChannel(GrimSetbackEvent.class,      setbackCh);

        // Bridge wiring: every concrete subtype registers with every abstract
        // parent it can bridge to. Order matters only when abstract subscribes
        // land before registerSubtype — in that case registerSubtype walks the
        // subscriber list. We wire after installing both channels, so at
        // construction time the subscriber list is empty and the install is
        // just bookkeeping.
        checkCh.registerSubtype(FlagEvent.class,                flagCh,     FlagEvent.Channel::bridgeFromCheck);
        checkCh.registerSubtype(CommandExecuteEvent.class,      commandCh,  CommandExecuteEvent.Channel::bridgeFromCheck);
        checkCh.registerSubtype(CompletePredictionEvent.class,  completeCh, CompletePredictionEvent.Channel::bridgeFromCheck);

        verboseCheckCh.registerSubtype(FlagEvent.class,            flagCh,    FlagEvent.Channel::bridgeFromVerboseCheck);
        verboseCheckCh.registerSubtype(CommandExecuteEvent.class,  commandCh, CommandExecuteEvent.Channel::bridgeFromVerboseCheck);

        setbackCh.registerSubtype(GrimPlayerSetbackEvent.class,   playerSetbackCh,  GrimPlayerSetbackEvent.Channel::bridgeFromSetback);
        setbackCh.registerSubtype(GrimVehicleSetbackEvent.class,  vehicleSetbackCh, GrimVehicleSetbackEvent.Channel::bridgeFromSetback);

        anyCh.registerSubtype(FlagEvent.class,                    flagCh,           FlagEvent.Channel::bridgeFromAny);
        anyCh.registerSubtype(CommandExecuteEvent.class,          commandCh,        CommandExecuteEvent.Channel::bridgeFromAny);
        anyCh.registerSubtype(CompletePredictionEvent.class,      completeCh,       CompletePredictionEvent.Channel::bridgeFromAny);
        anyCh.registerSubtype(GrimJoinEvent.class,                joinCh,           GrimJoinEvent.Channel::bridgeFromAny);
        anyCh.registerSubtype(GrimQuitEvent.class,                quitCh,           GrimQuitEvent.Channel::bridgeFromAny);
        anyCh.registerSubtype(GrimReloadEvent.class,              reloadCh,         GrimReloadEvent.Channel::bridgeFromAny);
        anyCh.registerSubtype(GrimTeleportEvent.class,            teleportCh,       GrimTeleportEvent.Channel::bridgeFromAny);
        anyCh.registerSubtype(GrimTransactionSendEvent.class,     txSendCh,         GrimTransactionSendEvent.Channel::bridgeFromAny);
        anyCh.registerSubtype(GrimTransactionReceivedEvent.class, txRecvCh,         GrimTransactionReceivedEvent.Channel::bridgeFromAny);
        anyCh.registerSubtype(GrimPlayerSetbackEvent.class,       playerSetbackCh,  GrimPlayerSetbackEvent.Channel::bridgeFromAny);
        anyCh.registerSubtype(GrimVehicleSetbackEvent.class,      vehicleSetbackCh, GrimVehicleSetbackEvent.Channel::bridgeFromAny);
    }

    private <E extends GrimEvent<C>, C extends EventChannel<? extends E, ?>> void installChannel(Class<E> eventClass, C channel) {
        channel.setPluginResolver(extensionManager::getPlugin);
        channels.put(eventClass, channel);
    }

    /**
     * Abstract-channel variant of {@link #installChannel}. Abstract events
     * (GrimEvent, GrimCheckEvent, GrimVerboseCheckEvent) are generic on
     * {@code CHANNEL}, which makes the tight
     * {@code E extends GrimEvent<C>, C extends EventChannel<? extends E, ?>}
     * constraint unsolvable when you pass the raw parent class with a
     * concrete abstract-channel. Loosen the E binding here; the abstract
     * channels are exhaustively listed in the constructor, so the looser
     * type safety is contained.
     */
    private <E, C extends EventChannel<?, ?>> void installAbstractChannel(Class<E> eventClass, C channel) {
        channel.setPluginResolver(extensionManager::getPlugin);
        channels.put(eventClass, channel);
    }

    // ───── Typed channel API ───────────────────────────────────────────────

    @Override
    @SuppressWarnings("unchecked")
    public <E extends GrimEvent<C>, C extends EventChannel<? extends E, ?>> @NotNull C get(@NotNull Class<E> eventClass) {
        EventChannel<?, ?> ch = channels.get(eventClass);
        if (ch == null) {
            throw new IllegalArgumentException("No EventChannel registered for " + eventClass.getName()
                    + " — addons must call EventBus.register(Class, Channel) before first use.");
        }
        return (C) ch;
    }

    @Override
    public <E extends GrimEvent<C>, C extends EventChannel<? extends E, ?>> void register(@NotNull Class<E> eventClass, @NotNull C channel) {
        channel.setPluginResolver(extensionManager::getPlugin);
        channels.put(eventClass, channel);
    }

    // ───── Reflective annotated-method registration ────────────────────────

    @Override
    public void registerAnnotatedListeners(@NotNull Object pluginContext, @NotNull Object listener) {
        GrimPlugin plugin = extensionManager.getPlugin(pluginContext);
        registerAnnotatedListeners(plugin, listener);
    }

    @Override
    public void registerAnnotatedListeners(@NotNull GrimPlugin plugin, @NotNull Object listener) {
        registerMethods(plugin, listener, listener.getClass(), listener);
    }

    @Override
    public void registerStaticAnnotatedListeners(@NotNull Object pluginContext, @NotNull Class<?> clazz) {
        GrimPlugin plugin = extensionManager.getPlugin(pluginContext);
        registerStaticAnnotatedListeners(plugin, clazz);
    }

    @Override
    public void registerStaticAnnotatedListeners(@NotNull GrimPlugin plugin, @NotNull Class<?> clazz) {
        registerMethods(plugin, null, clazz, clazz);
    }

    private void registerMethods(@NotNull GrimPlugin plugin, @Nullable Object instance, @NotNull Class<?> clazz, @NotNull Object registrationKey) {
        for (Method method : clazz.getDeclaredMethods()) {
            GrimEventHandler annotation = method.getAnnotation(GrimEventHandler.class);
            if (annotation == null || method.getParameterCount() != 1) continue;
            Class<?> paramType = method.getParameterTypes()[0];
            if (!GrimEvent.class.isAssignableFrom(paramType)) continue;
            if (instance == null && !Modifier.isStatic(method.getModifiers())) continue;

            @SuppressWarnings("unchecked")
            Class<? extends GrimEvent<?>> eventClass = (Class<? extends GrimEvent<?>>) (Class<?>) paramType;
            EventChannel<?, ?> channel = channels.get(eventClass);
            if (channel == null) continue; // no channel for this event — ignore silently (matches old behavior)

            try {
                method.setAccessible(true);
                MethodHandle handle = lookup.unreflect(method);
                GrimEventListener<GrimEvent<?>> listener = buildListener(instance, handle);
                subscribeLegacyInternal(plugin, channel, eventClass, listener,
                        annotation.priority(), annotation.ignoreCancelled(), method.getDeclaringClass(), registrationKey);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    /** Wrap a reflected method + instance as a GrimEventListener. */
    private static GrimEventListener<GrimEvent<?>> buildListener(@Nullable Object instance, MethodHandle handle) {
        if (instance != null) {
            return event -> {
                try {
                    handle.invoke(instance, event);
                } catch (Throwable t) {
                    throw new RuntimeException("Failed to invoke listener for " + event.getClass().getName(), t);
                }
            };
        }
        return event -> {
            try {
                handle.invoke(event);
            } catch (Throwable t) {
                throw new RuntimeException("Failed to invoke listener for " + event.getClass().getName(), t);
            }
        };
    }

    // ───── Class-keyed explicit subscribe ──────────────────────────────────

    @Override
    public <T extends GrimEvent<?>> void subscribe(@NotNull Object pluginContext, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener, int priority, boolean ignoreCancelled, @NotNull Class<?> declaringClass) {
        GrimPlugin plugin = extensionManager.getPlugin(pluginContext);
        subscribe(plugin, eventType, listener, priority, ignoreCancelled, declaringClass);
    }

    @Override
    public <T extends GrimEvent<?>> void subscribe(@NotNull GrimPlugin plugin, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener, int priority, boolean ignoreCancelled, @NotNull Class<?> declaringClass) {
        EventChannel<?, ?> channel = channels.get(eventType);
        if (channel == null) {
            throw new IllegalArgumentException("No EventChannel registered for " + eventType.getName());
        }
        subscribeLegacyInternal(plugin, channel, eventType, listener, priority, ignoreCancelled, declaringClass, listener);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void subscribeLegacyInternal(
            @NotNull GrimPlugin plugin,
            @NotNull EventChannel<?, ?> channel,
            @NotNull Class<? extends GrimEvent<?>> eventClass,
            @NotNull GrimEventListener<?> listener,
            int priority, boolean ignoreCancelled,
            @NotNull Class<?> declaringClass,
            @NotNull Object registrationKey) {
        ((EventChannel) channel).subscribeLegacy(listener, eventClass, priority, ignoreCancelled, plugin, declaringClass);
        synchronized (this) {
            Map<Object, List<Registration>> byPlugin = instanceRegistrations.computeIfAbsent(plugin, p -> new IdentityHashMap<>());
            byPlugin.computeIfAbsent(registrationKey, k -> new ArrayList<>()).add(new Registration(channel, listener));
        }
    }

    // ───── Unregister ──────────────────────────────────────────────────────

    @Override
    public void unregisterListeners(@NotNull Object pluginContext, @NotNull Object listener) {
        unregisterListeners(extensionManager.getPlugin(pluginContext), listener);
    }

    @Override
    public void unregisterListeners(@NotNull GrimPlugin plugin, @NotNull Object listener) {
        removeInstanceRegistrations(plugin, listener);
    }

    @Override
    public void unregisterStaticListeners(@NotNull Object pluginContext, @NotNull Class<?> clazz) {
        unregisterStaticListeners(extensionManager.getPlugin(pluginContext), clazz);
    }

    @Override
    public void unregisterStaticListeners(@NotNull GrimPlugin plugin, @NotNull Class<?> clazz) {
        removeInstanceRegistrations(plugin, clazz);
    }

    @Override
    public void unregisterAllListeners(@NotNull Object pluginContext) {
        unregisterAllListeners(extensionManager.getPlugin(pluginContext));
    }

    @Override
    public void unregisterAllListeners(@NotNull GrimPlugin plugin) {
        List<Registration> all = new ArrayList<>();
        synchronized (this) {
            Map<Object, List<Registration>> byPlugin = instanceRegistrations.remove(plugin);
            if (byPlugin != null) {
                for (List<Registration> regs : byPlugin.values()) all.addAll(regs);
            }
        }
        for (Registration r : all) r.channel.unsubscribeLegacy(r.listener);
        for (EventChannel<?, ?> ch : channels.values()) ch.unsubscribeAllFromPlugin(plugin);
    }

    @Override
    public void unregisterListener(@NotNull Object pluginContext, @NotNull GrimEventListener<?> listener) {
        unregisterListener(extensionManager.getPlugin(pluginContext), listener);
    }

    @Override
    public void unregisterListener(@NotNull GrimPlugin plugin, @NotNull GrimEventListener<?> listener) {
        for (EventChannel<?, ?> ch : channels.values()) ch.unsubscribeLegacy(listener);
        synchronized (this) {
            Map<Object, List<Registration>> byPlugin = instanceRegistrations.get(plugin);
            if (byPlugin != null) {
                for (List<Registration> regs : byPlugin.values()) {
                    regs.removeIf(r -> r.listener == listener);
                }
            }
        }
    }

    private void removeInstanceRegistrations(@NotNull GrimPlugin plugin, @NotNull Object key) {
        List<Registration> toRemove;
        synchronized (this) {
            Map<Object, List<Registration>> byPlugin = instanceRegistrations.get(plugin);
            if (byPlugin == null) return;
            List<Registration> regs = byPlugin.remove(key);
            if (regs == null) return;
            toRemove = regs;
        }
        for (Registration r : toRemove) r.channel.unsubscribeLegacy(r.listener);
    }

    // ───── Legacy post ─────────────────────────────────────────────────────

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void post(@NotNull GrimEvent<?> event) {
        // Route to the concrete-class channel. Walks the superclass chain so a
        // hypothetical abstract-event subscriber (via addon-registered channel)
        // still receives the event.
        Class<?> c = event.getClass();
        while (c != null && GrimEvent.class.isAssignableFrom(c)) {
            EventChannel<?, ?> ch = channels.get(c);
            if (ch != null) {
                ((EventChannel) ch).dispatchLegacy(event);
            }
            c = c.getSuperclass();
        }
    }

    /** Reflective-registration bookkeeping entry. */
    private static final class Registration {
        final EventChannel<?, ?> channel;
        final GrimEventListener<?> listener;

        Registration(EventChannel<?, ?> channel, GrimEventListener<?> listener) {
            this.channel = channel;
            this.listener = listener;
        }
    }
}
