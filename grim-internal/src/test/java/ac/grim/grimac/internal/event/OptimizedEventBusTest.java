package ac.grim.grimac.internal.event;

import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.event.GrimEventHandler;
import ac.grim.grimac.api.event.GrimEventListener;
import ac.grim.grimac.api.plugin.BasicGrimPlugin;
import ac.grim.grimac.api.plugin.GrimPlugin;
import ac.grim.grimac.internal.plugin.resolver.GrimExtensionManager;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class OptimizedEventBusTest {

    // ── Fixtures ───────────────────────────────────────────────────────────

    /** Test-only event to verify register/get paths for addon-defined events. */
    public static class AddonEvent extends GrimEvent<AddonEvent.Channel> {
        private int value;

        public AddonEvent() {}
        public AddonEvent(int value) { this.value = value; }
        public void init(int value) { resetForReuse(); this.value = value; }
        public int getValue() { return value; }

        @FunctionalInterface
        public interface Handler {
            void onAddon(int value);
        }

        public static final class Channel extends EventChannel<AddonEvent, Handler> {
            private final ThreadLocal<AddonEvent> legacyPool = ThreadLocal.withInitial(AddonEvent::new);

            public Channel() { super(AddonEvent.class, Handler.class); }

            public void onAddon(@NotNull Handler h) { subscribe(h, 0, false, null, null); }

            public void fire(int value) {
                Entry<Handler>[] entries = entries();
                if (entries.length == 0) return;
                if (!hasLegacy()) {
                    for (Entry<Handler> e : entries) e.handler.onAddon(value);
                    return;
                }
                AddonEvent pooled = legacyPool.get();
                pooled.init(value);
                for (Entry<Handler> e : entries) {
                    if (e.legacyListener != null) {
                        try { e.<AddonEvent>legacyListenerAs().handle(pooled); } catch (Throwable t) { t.printStackTrace(); }
                    } else {
                        e.handler.onAddon(value);
                    }
                }
            }

            @Override
            protected boolean dispatchTypedFromLegacy(@NotNull AddonEvent event, @NotNull Handler handler, boolean cancelled) {
                handler.onAddon(event.getValue());
                return false;
            }
        }
    }

    /** Reflective-path listener with an annotated method. */
    public static class ReflectiveListener {
        final List<Integer> seen = new ArrayList<>();

        @GrimEventHandler(priority = 5)
        public void onAddon(AddonEvent event) {
            seen.add(event.getValue());
        }
    }

    private OptimizedEventBus bus;
    private GrimPlugin plugin;

    @BeforeEach
    void setUp() {
        GrimExtensionManager extensionManager = new GrimExtensionManager();
        bus = new OptimizedEventBus(extensionManager);
        plugin = new BasicGrimPlugin(Logger.getLogger("test"), new File("/tmp"), "0", "", Collections.emptyList());
        // Addon events are NOT built-in; register before use.
        bus.register(AddonEvent.class, new AddonEvent.Channel());
    }

    // ── get / register ─────────────────────────────────────────────────────

    @Test
    void getReturnsTheRegisteredChannel() {
        AddonEvent.Channel first = bus.get(AddonEvent.class);
        AddonEvent.Channel second = bus.get(AddonEvent.class);
        assertSame(first, second, "bus.get must return the cached channel, not a new one per call");
    }

    // ── Typed subscribe + fire ────────────────────────────────────────────

    @Test
    void typedSubscribeAndFireWorks() {
        AtomicInteger seen = new AtomicInteger();
        bus.get(AddonEvent.class).onAddon(value -> seen.set(value));

        bus.get(AddonEvent.class).fire(42);

        assertEquals(42, seen.get());
    }

    // ── Legacy post() ──────────────────────────────────────────────────────

    @Test
    void postReachesBothLegacyAndTypedSubscribers() {
        List<String> order = new ArrayList<>();
        bus.get(AddonEvent.class).onAddon(value -> order.add("typed:" + value));
        bus.subscribe(plugin, AddonEvent.class, e -> order.add("legacy:" + e.getValue()));

        bus.post(new AddonEvent(7));

        assertEquals(2, order.size(), "both subscribers should run");
        assertEquals("typed:7", order.stream().filter(s -> s.startsWith("typed")).findFirst().orElseThrow());
        assertEquals("legacy:7", order.stream().filter(s -> s.startsWith("legacy")).findFirst().orElseThrow());
    }

    @Test
    void subscribeClassKeyedRoutesToLegacySlot() {
        AtomicInteger seen = new AtomicInteger();
        GrimEventListener<AddonEvent> listener = e -> seen.set(e.getValue());
        bus.subscribe(plugin, AddonEvent.class, listener);

        bus.get(AddonEvent.class).fire(99);

        assertEquals(99, seen.get());
    }

    // ── Reflective registration ───────────────────────────────────────────

    @Test
    void registerAnnotatedListenersReflectsAndFires() {
        ReflectiveListener listener = new ReflectiveListener();
        bus.registerAnnotatedListeners(plugin, listener);

        bus.get(AddonEvent.class).fire(3);
        bus.get(AddonEvent.class).fire(4);

        assertEquals(List.of(3, 4), listener.seen);
    }

    // ── Unregister ────────────────────────────────────────────────────────

    @Test
    void unregisterListenersRemovesOnlyTheMatchingInstance() {
        ReflectiveListener a = new ReflectiveListener();
        ReflectiveListener b = new ReflectiveListener();
        bus.registerAnnotatedListeners(plugin, a);
        bus.registerAnnotatedListeners(plugin, b);

        bus.unregisterListeners(plugin, a);

        bus.get(AddonEvent.class).fire(1);

        assertEquals(Collections.emptyList(), a.seen);
        assertEquals(List.of(1), b.seen);
    }

    @Test
    void unregisterListenerByListenerObject() {
        AtomicInteger seen = new AtomicInteger();
        GrimEventListener<AddonEvent> listener = e -> seen.incrementAndGet();
        bus.subscribe(plugin, AddonEvent.class, listener);

        bus.get(AddonEvent.class).fire(1);
        assertEquals(1, seen.get());

        bus.unregisterListener(plugin, listener);
        bus.get(AddonEvent.class).fire(2);
        assertEquals(1, seen.get(), "listener should not fire after unregistration");
    }

    @Test
    void unregisterAllListenersSweepsPluginFromEveryChannel() {
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();

        // Subscribe legacy + via annotated method (separate channels not needed — same one suffices).
        bus.subscribe(plugin, AddonEvent.class, e -> a.incrementAndGet());
        bus.registerAnnotatedListeners(plugin, new Object() {
            @GrimEventHandler
            public void onAddon(AddonEvent event) { b.incrementAndGet(); }
        });

        bus.get(AddonEvent.class).fire(0);
        assertEquals(1, a.get());
        assertEquals(1, b.get());

        bus.unregisterAllListeners(plugin);
        bus.get(AddonEvent.class).fire(0);
        assertEquals(1, a.get(), "class-keyed subscribe should be swept");
        assertEquals(1, b.get(), "reflective subscribe should be swept");
    }

}
