package ac.grim.grimac.internal.event;

import ac.grim.grimac.api.event.Cancellable;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.event.GrimEventListener;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventChannelTest {

    // ── Test fixtures ──────────────────────────────────────────────────────

    /** Non-cancellable test event with a single string payload. */
    public static class Ping extends GrimEvent<Ping.Channel> {
        private String msg;

        public Ping() {}

        public Ping(String msg) { this.msg = msg; }

        public void init(String msg) { resetForReuse(); this.msg = msg; }

        public String getMsg() { return msg; }

        @FunctionalInterface
        public interface Handler {
            void onPing(@NotNull String msg);
        }

        public static final class Channel extends EventChannel<Ping, Handler> {
            private final ThreadLocal<Ping> legacyPool = ThreadLocal.withInitial(Ping::new);

            public Channel() { super(Ping.class, Handler.class); }

            public void onPing(@NotNull Handler h) { subscribe(h, 0, false, null, null); }
            public void onPing(@NotNull Handler h, int priority) { subscribe(h, priority, false, null, null); }

            public void fire(@NotNull String msg) {
                Entry<Handler>[] entries = entries();
                if (entries.length == 0) return;
                if (!hasLegacy()) {
                    for (Entry<Handler> e : entries) e.handler.onPing(msg);
                    return;
                }
                Ping pooled = legacyPool.get();
                pooled.init(msg);
                for (Entry<Handler> e : entries) {
                    if (e.legacyListener != null) {
                        try { e.<Ping>legacyListenerAs().handle(pooled); } catch (Throwable t) { t.printStackTrace(); }
                    } else {
                        e.handler.onPing(msg);
                    }
                }
            }

            @Override
            protected boolean dispatchTypedFromLegacy(@NotNull Ping event, @NotNull Handler handler, boolean cancelled) {
                handler.onPing(event.getMsg());
                return false;
            }
        }
    }

    /** Cancellable test event with an int payload. */
    public static class Quest extends GrimEvent<Quest.Channel> implements Cancellable {
        private int value;
        private boolean cancelled;

        public Quest() {}

        public Quest(int value) { this.value = value; }

        public void init(int value) { resetForReuse(); this.value = value; this.cancelled = false; }

        public int getValue() { return value; }

        @Override public boolean isCancelled() { return cancelled; }
        @Override public void setCancelled(boolean cancelled) { this.cancelled = cancelled; }
        @Override public boolean isCancellable() { return true; }

        @FunctionalInterface
        public interface Handler {
            boolean onQuest(int value, boolean currentlyCancelled);
        }

        public static final class Channel extends EventChannel<Quest, Handler> {
            private final ThreadLocal<Quest> legacyPool = ThreadLocal.withInitial(Quest::new);

            public Channel() { super(Quest.class, Handler.class); }

            public void onQuest(@NotNull Handler h) { subscribe(h, 0, false, null, null); }
            public void onQuest(@NotNull Handler h, int priority, boolean ignoreCancelled) {
                subscribe(h, priority, ignoreCancelled, null, null);
            }

            public boolean fire(int value) {
                Entry<Handler>[] entries = entries();
                if (entries.length == 0) return false;
                boolean cancelled = false;
                if (!hasLegacy()) {
                    for (Entry<Handler> e : entries) {
                        if (cancelled && !e.ignoreCancelled) continue;
                        cancelled = e.handler.onQuest(value, cancelled);
                    }
                    return cancelled;
                }
                Quest pooled = legacyPool.get();
                pooled.init(value);
                for (Entry<Handler> e : entries) {
                    if (cancelled && !e.ignoreCancelled) continue;
                    if (e.legacyListener != null) {
                        pooled.setCancelled(cancelled);
                        try { e.<Quest>legacyListenerAs().handle(pooled); } catch (Throwable t) { t.printStackTrace(); }
                        cancelled = pooled.isCancelled();
                    } else {
                        cancelled = e.handler.onQuest(value, cancelled);
                    }
                }
                return cancelled;
            }

            @Override
            protected boolean dispatchTypedFromLegacy(@NotNull Quest event, @NotNull Handler handler, boolean cancelled) {
                return handler.onQuest(event.getValue(), cancelled);
            }
        }
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    @Test
    void typedHandlersFireInPriorityOrderLowerFirst() {
        Ping.Channel ch = new Ping.Channel();
        List<String> order = new ArrayList<>();
        ch.onPing(msg -> order.add("low"), 0);
        ch.onPing(msg -> order.add("high"), 10);
        ch.onPing(msg -> order.add("mid"), 5);

        ch.fire("hi");

        // Bukkit EventPriority order: lowest first, highest gets final say.
        assertEquals(List.of("low", "mid", "high"), order);
    }

    @Test
    void legacyListenerReceivesPooledEventWithCorrectFields() {
        Ping.Channel ch = new Ping.Channel();
        AtomicInteger hits = new AtomicInteger();
        List<String> seen = new ArrayList<>();

        GrimEventListener<Ping> listener = event -> {
            hits.incrementAndGet();
            seen.add(event.getMsg());
        };
        ch.subscribeLegacy(listener, Ping.class, 0, false, null, null);

        ch.fire("first");
        ch.fire("second");

        assertEquals(2, hits.get());
        assertEquals(List.of("first", "second"), seen);
    }

    @Test
    void typedAndLegacyInterleavedByPriority() {
        Ping.Channel ch = new Ping.Channel();
        List<String> order = new ArrayList<>();

        ch.onPing(msg -> order.add("typed-0"), 0);
        ch.subscribeLegacy(e -> order.add("legacy-5"), Ping.class, 5, false, null, null);
        ch.onPing(msg -> order.add("typed-10"), 10);

        ch.fire("x");

        assertEquals(List.of("typed-0", "legacy-5", "typed-10"), order);
    }

    @Test
    void hasLegacyTogglesAsLegacyListenersComeAndGo() {
        Ping.Channel ch = new Ping.Channel();
        GrimEventListener<Ping> listener = e -> {};
        assertFalse(ch.hasLegacy());

        ch.subscribeLegacy(listener, Ping.class, 0, false, null, null);
        assertTrue(ch.hasLegacy());

        ch.unsubscribeLegacy(listener);
        assertFalse(ch.hasLegacy());
    }

    @Test
    void unsubscribeRemovesOnlyThatHandler() {
        Ping.Channel ch = new Ping.Channel();
        List<String> order = new ArrayList<>();
        Ping.Handler keep = msg -> order.add("keep");
        Ping.Handler remove = msg -> order.add("remove");

        ch.onPing(keep, 0);
        ch.onPing(remove, 0);

        ch.unsubscribe(remove);
        ch.fire("x");
        assertEquals(List.of("keep"), order);
    }

    @Test
    void unsubscribeAllFromPluginRemovesAllMatchingEntries() {
        Ping.Channel ch = new Ping.Channel();
        Object pluginA = new Object();
        Object pluginB = new Object();
        AtomicInteger hitsA = new AtomicInteger();
        AtomicInteger hitsB = new AtomicInteger();

        ch.subscribeLegacy(e -> hitsA.incrementAndGet(), Ping.class, 0, false, pluginA, null);
        ch.subscribeLegacy(e -> hitsB.incrementAndGet(), Ping.class, 0, false, pluginB, null);

        ch.fire("x");
        assertEquals(1, hitsA.get());
        assertEquals(1, hitsB.get());

        ch.unsubscribeAllFromPlugin(pluginA);
        ch.fire("x");
        assertEquals(1, hitsA.get(), "pluginA's listener should be gone");
        assertEquals(2, hitsB.get());

        ch.unsubscribeAllFromPlugin(pluginB);
        ch.fire("x");
        assertEquals(2, hitsB.get());
        assertFalse(ch.hasLegacy());
    }

    @Test
    void cancelledEventSkipsLaterHandlersUnlessIgnoreCancelled() {
        Quest.Channel ch = new Quest.Channel();
        List<String> order = new ArrayList<>();

        // Lowest priority fires first. A cancel at priority 0 means subsequent
        // handlers without ignoreCancelled skip; the ignoreCancelled one still runs.
        ch.onQuest((value, cancelled) -> { order.add("a-cancels"); return true; }, 0, false);
        ch.onQuest((value, cancelled) -> { order.add("b-normal"); return cancelled; }, 5, false);
        ch.onQuest((value, cancelled) -> { order.add("c-ignores"); return cancelled; }, 10, true);

        boolean finalCancelled = ch.fire(42);

        assertTrue(finalCancelled);
        assertEquals(List.of("a-cancels", "c-ignores"), order);
    }

    @Test
    void cancellationThreadedIntoSubsequentHandlers() {
        Quest.Channel ch = new Quest.Channel();
        List<Boolean> seen = new ArrayList<>();

        // Low-priority canceller runs first; high-priority observer sees the
        // cancelled state threaded in via the last arg.
        ch.onQuest((value, cancelled) -> { seen.add(cancelled); return true; }, 0, false);
        ch.onQuest((value, cancelled) -> { seen.add(cancelled); return cancelled; }, 10, true);

        ch.fire(0);

        assertEquals(List.of(false, true), seen);
    }

    @Test
    void legacyListenerSeesCancellationStateViaPooledEvent() {
        Quest.Channel ch = new Quest.Channel();
        AtomicInteger sawCancelled = new AtomicInteger();

        // Low-priority typed cancels first. High-priority legacy with
        // ignoreCancelled=true runs after and must see cancelled=true via
        // the pooled event.
        ch.onQuest((value, cancelled) -> true, 0, false);
        ch.subscribeLegacy(e -> { if (e.isCancelled()) sawCancelled.incrementAndGet(); },
                Quest.class, 10, true, null, null);

        ch.fire(0);

        assertEquals(1, sawCancelled.get());
    }

    @Test
    void dispatchLegacyInvokesBothLegacyAndTypedHandlers() {
        Ping.Channel ch = new Ping.Channel();
        List<String> order = new ArrayList<>();

        // Legacy at priority 0 fires first; typed at priority 10 fires second.
        ch.subscribeLegacy(e -> order.add("legacy:" + e.getMsg()), Ping.class, 0, false, null, null);
        ch.onPing(msg -> order.add("typed:" + msg), 10);

        Ping provided = new Ping("dispatched");
        ch.dispatchLegacy(provided);

        assertEquals(List.of("legacy:dispatched", "typed:dispatched"), order);
    }

}
