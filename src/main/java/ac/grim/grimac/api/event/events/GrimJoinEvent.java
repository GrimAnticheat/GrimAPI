package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

public class GrimJoinEvent extends GrimEvent<GrimJoinEvent.Channel> implements GrimUserEvent {
    private GrimUser user;

    /** Pool constructor — fields populated via {@link #init}. */
    public GrimJoinEvent() {
        super(true); // Async
    }

    public GrimJoinEvent(GrimUser user) {
        super(true); // Async
        this.user = user;
    }

    @ApiStatus.Internal
    public void init(GrimUser user) {
        resetForReuse();
        this.user = user;
    }

    @Override
    public GrimUser getUser() {
        return user;
    }

    @FunctionalInterface
    public interface Handler {
        void onJoin(@NotNull GrimUser user);
    }

    public static final class Channel extends EventChannel<GrimJoinEvent, Handler> {
        private final ThreadLocal<GrimJoinEvent> legacyPool = ThreadLocal.withInitial(GrimJoinEvent::new);

        public Channel() {
            super(GrimJoinEvent.class, Handler.class);
        }

        public void onJoin(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onJoin(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onJoin(@NotNull Object pluginContext, @NotNull Handler handler) {
            onJoin(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onJoin(Object, Handler)}. */
        @Deprecated
        public void onJoin(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onJoin(resolvePlugin(pluginContext), handler, priority);
        }

        public void fire(@NotNull GrimUser user) {
            Entry<Handler>[] entries = entries();
            if (entries.length == 0) return;
            if (!hasLegacy()) {
                for (Entry<Handler> e : entries) {
                    try {
                        e.handler.onJoin(user);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
                return;
            }
            GrimJoinEvent pooled = legacyPool.get();
            pooled.init(user);
            for (Entry<Handler> e : entries) {
                try {
                    if (e.legacyListener != null) {
                        e.<GrimJoinEvent>legacyListenerAs().handle(pooled);
                    } else {
                        e.handler.onJoin(user);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull GrimJoinEvent event, @NotNull Handler handler, boolean cancelled) {
            handler.onJoin(event.getUser());
            return false;
        }

        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return user -> abstractHandler.onAnyEvent(GrimJoinEvent.class, false);
        }
    }
}
