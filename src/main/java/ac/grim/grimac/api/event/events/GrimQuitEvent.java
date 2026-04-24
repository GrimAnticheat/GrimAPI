package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

public class GrimQuitEvent extends GrimEvent<GrimQuitEvent.Channel> implements GrimUserEvent {
    private GrimUser user;

    /** Pool constructor — fields populated via {@link #init}. */
    public GrimQuitEvent() {
        super(true); // Async
    }

    public GrimQuitEvent(GrimUser user) {
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
        void onQuit(@NotNull GrimUser user);
    }

    public static final class Channel extends EventChannel<GrimQuitEvent, Handler> {
        private final ThreadLocal<GrimQuitEvent> legacyPool = ThreadLocal.withInitial(GrimQuitEvent::new);

        public Channel() {
            super(GrimQuitEvent.class, Handler.class);
        }

        public void onQuit(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onQuit(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onQuit(@NotNull Object pluginContext, @NotNull Handler handler) {
            onQuit(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onQuit(Object, Handler)}. */
        @Deprecated
        public void onQuit(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onQuit(resolvePlugin(pluginContext), handler, priority);
        }

        public void fire(@NotNull GrimUser user) {
            Entry<Handler>[] entries = entries();
            if (entries.length == 0) return;
            if (!hasLegacy()) {
                for (Entry<Handler> e : entries) {
                    try {
                        e.handler.onQuit(user);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
                return;
            }
            GrimQuitEvent pooled = legacyPool.get();
            pooled.init(user);
            for (Entry<Handler> e : entries) {
                try {
                    if (e.legacyListener != null) {
                        e.<GrimQuitEvent>legacyListenerAs().handle(pooled);
                    } else {
                        e.handler.onQuit(user);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull GrimQuitEvent event, @NotNull Handler handler, boolean cancelled) {
            handler.onQuit(event.getUser());
            return false;
        }

        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return user -> abstractHandler.onAnyEvent(GrimQuitEvent.class, false);
        }
    }
}
