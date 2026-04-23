package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

public class GrimReloadEvent extends GrimEvent<GrimReloadEvent.Channel> {
    private boolean success;

    /** Pool constructor — fields populated via {@link #init}. */
    public GrimReloadEvent() {
        super(true); // Async
    }

    public GrimReloadEvent(boolean success) {
        super(true); // Async
        this.success = success;
    }

    @ApiStatus.Internal
    public void init(boolean success) {
        resetForReuse();
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }

    @FunctionalInterface
    public interface Handler {
        void onReload(boolean success);
    }

    public static final class Channel extends EventChannel<GrimReloadEvent, Handler> {
        private final ThreadLocal<GrimReloadEvent> legacyPool = ThreadLocal.withInitial(GrimReloadEvent::new);

        public Channel() {
            super(GrimReloadEvent.class, Handler.class);
        }

        public void onReload(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onReload(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onReload(@NotNull Object pluginContext, @NotNull Handler handler) {
            onReload(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onReload(Object, Handler)}. */
        @Deprecated
        public void onReload(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onReload(resolvePlugin(pluginContext), handler, priority);
        }

        public void fire(boolean success) {
            Entry<Handler>[] entries = entries();
            if (entries.length == 0) return;
            if (!hasLegacy()) {
                for (Entry<Handler> e : entries) {
                    try {
                        e.handler.onReload(success);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
                return;
            }
            GrimReloadEvent pooled = legacyPool.get();
            pooled.init(success);
            for (Entry<Handler> e : entries) {
                try {
                    if (e.legacyListener != null) {
                        e.<GrimReloadEvent>legacyListenerAs().handle(pooled);
                    } else {
                        e.handler.onReload(success);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull GrimReloadEvent event, @NotNull Handler handler, boolean cancelled) {
            handler.onReload(event.isSuccess());
            return false;
        }

        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return success -> abstractHandler.onAnyEvent(GrimReloadEvent.class, false);
        }
    }
}
