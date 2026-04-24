package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.NotNull;

/**
 * Fired when Grim sends a teleport packet to the client.
 *
 * <p>Exists to help maintain compatibility with packet-based plugins and other
 * anticheats that track inbound/outbound teleport packets to build a
 * pending-teleport deque.
 *
 * <p>Fires on the Netty thread associated with the PacketEvents user.
 * Observational, not cancellable.
 */
public final class GrimTeleportEvent extends GrimEvent<GrimTeleportEvent.Channel> {
    private GrimTeleportEvent() {
        // Never instantiated — exists only as a Class key for bus.get(GrimTeleportEvent.class).
    }

    @FunctionalInterface
    public interface Handler {
        void onTeleport(@NotNull GrimUser user, int teleportId, long timestamp);
    }

    public static final class Channel extends EventChannel<GrimTeleportEvent, Handler> {
        public Channel() {
            super(GrimTeleportEvent.class, Handler.class);
        }

        public void onTeleport(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onTeleport(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onTeleport(@NotNull Object pluginContext, @NotNull Handler handler) {
            onTeleport(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onTeleport(Object, Handler)}. */
        @Deprecated
        public void onTeleport(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onTeleport(resolvePlugin(pluginContext), handler, priority);
        }

        public void fire(@NotNull GrimUser user, int teleportId, long timestamp) {
            Entry<Handler>[] entries = entries();
            for (Entry<Handler> e : entries) {
                try {
                    e.handler.onTeleport(user, teleportId, timestamp);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull GrimTeleportEvent event, @NotNull Handler handler, boolean cancelled) {
            // Unreachable — GrimTeleportEvent has no public constructor, so no caller can post() one.
            throw new UnsupportedOperationException("GrimTeleportEvent has no legacy representation");
        }

        @org.jetbrains.annotations.ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, id, ts) -> abstractHandler.onAnyEvent(GrimTeleportEvent.class, false);
        }
    }
}
