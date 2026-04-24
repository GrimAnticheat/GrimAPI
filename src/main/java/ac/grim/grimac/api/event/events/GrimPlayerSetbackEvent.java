package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Fired when Grim sends a player-on-foot setback — the
 * {@code ServerPlayerPositionAndLook} / teleport packet branch of
 * {@link GrimSetbackEvent}.
 *
 * <p>Carries the outbound teleport id so packet-tracking consumers
 * (e.g. sibling anticheats that need to dedupe the teleport-confirm the
 * client will emit in response) can correlate on id. See
 * {@link GrimTeleportEvent} for the complementary
 * "every outbound teleport packet" signal — that event fires at this
 * site as well so either subscription sees the setback teleport.
 *
 * <p>Fires on the Netty thread associated with the user. Observational,
 * not cancellable.
 */
public final class GrimPlayerSetbackEvent extends GrimSetbackEvent<GrimPlayerSetbackEvent.Channel> {
    private GrimPlayerSetbackEvent() {
        // Never instantiated — exists only as a Class key for bus.get(GrimPlayerSetbackEvent.class).
    }

    @FunctionalInterface
    public interface Handler {
        void onPlayerSetback(@NotNull GrimUser user, int teleportId,
                             double x, double y, double z, long timestamp);
    }

    public static final class Channel extends EventChannel<GrimPlayerSetbackEvent, Handler> {
        public Channel() {
            super(GrimPlayerSetbackEvent.class, Handler.class);
        }

        public void onPlayerSetback(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onPlayerSetback(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onPlayerSetback(@NotNull Object pluginContext, @NotNull Handler handler) {
            onPlayerSetback(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onPlayerSetback(Object, Handler)}. */
        @Deprecated
        public void onPlayerSetback(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onPlayerSetback(resolvePlugin(pluginContext), handler, priority);
        }

        public void fire(@NotNull GrimUser user, int teleportId,
                         double x, double y, double z, long timestamp) {
            Entry<Handler>[] entries = entries();
            for (Entry<Handler> e : entries) {
                try {
                    e.handler.onPlayerSetback(user, teleportId, x, y, z, timestamp);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull GrimPlayerSetbackEvent event, @NotNull Handler handler, boolean cancelled) {
            // Unreachable — no public constructor, so no caller can post() one.
            throw new UnsupportedOperationException("GrimPlayerSetbackEvent has no legacy representation");
        }

        /** Bridge from {@link GrimSetbackEvent.Handler} — used by the abstract channel when a setback-level subscriber registers. */
        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromSetback(@NotNull GrimSetbackEvent.Handler abstractHandler) {
            return (user, id, x, y, z, ts) -> abstractHandler.onAnySetback(user, ts);
        }

        /** Bridge from root-level {@link ac.grim.grimac.api.event.GrimEvent.Handler}. */
        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, id, x, y, z, ts) -> abstractHandler.onAnyEvent(GrimPlayerSetbackEvent.class, false);
        }
    }
}
