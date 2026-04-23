package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Fired when Grim sends a player-in-vehicle setback — the
 * {@code ServerVehicleMove} packet branch of {@link GrimSetbackEvent}.
 *
 * <p>Unlike {@link GrimPlayerSetbackEvent}, vehicle-move packets carry no
 * teleport id, so there is nothing for a packet-tracking consumer to
 * correlate against an incoming confirm; this event exists primarily for
 * the semantic "Grim did a setback" audience (admin tools, stats,
 * anticheat-test harnesses).
 *
 * <p>Fires on the Netty thread associated with the user. Observational,
 * not cancellable.
 */
public final class GrimVehicleSetbackEvent extends GrimSetbackEvent<GrimVehicleSetbackEvent.Channel> {
    private GrimVehicleSetbackEvent() {
        // Never instantiated — exists only as a Class key for bus.get(GrimVehicleSetbackEvent.class).
    }

    @FunctionalInterface
    public interface Handler {
        void onVehicleSetback(@NotNull GrimUser user,
                              double x, double y, double z, long timestamp);
    }

    public static final class Channel extends EventChannel<GrimVehicleSetbackEvent, Handler> {
        public Channel() {
            super(GrimVehicleSetbackEvent.class, Handler.class);
        }

        public void onVehicleSetback(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onVehicleSetback(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onVehicleSetback(@NotNull Object pluginContext, @NotNull Handler handler) {
            onVehicleSetback(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onVehicleSetback(Object, Handler)}. */
        @Deprecated
        public void onVehicleSetback(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onVehicleSetback(resolvePlugin(pluginContext), handler, priority);
        }

        public void fire(@NotNull GrimUser user,
                         double x, double y, double z, long timestamp) {
            Entry<Handler>[] entries = entries();
            for (Entry<Handler> e : entries) {
                try {
                    e.handler.onVehicleSetback(user, x, y, z, timestamp);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull GrimVehicleSetbackEvent event, @NotNull Handler handler, boolean cancelled) {
            // Unreachable — no public constructor, so no caller can post() one.
            throw new UnsupportedOperationException("GrimVehicleSetbackEvent has no legacy representation");
        }

        /** Bridge from {@link GrimSetbackEvent.Handler} — used by the abstract channel when a setback-level subscriber registers. */
        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromSetback(@NotNull GrimSetbackEvent.Handler abstractHandler) {
            return (user, x, y, z, ts) -> abstractHandler.onAnySetback(user, ts);
        }

        /** Bridge from root-level {@link ac.grim.grimac.api.event.GrimEvent.Handler}. */
        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, x, y, z, ts) -> abstractHandler.onAnyEvent(GrimVehicleSetbackEvent.class, false);
        }
    }
}
