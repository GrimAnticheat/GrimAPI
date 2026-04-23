package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.NotNull;

/**
 * Fired when Grim receives an inbound response for a transaction packet that
 * it previously sent.
 *
 * <p>Grim cancels these inbound packets by default, controlled by the
 * {@code disable-pong-cancelling} option in {@code config.yml}. The
 * {@code packetCancelled} parameter reflects whether Grim cancelled packet
 * handling; it is not an event-cancellation flag (this event is observational
 * and not cancellable).
 *
 * <p>Only fires for transactions initiated by Grim, on the Netty thread
 * associated with the user.
 */
public final class GrimTransactionReceivedEvent extends GrimEvent<GrimTransactionReceivedEvent.Channel> {
    private GrimTransactionReceivedEvent() {
        // Never instantiated — exists only as a Class key for bus.get(GrimTransactionReceivedEvent.class).
    }

    @FunctionalInterface
    public interface Handler {
        void onTransactionReceived(@NotNull GrimUser user, int transactionId, boolean packetCancelled, long timestamp);
    }

    public static final class Channel extends EventChannel<GrimTransactionReceivedEvent, Handler> {
        public Channel() {
            super(GrimTransactionReceivedEvent.class, Handler.class);
        }

        public void onTransactionReceived(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onTransactionReceived(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onTransactionReceived(@NotNull Object pluginContext, @NotNull Handler handler) {
            onTransactionReceived(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onTransactionReceived(Object, Handler)}. */
        @Deprecated
        public void onTransactionReceived(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onTransactionReceived(resolvePlugin(pluginContext), handler, priority);
        }

        public void fire(@NotNull GrimUser user, int transactionId, boolean packetCancelled, long timestamp) {
            Entry<Handler>[] entries = entries();
            for (Entry<Handler> e : entries) {
                try {
                    e.handler.onTransactionReceived(user, transactionId, packetCancelled, timestamp);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull GrimTransactionReceivedEvent event, @NotNull Handler handler, boolean cancelled) {
            throw new UnsupportedOperationException("GrimTransactionReceivedEvent has no legacy representation");
        }

        @org.jetbrains.annotations.ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, id, c, ts) -> abstractHandler.onAnyEvent(GrimTransactionReceivedEvent.class, false);
        }
    }
}
