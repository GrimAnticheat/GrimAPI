package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.NotNull;

/**
 * Fired when Grim sends a transaction packet to the client.
 *
 * <p>Plugins can use this event to track transaction ids issued by Grim and
 * correlate them with the matching {@link GrimTransactionReceivedEvent} once a
 * response is received.
 *
 * <p>Fires on the Netty thread associated with the user. Observational, not
 * cancellable.
 */
public final class GrimTransactionSendEvent extends GrimEvent<GrimTransactionSendEvent.Channel> {
    private GrimTransactionSendEvent() {
        // Never instantiated — exists only as a Class key for bus.get(GrimTransactionSendEvent.class).
    }

    @FunctionalInterface
    public interface Handler {
        void onTransactionSend(@NotNull GrimUser user, int transactionId, long timestamp);
    }

    public static final class Channel extends EventChannel<GrimTransactionSendEvent, Handler> {
        public Channel() {
            super(GrimTransactionSendEvent.class, Handler.class);
        }

        public void onTransactionSend(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onTransactionSend(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onTransactionSend(@NotNull Object pluginContext, @NotNull Handler handler) {
            onTransactionSend(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onTransactionSend(Object, Handler)}. */
        @Deprecated
        public void onTransactionSend(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onTransactionSend(resolvePlugin(pluginContext), handler, priority);
        }

        public void fire(@NotNull GrimUser user, int transactionId, long timestamp) {
            Entry<Handler>[] entries = entries();
            for (Entry<Handler> e : entries) {
                try {
                    e.handler.onTransactionSend(user, transactionId, timestamp);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull GrimTransactionSendEvent event, @NotNull Handler handler, boolean cancelled) {
            throw new UnsupportedOperationException("GrimTransactionSendEvent has no legacy representation");
        }

        @org.jetbrains.annotations.ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, id, ts) -> abstractHandler.onAnyEvent(GrimTransactionSendEvent.class, false);
        }
    }
}
