package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

public class CompletePredictionEvent extends GrimCheckEvent<CompletePredictionEvent.Channel> {
    private double offset;

    /** Pool constructor — fields populated via {@link #init}. */
    public CompletePredictionEvent() {
        super();
    }

    public CompletePredictionEvent(GrimUser player, AbstractCheck check, double offset) {
        super(player, check);
        this.offset = offset;
    }

    @ApiStatus.Internal
    public void init(GrimUser user, AbstractCheck check, double offset) {
        super.init(user, check);
        this.offset = offset;
    }

    public double getOffset() {
        return offset;
    }

    /**
     * Typed prediction-complete handler. Returns the new cancelled state — see
     * {@link FlagEvent.Handler} for the cancellation contract.
     */
    @FunctionalInterface
    public interface Handler {
        boolean onCompletePrediction(@NotNull GrimUser user, @NotNull AbstractCheck check,
                                     double offset, boolean currentlyCancelled);
    }

    public static final class Channel extends EventChannel<CompletePredictionEvent, Handler> {
        private final ThreadLocal<CompletePredictionEvent> legacyPool =
                ThreadLocal.withInitial(CompletePredictionEvent::new);

        public Channel() {
            super(CompletePredictionEvent.class, Handler.class);
        }

        public void onCompletePrediction(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onCompletePrediction(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        public void onCompletePrediction(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            subscribe(handler, priority, ignoreCancelled, plugin, null);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onCompletePrediction(@NotNull Object pluginContext, @NotNull Handler handler) {
            onCompletePrediction(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onCompletePrediction(Object, Handler)}. */
        @Deprecated
        public void onCompletePrediction(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onCompletePrediction(resolvePlugin(pluginContext), handler, priority);
        }

        /** @deprecated see {@link #onCompletePrediction(Object, Handler)}. */
        @Deprecated
        public void onCompletePrediction(@NotNull Object pluginContext, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            onCompletePrediction(resolvePlugin(pluginContext), handler, priority, ignoreCancelled);
        }

        public boolean fire(@NotNull GrimUser user, @NotNull AbstractCheck check, double offset) {
            Entry<Handler>[] entries = entries();
            if (entries.length == 0) return false;

            boolean cancelled = false;
            if (!hasLegacy()) {
                for (Entry<Handler> e : entries) {
                    if (cancelled && !e.ignoreCancelled) continue;
                    try {
                        cancelled = e.handler.onCompletePrediction(user, check, offset, cancelled);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
                return cancelled;
            }

            CompletePredictionEvent pooled = legacyPool.get();
            pooled.init(user, check, offset);
            for (Entry<Handler> e : entries) {
                if (cancelled && !e.ignoreCancelled) continue;
                try {
                    if (e.legacyListener != null) {
                        pooled.setCancelled(cancelled);
                        e.<CompletePredictionEvent>legacyListenerAs().handle(pooled);
                        cancelled = pooled.isCancelled();
                    } else {
                        cancelled = e.handler.onCompletePrediction(user, check, offset, cancelled);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
            return cancelled;
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull CompletePredictionEvent event, @NotNull Handler handler, boolean cancelled) {
            return handler.onCompletePrediction(event.getUser(), event.getCheck(), event.getOffset(), cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromCheck(@NotNull GrimCheckEvent.Handler abstractHandler) {
            return (user, check, offset, cancelled) -> abstractHandler.onCheck(user, check, cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, check, offset, cancelled) -> {
                abstractHandler.onAnyEvent(CompletePredictionEvent.class, cancelled);
                return cancelled;
            };
        }
    }
}
