package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

public class FlagEvent extends GrimVerboseCheckEvent<FlagEvent.Channel> {

    /** Pool constructor — fields populated via {@link #init}. */
    public FlagEvent() {
        super();
    }

    public FlagEvent(GrimUser user, AbstractCheck check, String verbose) {
        super(user, check, verbose);
    }

    @ApiStatus.Internal
    @Override
    public void init(GrimUser user, AbstractCheck check, String verbose) {
        super.init(user, check, verbose);
    }

    /**
     * Typed flag handler. Returns the new cancelled state — returning
     * {@code true} cancels, {@code false} leaves uncancelled. The last
     * parameter carries the cancelled state threaded through priority-ordered
     * dispatch; a handler registered with {@code ignoreCancelled = true}
     * still runs when a higher-priority handler already cancelled the event.
     */
    @FunctionalInterface
    public interface Handler {
        boolean onFlag(@NotNull GrimUser user, @NotNull AbstractCheck check,
                       @NotNull String verbose, boolean currentlyCancelled);
    }

    public static final class Channel extends EventChannel<FlagEvent, Handler> {
        private final ThreadLocal<FlagEvent> legacyPool = ThreadLocal.withInitial(FlagEvent::new);

        public Channel() {
            super(FlagEvent.class, Handler.class);
        }

        public void onFlag(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onFlag(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        public void onFlag(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            subscribe(handler, priority, ignoreCancelled, plugin, null);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onFlag(@NotNull Object pluginContext, @NotNull Handler handler) {
            onFlag(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onFlag(Object, Handler)}. */
        @Deprecated
        public void onFlag(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onFlag(resolvePlugin(pluginContext), handler, priority);
        }

        /** @deprecated see {@link #onFlag(Object, Handler)}. */
        @Deprecated
        public void onFlag(@NotNull Object pluginContext, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            onFlag(resolvePlugin(pluginContext), handler, priority, ignoreCancelled);
        }

        /** Dispatches the event. Returns the final cancelled state after all handlers run. */
        public boolean fire(@NotNull GrimUser user, @NotNull AbstractCheck check, @NotNull String verbose) {
            Entry<Handler>[] entries = entries();
            if (entries.length == 0) return false;

            boolean cancelled = false;
            if (!hasLegacy()) {
                for (Entry<Handler> e : entries) {
                    if (cancelled && !e.ignoreCancelled) continue;
                    try {
                        cancelled = e.handler.onFlag(user, check, verbose, cancelled);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
                return cancelled;
            }

            FlagEvent pooled = legacyPool.get();
            pooled.init(user, check, verbose);
            for (Entry<Handler> e : entries) {
                if (cancelled && !e.ignoreCancelled) continue;
                try {
                    if (e.legacyListener != null) {
                        pooled.setCancelled(cancelled);
                        e.<FlagEvent>legacyListenerAs().handle(pooled);
                        cancelled = pooled.isCancelled();
                    } else {
                        cancelled = e.handler.onFlag(user, check, verbose, cancelled);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
            return cancelled;
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull FlagEvent event, @NotNull Handler handler, boolean cancelled) {
            return handler.onFlag(event.getUser(), event.getCheck(), event.getVerbose(), cancelled);
        }

        /** Bridge from {@link GrimCheckEvent.Handler} — used by the abstract channel when a check-level subscriber registers. */
        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromCheck(@NotNull GrimCheckEvent.Handler abstractHandler) {
            return (user, check, verbose, cancelled) -> abstractHandler.onCheck(user, check, cancelled);
        }

        /** Bridge from {@link GrimVerboseCheckEvent.Handler}. */
        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromVerboseCheck(@NotNull GrimVerboseCheckEvent.Handler abstractHandler) {
            return (user, check, verbose, cancelled) -> abstractHandler.onVerboseCheck(user, check, verbose, cancelled);
        }

        /** Bridge from root-level {@link ac.grim.grimac.api.event.GrimEvent.Handler} — observational only, cancelled is passed through unchanged. */
        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, check, verbose, cancelled) -> {
                abstractHandler.onAnyEvent(FlagEvent.class, cancelled);
                return cancelled;
            };
        }
    }
}
