package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

public class FlagEvent extends GrimVerboseCheckEvent<FlagEvent.Channel> {

    /** Pool constructor — fields populated via {@link #init}. */
    public FlagEvent() {
        super();
    }

    public FlagEvent(GrimUser user, AbstractCheck check, String verbose) {
        super(user, check, verbose);
    }

    public FlagEvent(GrimUser user, AbstractCheck check, Supplier<String> verboseSupplier) {
        super(user, check, verboseSupplier);
    }

    @ApiStatus.Internal
    @Override
    public void init(GrimUser user, AbstractCheck check, String verbose) {
        super.init(user, check, verbose);
    }

    @ApiStatus.Internal
    public void init(GrimUser user, AbstractCheck check, Supplier<String> verboseSupplier) {
        super.init(user, check, verboseSupplier);
    }

    /**
     * Typed flag handler. Returns the new cancelled state — returning
     * {@code true} cancels, {@code false} leaves uncancelled. The last
     * parameter carries the cancelled state threaded through priority-ordered
     * dispatch; a handler registered with {@code ignoreCancelled = true}
     * still runs when a higher-priority handler already cancelled the event.
     *
     * @deprecated Prefer {@link LazyHandler}; this handler forces verbose
     * rendering before your callback runs.
     */
    @Deprecated
    @FunctionalInterface
    public interface Handler {
        boolean onFlag(@NotNull GrimUser user, @NotNull AbstractCheck check,
                       @NotNull String verbose, boolean currentlyCancelled);
    }

    /**
     * Typed flag handler that receives a memoized verbose supplier. Calling
     * {@link Supplier#get()} computes the human string at most once.
     */
    @FunctionalInterface
    public interface LazyHandler {
        boolean onFlag(@NotNull GrimUser user, @NotNull AbstractCheck check,
                       @NotNull Supplier<String> verbose, boolean currentlyCancelled);
    }

    public static final class Channel extends EventChannel<FlagEvent, LazyHandler> {
        private final ThreadLocal<FlagEvent> legacyPool = ThreadLocal.withInitial(FlagEvent::new);

        public Channel() {
            super(FlagEvent.class, LazyHandler.class);
        }

        public void onFlagLazy(@NotNull GrimPlugin plugin, @NotNull LazyHandler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onFlagLazy(@NotNull GrimPlugin plugin, @NotNull LazyHandler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        public void onFlagLazy(@NotNull GrimPlugin plugin, @NotNull LazyHandler handler, int priority, boolean ignoreCancelled) {
            subscribe(handler, priority, ignoreCancelled, plugin, null);
        }

        /**
         * @deprecated Prefer {@link #onFlagLazy(GrimPlugin, LazyHandler)} so
         * verbose rendering remains lazy when the handler does not need it.
         */
        @Deprecated
        public void onFlag(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            onFlagLazy(plugin, adapt(handler));
        }

        /**
         * @deprecated Prefer {@link #onFlagLazy(GrimPlugin, LazyHandler, int)}.
         */
        @Deprecated
        public void onFlag(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            onFlagLazy(plugin, adapt(handler), priority);
        }

        /**
         * @deprecated Prefer {@link #onFlagLazy(GrimPlugin, LazyHandler, int, boolean)}.
         */
        @Deprecated
        public void onFlag(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            onFlagLazy(plugin, adapt(handler), priority, ignoreCancelled);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onFlagLazy(@NotNull Object pluginContext, @NotNull LazyHandler handler) {
            onFlagLazy(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onFlagLazy(Object, LazyHandler)}. */
        @Deprecated
        public void onFlagLazy(@NotNull Object pluginContext, @NotNull LazyHandler handler, int priority) {
            onFlagLazy(resolvePlugin(pluginContext), handler, priority);
        }

        /** @deprecated see {@link #onFlagLazy(Object, LazyHandler)}. */
        @Deprecated
        public void onFlagLazy(@NotNull Object pluginContext, @NotNull LazyHandler handler, int priority, boolean ignoreCancelled) {
            onFlagLazy(resolvePlugin(pluginContext), handler, priority, ignoreCancelled);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onFlag(@NotNull Object pluginContext, @NotNull Handler handler) {
            onFlagLazy(pluginContext, adapt(handler));
        }

        /** @deprecated see {@link #onFlag(Object, Handler)}. */
        @Deprecated
        public void onFlag(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onFlagLazy(pluginContext, adapt(handler), priority);
        }

        /** @deprecated see {@link #onFlag(Object, Handler)}. */
        @Deprecated
        public void onFlag(@NotNull Object pluginContext, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            onFlagLazy(pluginContext, adapt(handler), priority, ignoreCancelled);
        }

        /** Dispatches the event. Returns the final cancelled state after all handlers run. */
        public boolean fire(@NotNull GrimUser user, @NotNull AbstractCheck check, @NotNull String verbose) {
            return fire(user, check, constant(verbose));
        }

        /** Dispatches the event. Returns the final cancelled state after all handlers run. */
        public boolean fire(@NotNull GrimUser user, @NotNull AbstractCheck check, @NotNull Supplier<String> verboseSupplier) {
            Entry<LazyHandler>[] entries = entries();
            if (entries.length == 0) return false;

            Supplier<String> verbose = memoize(verboseSupplier);
            boolean cancelled = false;
            if (!hasLegacy()) {
                for (Entry<LazyHandler> e : entries) {
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
            for (Entry<LazyHandler> e : entries) {
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
        protected boolean dispatchTypedFromLegacy(@NotNull FlagEvent event, @NotNull LazyHandler handler, boolean cancelled) {
            return handler.onFlag(event.getUser(), event.getCheck(), event.getVerboseSupplier(), cancelled);
        }

        /** Bridge from {@link GrimCheckEvent.Handler} — used by the abstract channel when a check-level subscriber registers. */
        @ApiStatus.Internal
        public static @NotNull LazyHandler bridgeFromCheck(@NotNull GrimCheckEvent.Handler abstractHandler) {
            return (user, check, verbose, cancelled) -> abstractHandler.onCheck(user, check, cancelled);
        }

        /** Bridge from {@link GrimVerboseCheckEvent.Handler}. */
        @ApiStatus.Internal
        public static @NotNull LazyHandler bridgeFromVerboseCheck(@NotNull GrimVerboseCheckEvent.LazyHandler abstractHandler) {
            return (user, check, verbose, cancelled) -> abstractHandler.onVerboseCheck(user, check, verbose, cancelled);
        }

        /** Bridge from root-level {@link ac.grim.grimac.api.event.GrimEvent.Handler} — observational only, cancelled is passed through unchanged. */
        @ApiStatus.Internal
        public static @NotNull LazyHandler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, check, verbose, cancelled) -> {
                abstractHandler.onAnyEvent(FlagEvent.class, cancelled);
                return cancelled;
            };
        }

        private static @NotNull LazyHandler adapt(@NotNull Handler handler) {
            return (user, check, verbose, cancelled) -> handler.onFlag(user, check, verbose.get(), cancelled);
        }
    }
}
