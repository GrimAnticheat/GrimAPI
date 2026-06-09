package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.AbstractEventChannel;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

public abstract class GrimVerboseCheckEvent<CHANNEL extends EventChannel<?, ?>>
        extends GrimCheckEvent<CHANNEL> {
    private Supplier<String> verboseSupplier = () -> "";

    /** Pool constructor — fields populated via {@link #init(GrimUser, AbstractCheck, String)}. */
    protected GrimVerboseCheckEvent() {
        super();
    }

    public GrimVerboseCheckEvent(GrimUser user, AbstractCheck check, String verbose) {
        super(user, check);
        setVerboseSupplier(constant(verbose));
    }

    public GrimVerboseCheckEvent(GrimUser user, AbstractCheck check, Supplier<String> verboseSupplier) {
        super(user, check);
        setVerboseSupplier(verboseSupplier);
    }

    @ApiStatus.Internal
    protected void init(GrimUser user, AbstractCheck check, String verbose) {
        init(user, check, constant(verbose));
    }

    @ApiStatus.Internal
    protected void init(GrimUser user, AbstractCheck check, Supplier<String> verboseSupplier) {
        super.init(user, check);
        setVerboseSupplier(verboseSupplier);
    }

    /**
     * Returns a memoized supplier for the human verbose string. Calling
     * {@link Supplier#get()} computes the string at most once for this event.
     */
    public @NotNull Supplier<String> getVerboseSupplier() {
        return verboseSupplier;
    }

    /**
     * @deprecated Prefer {@link #getVerboseSupplier()} so listeners that do
     * not need the human string do not force verbose rendering.
     */
    @Deprecated
    public @NotNull String getVerbose() {
        return verboseSupplier.get();
    }

    protected static @NotNull Supplier<String> constant(String verbose) {
        String value = verbose == null ? "" : verbose;
        return () -> value;
    }

    protected static @NotNull Supplier<String> memoize(@NotNull Supplier<String> supplier) {
        return new Supplier<>() {
            private String value;
            private boolean computed;

            @Override
            public String get() {
                if (!computed) {
                    value = safeGet(supplier);
                    computed = true;
                }
                return value;
            }
        };
    }

    private void setVerboseSupplier(Supplier<String> verboseSupplier) {
        this.verboseSupplier = memoize(verboseSupplier == null ? () -> "" : verboseSupplier);
    }

    private static @NotNull String safeGet(@NotNull Supplier<String> supplier) {
        try {
            String value = supplier.get();
            return value == null ? "" : value;
        } catch (Throwable ignored) {
            return "";
        }
    }

    /**
     * Abstract-level verbose-check handler. Fires for every concrete
     * {@code GrimVerboseCheckEvent} subtype — FlagEvent and
     * CommandExecuteEvent out of the box, plus any addon subtypes that
     * opt into bridging. Does not fire for
     * {@link CompletePredictionEvent}, which extends {@link GrimCheckEvent}
     * directly and has no verbose field.
     */
    @Deprecated
    @FunctionalInterface
    public interface Handler {
        boolean onVerboseCheck(@NotNull GrimUser user, @NotNull AbstractCheck check,
                               @NotNull String verbose, boolean currentlyCancelled);
    }

    @FunctionalInterface
    public interface LazyHandler {
        boolean onVerboseCheck(@NotNull GrimUser user, @NotNull AbstractCheck check,
                               @NotNull Supplier<String> verbose, boolean currentlyCancelled);
    }

    public static final class Channel extends AbstractEventChannel<GrimVerboseCheckEvent<?>, LazyHandler> {
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Channel() {
            super((Class<GrimVerboseCheckEvent<?>>) (Class) GrimVerboseCheckEvent.class, LazyHandler.class);
        }

        public void onVerboseCheckLazy(@NotNull GrimPlugin plugin, @NotNull LazyHandler handler) {
            subscribeAbstract(handler, 0, false, plugin);
        }

        public void onVerboseCheckLazy(@NotNull GrimPlugin plugin, @NotNull LazyHandler handler, int priority) {
            subscribeAbstract(handler, priority, false, plugin);
        }

        public void onVerboseCheckLazy(@NotNull GrimPlugin plugin, @NotNull LazyHandler handler, int priority, boolean ignoreCancelled) {
            subscribeAbstract(handler, priority, ignoreCancelled, plugin);
        }

        /**
         * @deprecated Prefer {@link #onVerboseCheckLazy(GrimPlugin, LazyHandler)}
         * so verbose rendering remains lazy when the handler does not need it.
         */
        @Deprecated
        public void onVerboseCheck(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            onVerboseCheckLazy(plugin, adapt(handler));
        }

        /**
         * @deprecated Prefer {@link #onVerboseCheckLazy(GrimPlugin, LazyHandler, int)}.
         */
        @Deprecated
        public void onVerboseCheck(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            onVerboseCheckLazy(plugin, adapt(handler), priority);
        }

        /**
         * @deprecated Prefer {@link #onVerboseCheckLazy(GrimPlugin, LazyHandler, int, boolean)}.
         */
        @Deprecated
        public void onVerboseCheck(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            onVerboseCheckLazy(plugin, adapt(handler), priority, ignoreCancelled);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onVerboseCheckLazy(@NotNull Object pluginContext, @NotNull LazyHandler handler) {
            subscribeAbstractResolving(pluginContext, handler, 0, false);
        }

        /** @deprecated see {@link #onVerboseCheckLazy(Object, LazyHandler)}. */
        @Deprecated
        public void onVerboseCheckLazy(@NotNull Object pluginContext, @NotNull LazyHandler handler, int priority) {
            subscribeAbstractResolving(pluginContext, handler, priority, false);
        }

        /** @deprecated see {@link #onVerboseCheckLazy(Object, LazyHandler)}. */
        @Deprecated
        public void onVerboseCheckLazy(@NotNull Object pluginContext, @NotNull LazyHandler handler, int priority, boolean ignoreCancelled) {
            subscribeAbstractResolving(pluginContext, handler, priority, ignoreCancelled);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onVerboseCheck(@NotNull Object pluginContext, @NotNull Handler handler) {
            onVerboseCheckLazy(pluginContext, adapt(handler));
        }

        /** @deprecated see {@link #onVerboseCheck(Object, Handler)}. */
        @Deprecated
        public void onVerboseCheck(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onVerboseCheckLazy(pluginContext, adapt(handler), priority);
        }

        /** @deprecated see {@link #onVerboseCheck(Object, Handler)}. */
        @Deprecated
        public void onVerboseCheck(@NotNull Object pluginContext, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            onVerboseCheckLazy(pluginContext, adapt(handler), priority, ignoreCancelled);
        }

        private static @NotNull LazyHandler adapt(@NotNull Handler handler) {
            return (user, check, verbose, cancelled) -> handler.onVerboseCheck(user, check, verbose.get(), cancelled);
        }
    }
}
