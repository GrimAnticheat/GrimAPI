package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.AbstractEventChannel;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;
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

    /** Returns the human verbose string, rendering it lazily on first use. */
    public @NotNull String getVerbose() {
        return verboseSupplier.get();
    }

    @ApiStatus.Internal
    final @NotNull Supplier<String> verboseSupplier() {
        return verboseSupplier;
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
    @FunctionalInterface
    public interface Handler {
        boolean onVerboseCheck(@NotNull GrimUser user, @NotNull AbstractCheck check,
                               @NotNull Supplier<String> verbose, boolean currentlyCancelled);
    }

    /**
     * @deprecated Prefer {@link Handler}. This string handler forces verbose
     * rendering before your callback runs.
     */
    @Deprecated
    @FunctionalInterface
    public interface StringHandler {
        boolean onVerboseCheck(@NotNull GrimUser user, @NotNull AbstractCheck check,
                               @NotNull String verbose, boolean currentlyCancelled);
    }

    public static final class Channel extends AbstractEventChannel<GrimVerboseCheckEvent<?>, Handler> {
        private static final AtomicBoolean STRING_HANDLER_WARNING = new AtomicBoolean();

        @SuppressWarnings({"unchecked", "rawtypes"})
        public Channel() {
            super((Class<GrimVerboseCheckEvent<?>>) (Class) GrimVerboseCheckEvent.class, Handler.class);
        }

        public void onVerboseCheck(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribeAbstract(handler, 0, false, plugin);
        }

        public void onVerboseCheck(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribeAbstract(handler, priority, false, plugin);
        }

        public void onVerboseCheck(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            subscribeAbstract(handler, priority, ignoreCancelled, plugin);
        }

        /**
         * @deprecated Prefer {@link #onVerboseCheck(GrimPlugin, Handler)}.
         */
        @Deprecated
        public void onVerboseCheckString(@NotNull GrimPlugin plugin, @NotNull StringHandler handler) {
            warnStringHandler(plugin);
            onVerboseCheck(plugin, adapt(handler));
        }

        /**
         * @deprecated Prefer {@link #onVerboseCheck(GrimPlugin, Handler, int)}.
         */
        @Deprecated
        public void onVerboseCheckString(@NotNull GrimPlugin plugin, @NotNull StringHandler handler, int priority) {
            warnStringHandler(plugin);
            onVerboseCheck(plugin, adapt(handler), priority);
        }

        /**
         * @deprecated Prefer {@link #onVerboseCheck(GrimPlugin, Handler, int, boolean)}.
         */
        @Deprecated
        public void onVerboseCheckString(@NotNull GrimPlugin plugin, @NotNull StringHandler handler, int priority, boolean ignoreCancelled) {
            warnStringHandler(plugin);
            onVerboseCheck(plugin, adapt(handler), priority, ignoreCancelled);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onVerboseCheck(@NotNull Object pluginContext, @NotNull Handler handler) {
            subscribeAbstractResolving(pluginContext, handler, 0, false);
        }

        /** @deprecated see {@link #onVerboseCheck(Object, Handler)}. */
        @Deprecated
        public void onVerboseCheck(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            subscribeAbstractResolving(pluginContext, handler, priority, false);
        }

        /** @deprecated see {@link #onVerboseCheck(Object, Handler)}. */
        @Deprecated
        public void onVerboseCheck(@NotNull Object pluginContext, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            subscribeAbstractResolving(pluginContext, handler, priority, ignoreCancelled);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onVerboseCheckString(@NotNull Object pluginContext, @NotNull StringHandler handler) {
            GrimPlugin plugin = resolvePlugin(pluginContext);
            onVerboseCheckString(plugin, handler);
        }

        /** @deprecated see {@link #onVerboseCheckString(Object, StringHandler)}. */
        @Deprecated
        public void onVerboseCheckString(@NotNull Object pluginContext, @NotNull StringHandler handler, int priority) {
            GrimPlugin plugin = resolvePlugin(pluginContext);
            onVerboseCheckString(plugin, handler, priority);
        }

        /** @deprecated see {@link #onVerboseCheckString(Object, StringHandler)}. */
        @Deprecated
        public void onVerboseCheckString(@NotNull Object pluginContext, @NotNull StringHandler handler, int priority, boolean ignoreCancelled) {
            GrimPlugin plugin = resolvePlugin(pluginContext);
            onVerboseCheckString(plugin, handler, priority, ignoreCancelled);
        }

        private static @NotNull Handler adapt(@NotNull StringHandler handler) {
            return (user, check, verbose, cancelled) -> handler.onVerboseCheck(user, check, verbose.get(), cancelled);
        }

        private static void warnStringHandler(@NotNull GrimPlugin plugin) {
            if (STRING_HANDLER_WARNING.compareAndSet(false, true)) {
                plugin.getLogger().warning("Deprecated Grim verbose string listener registered; use the Supplier<String> verbose handler and call verbose.get() only when text is needed.");
            }
        }
    }
}
