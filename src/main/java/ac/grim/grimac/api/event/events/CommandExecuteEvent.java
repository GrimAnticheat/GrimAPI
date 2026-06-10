package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class CommandExecuteEvent extends GrimVerboseCheckEvent<CommandExecuteEvent.Channel> {
    private String command;

    /** Pool constructor — fields populated via {@link #init}. */
    public CommandExecuteEvent() {
        super();
    }

    public CommandExecuteEvent(GrimUser player, AbstractCheck check, String verbose, String command) {
        super(player, check, verbose);
        this.command = command;
    }

    public CommandExecuteEvent(GrimUser player, AbstractCheck check, Supplier<String> verboseSupplier, String command) {
        super(player, check, verboseSupplier);
        this.command = command;
    }

    @ApiStatus.Internal
    public void init(GrimUser user, AbstractCheck check, String verbose, String command) {
        super.init(user, check, verbose);
        this.command = command;
    }

    @ApiStatus.Internal
    public void init(GrimUser user, AbstractCheck check, Supplier<String> verboseSupplier, String command) {
        super.init(user, check, verboseSupplier);
        this.command = command;
    }

    public String getCommand() {
        return command;
    }

    /**
     * Typed command-execute handler. Returns the new cancelled state — see
     * {@link FlagEvent.Handler} for the cancellation contract.
     */
    @FunctionalInterface
    public interface Handler {
        boolean onCommandExecute(@NotNull GrimUser user, @NotNull AbstractCheck check,
                                 @NotNull String verbose, @NotNull String command,
                                 boolean currentlyCancelled);
    }

    @FunctionalInterface
    public interface SupplierHandler {
        boolean onCommandExecute(@NotNull GrimUser user, @NotNull AbstractCheck check,
                                 @NotNull Supplier<String> verbose, @NotNull String command,
                                 boolean currentlyCancelled);
    }

    public static final class Channel extends EventChannel<CommandExecuteEvent, SupplierHandler> {
        private static final AtomicBoolean STRING_HANDLER_WARNING = new AtomicBoolean();
        private final ThreadLocal<CommandExecuteEvent> legacyPool = ThreadLocal.withInitial(CommandExecuteEvent::new);

        public Channel() {
            super(CommandExecuteEvent.class, SupplierHandler.class);
        }

        public void onCommandExecuteSupplier(@NotNull GrimPlugin plugin, @NotNull SupplierHandler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onCommandExecuteSupplier(@NotNull GrimPlugin plugin, @NotNull SupplierHandler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        public void onCommandExecuteSupplier(@NotNull GrimPlugin plugin, @NotNull SupplierHandler handler, int priority, boolean ignoreCancelled) {
            subscribe(handler, priority, ignoreCancelled, plugin, null);
        }

        /**
         * @deprecated Prefer {@link #onCommandExecuteSupplier(GrimPlugin, SupplierHandler)}.
         */
        @Deprecated
        public void onCommandExecute(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            warnStringHandler(plugin);
            onCommandExecuteSupplier(plugin, adapt(handler));
        }

        /**
         * @deprecated Prefer {@link #onCommandExecuteSupplier(GrimPlugin, SupplierHandler, int)}.
         */
        @Deprecated
        public void onCommandExecute(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            warnStringHandler(plugin);
            onCommandExecuteSupplier(plugin, adapt(handler), priority);
        }

        /**
         * @deprecated Prefer {@link #onCommandExecuteSupplier(GrimPlugin, SupplierHandler, int, boolean)}.
         */
        @Deprecated
        public void onCommandExecute(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            warnStringHandler(plugin);
            onCommandExecuteSupplier(plugin, adapt(handler), priority, ignoreCancelled);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onCommandExecuteSupplier(@NotNull Object pluginContext, @NotNull SupplierHandler handler) {
            onCommandExecuteSupplier(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onCommandExecuteSupplier(Object, SupplierHandler)}. */
        @Deprecated
        public void onCommandExecuteSupplier(@NotNull Object pluginContext, @NotNull SupplierHandler handler, int priority) {
            onCommandExecuteSupplier(resolvePlugin(pluginContext), handler, priority);
        }

        /** @deprecated see {@link #onCommandExecuteSupplier(Object, SupplierHandler)}. */
        @Deprecated
        public void onCommandExecuteSupplier(@NotNull Object pluginContext, @NotNull SupplierHandler handler, int priority, boolean ignoreCancelled) {
            onCommandExecuteSupplier(resolvePlugin(pluginContext), handler, priority, ignoreCancelled);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onCommandExecute(@NotNull Object pluginContext, @NotNull Handler handler) {
            onCommandExecute(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onCommandExecute(Object, Handler)}. */
        @Deprecated
        public void onCommandExecute(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onCommandExecute(resolvePlugin(pluginContext), handler, priority);
        }

        /** @deprecated see {@link #onCommandExecute(Object, Handler)}. */
        @Deprecated
        public void onCommandExecute(@NotNull Object pluginContext, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            onCommandExecute(resolvePlugin(pluginContext), handler, priority, ignoreCancelled);
        }

        public boolean fire(@NotNull GrimUser user, @NotNull AbstractCheck check,
                            @NotNull String verbose, @NotNull String command) {
            return fire(user, check, VerboseSuppliers.constant(verbose), command);
        }

        public boolean fire(@NotNull GrimUser user, @NotNull AbstractCheck check,
                            @NotNull Supplier<String> verboseSupplier, @NotNull String command) {
            Entry<SupplierHandler>[] entries = entries();
            if (entries.length == 0) return false;

            Supplier<String> verbose = VerboseSuppliers.memoize(verboseSupplier);
            boolean cancelled = false;
            if (!hasLegacy()) {
                for (Entry<SupplierHandler> e : entries) {
                    if (cancelled && !e.ignoreCancelled) continue;
                    try {
                        cancelled = e.handler.onCommandExecute(user, check, verbose, command, cancelled);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
                return cancelled;
            }

            CommandExecuteEvent pooled = legacyPool.get();
            pooled.init(user, check, verbose, command);
            for (Entry<SupplierHandler> e : entries) {
                if (cancelled && !e.ignoreCancelled) continue;
                try {
                    if (e.legacyListener != null) {
                        pooled.setCancelled(cancelled);
                        e.<CommandExecuteEvent>legacyListenerAs().handle(pooled);
                        cancelled = pooled.isCancelled();
                    } else {
                        cancelled = e.handler.onCommandExecute(user, check, verbose, command, cancelled);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
            return cancelled;
        }

        @Override
        protected boolean dispatchTypedFromLegacy(@NotNull CommandExecuteEvent event, @NotNull SupplierHandler handler, boolean cancelled) {
            return handler.onCommandExecute(event.getUser(), event.getCheck(), event.verboseSupplier(), event.getCommand(), cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull SupplierHandler bridgeFromCheck(@NotNull GrimCheckEvent.Handler abstractHandler) {
            return (user, check, verbose, command, cancelled) -> abstractHandler.onCheck(user, check, cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull SupplierHandler bridgeFromVerboseCheck(@NotNull GrimVerboseCheckEvent.SupplierHandler abstractHandler) {
            return (user, check, verbose, command, cancelled) -> abstractHandler.onVerboseCheck(user, check, verbose, cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull SupplierHandler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, check, verbose, command, cancelled) -> {
                abstractHandler.onAnyEvent(CommandExecuteEvent.class, cancelled);
                return cancelled;
            };
        }

        private static @NotNull SupplierHandler adapt(@NotNull Handler handler) {
            return (user, check, verbose, command, cancelled) -> handler.onCommandExecute(user, check, verbose.get(), command, cancelled);
        }

        private static void warnStringHandler(@NotNull GrimPlugin plugin) {
            if (STRING_HANDLER_WARNING.compareAndSet(false, true)) {
                plugin.getLogger().warning("Deprecated Grim command-execute string listener registered; use the Supplier<String> verbose handler and call verbose.get() only when text is needed.");
            }
        }
    }
}
