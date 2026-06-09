package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

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
     *
     * @deprecated Prefer {@link LazyHandler}; this handler forces verbose
     * rendering before your callback runs.
     */
    @Deprecated
    @FunctionalInterface
    public interface Handler {
        boolean onCommandExecute(@NotNull GrimUser user, @NotNull AbstractCheck check,
                                 @NotNull String verbose, @NotNull String command,
                                 boolean currentlyCancelled);
    }

    @FunctionalInterface
    public interface LazyHandler {
        boolean onCommandExecute(@NotNull GrimUser user, @NotNull AbstractCheck check,
                                 @NotNull Supplier<String> verbose, @NotNull String command,
                                 boolean currentlyCancelled);
    }

    public static final class Channel extends EventChannel<CommandExecuteEvent, LazyHandler> {
        private final ThreadLocal<CommandExecuteEvent> legacyPool = ThreadLocal.withInitial(CommandExecuteEvent::new);

        public Channel() {
            super(CommandExecuteEvent.class, LazyHandler.class);
        }

        public void onCommandExecuteLazy(@NotNull GrimPlugin plugin, @NotNull LazyHandler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onCommandExecuteLazy(@NotNull GrimPlugin plugin, @NotNull LazyHandler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        public void onCommandExecuteLazy(@NotNull GrimPlugin plugin, @NotNull LazyHandler handler, int priority, boolean ignoreCancelled) {
            subscribe(handler, priority, ignoreCancelled, plugin, null);
        }

        /**
         * @deprecated Prefer {@link #onCommandExecuteLazy(GrimPlugin, LazyHandler)}.
         */
        @Deprecated
        public void onCommandExecute(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            onCommandExecuteLazy(plugin, adapt(handler));
        }

        /**
         * @deprecated Prefer {@link #onCommandExecuteLazy(GrimPlugin, LazyHandler, int)}.
         */
        @Deprecated
        public void onCommandExecute(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            onCommandExecuteLazy(plugin, adapt(handler), priority);
        }

        /**
         * @deprecated Prefer {@link #onCommandExecuteLazy(GrimPlugin, LazyHandler, int, boolean)}.
         */
        @Deprecated
        public void onCommandExecute(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            onCommandExecuteLazy(plugin, adapt(handler), priority, ignoreCancelled);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onCommandExecuteLazy(@NotNull Object pluginContext, @NotNull LazyHandler handler) {
            onCommandExecuteLazy(resolvePlugin(pluginContext), handler);
        }

        /** @deprecated see {@link #onCommandExecuteLazy(Object, LazyHandler)}. */
        @Deprecated
        public void onCommandExecuteLazy(@NotNull Object pluginContext, @NotNull LazyHandler handler, int priority) {
            onCommandExecuteLazy(resolvePlugin(pluginContext), handler, priority);
        }

        /** @deprecated see {@link #onCommandExecuteLazy(Object, LazyHandler)}. */
        @Deprecated
        public void onCommandExecuteLazy(@NotNull Object pluginContext, @NotNull LazyHandler handler, int priority, boolean ignoreCancelled) {
            onCommandExecuteLazy(resolvePlugin(pluginContext), handler, priority, ignoreCancelled);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onCommandExecute(@NotNull Object pluginContext, @NotNull Handler handler) {
            onCommandExecuteLazy(pluginContext, adapt(handler));
        }

        /** @deprecated see {@link #onCommandExecute(Object, Handler)}. */
        @Deprecated
        public void onCommandExecute(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            onCommandExecuteLazy(pluginContext, adapt(handler), priority);
        }

        /** @deprecated see {@link #onCommandExecute(Object, Handler)}. */
        @Deprecated
        public void onCommandExecute(@NotNull Object pluginContext, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            onCommandExecuteLazy(pluginContext, adapt(handler), priority, ignoreCancelled);
        }

        public boolean fire(@NotNull GrimUser user, @NotNull AbstractCheck check,
                            @NotNull String verbose, @NotNull String command) {
            return fire(user, check, constant(verbose), command);
        }

        public boolean fire(@NotNull GrimUser user, @NotNull AbstractCheck check,
                            @NotNull Supplier<String> verboseSupplier, @NotNull String command) {
            Entry<LazyHandler>[] entries = entries();
            if (entries.length == 0) return false;

            Supplier<String> verbose = memoize(verboseSupplier);
            boolean cancelled = false;
            if (!hasLegacy()) {
                for (Entry<LazyHandler> e : entries) {
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
            for (Entry<LazyHandler> e : entries) {
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
        protected boolean dispatchTypedFromLegacy(@NotNull CommandExecuteEvent event, @NotNull LazyHandler handler, boolean cancelled) {
            return handler.onCommandExecute(event.getUser(), event.getCheck(), event.getVerboseSupplier(), event.getCommand(), cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull LazyHandler bridgeFromCheck(@NotNull GrimCheckEvent.Handler abstractHandler) {
            return (user, check, verbose, command, cancelled) -> abstractHandler.onCheck(user, check, cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull LazyHandler bridgeFromVerboseCheck(@NotNull GrimVerboseCheckEvent.LazyHandler abstractHandler) {
            return (user, check, verbose, command, cancelled) -> abstractHandler.onVerboseCheck(user, check, verbose, cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull LazyHandler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, check, verbose, command, cancelled) -> {
                abstractHandler.onAnyEvent(CommandExecuteEvent.class, cancelled);
                return cancelled;
            };
        }

        private static @NotNull LazyHandler adapt(@NotNull Handler handler) {
            return (user, check, verbose, command, cancelled) -> handler.onCommandExecute(user, check, verbose.get(), command, cancelled);
        }
    }
}
