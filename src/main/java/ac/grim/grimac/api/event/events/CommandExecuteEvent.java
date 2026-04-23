package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

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

    @ApiStatus.Internal
    public void init(GrimUser user, AbstractCheck check, String verbose, String command) {
        super.init(user, check, verbose);
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

    public static final class Channel extends EventChannel<CommandExecuteEvent, Handler> {
        private final ThreadLocal<CommandExecuteEvent> legacyPool = ThreadLocal.withInitial(CommandExecuteEvent::new);

        public Channel() {
            super(CommandExecuteEvent.class, Handler.class);
        }

        public void onCommandExecute(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribe(handler, 0, false, plugin, null);
        }

        public void onCommandExecute(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribe(handler, priority, false, plugin, null);
        }

        public void onCommandExecute(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            subscribe(handler, priority, ignoreCancelled, plugin, null);
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
            Entry<Handler>[] entries = entries();
            if (entries.length == 0) return false;

            boolean cancelled = false;
            if (!hasLegacy()) {
                for (Entry<Handler> e : entries) {
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
            for (Entry<Handler> e : entries) {
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
        protected boolean dispatchTypedFromLegacy(@NotNull CommandExecuteEvent event, @NotNull Handler handler, boolean cancelled) {
            return handler.onCommandExecute(event.getUser(), event.getCheck(), event.getVerbose(), event.getCommand(), cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromCheck(@NotNull GrimCheckEvent.Handler abstractHandler) {
            return (user, check, verbose, command, cancelled) -> abstractHandler.onCheck(user, check, cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromVerboseCheck(@NotNull GrimVerboseCheckEvent.Handler abstractHandler) {
            return (user, check, verbose, command, cancelled) -> abstractHandler.onVerboseCheck(user, check, verbose, cancelled);
        }

        @ApiStatus.Internal
        public static @NotNull Handler bridgeFromAny(@NotNull ac.grim.grimac.api.event.GrimEvent.Handler abstractHandler) {
            return (user, check, verbose, command, cancelled) -> {
                abstractHandler.onAnyEvent(CommandExecuteEvent.class, cancelled);
                return cancelled;
            };
        }
    }
}
