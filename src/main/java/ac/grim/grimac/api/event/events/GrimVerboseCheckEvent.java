package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.AbstractEventChannel;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

public abstract class GrimVerboseCheckEvent<CHANNEL extends EventChannel<?, ?>>
        extends GrimCheckEvent<CHANNEL> {
    private String verbose;

    /** Pool constructor — fields populated via {@link #init(GrimUser, AbstractCheck, String)}. */
    protected GrimVerboseCheckEvent() {
        super();
    }

    public GrimVerboseCheckEvent(GrimUser user, AbstractCheck check, String verbose) {
        super(user, check);
        this.verbose = verbose;
    }

    @ApiStatus.Internal
    protected void init(GrimUser user, AbstractCheck check, String verbose) {
        super.init(user, check);
        this.verbose = verbose;
    }

    public String getVerbose() {
        return verbose;
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
                               @NotNull String verbose, boolean currentlyCancelled);
    }

    public static final class Channel extends AbstractEventChannel<GrimVerboseCheckEvent<?>, Handler> {
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
    }
}
