package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.AbstractEventChannel;
import ac.grim.grimac.api.event.Cancellable;
import ac.grim.grimac.api.event.EventChannel;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.plugin.GrimPlugin;
import lombok.Getter;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

public abstract class GrimCheckEvent<CHANNEL extends EventChannel<?, ?>>
        extends GrimEvent<CHANNEL> implements GrimUserEvent, Cancellable {
    private GrimUser user;
    @Getter
    protected AbstractCheck check;
    private boolean cancelled;

    /** Pool constructor — fields populated via {@link #init(GrimUser, AbstractCheck)}. */
    protected GrimCheckEvent() {
        super(true); // Async
    }

    public GrimCheckEvent(GrimUser user, AbstractCheck check) {
        super(true); // Async
        this.user = user;
        this.check = check;
    }

    @ApiStatus.Internal
    protected void init(GrimUser user, AbstractCheck check) {
        resetForReuse();
        this.user = user;
        this.check = check;
        this.cancelled = false;
    }

    @Override
    public GrimUser getUser() {
        return user;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    @Override
    public boolean isCancellable() {
        return true;
    }

    public double getViolations() {
        return check.getViolations();
    }

    public boolean isSetback() {
        return check.getViolations() > check.getSetbackVL();
    }

    /**
     * Abstract-level check handler. Fires for every concrete
     * {@code GrimCheckEvent} subtype (FlagEvent, CompletePredictionEvent,
     * CommandExecuteEvent, and any addon subtypes that opt into bridging).
     *
     * <p>Returns the new cancelled state — the value is threaded back into
     * the priority-ordered dispatch loop of whichever concrete subtype
     * fired, so a high-priority abstract subscriber can cancel and
     * lower-priority direct subscribers to the concrete event see the
     * cancellation just like any other priority-ordered handler.
     */
    @FunctionalInterface
    public interface Handler {
        boolean onCheck(@NotNull GrimUser user, @NotNull AbstractCheck check, boolean currentlyCancelled);
    }

    public static final class Channel extends AbstractEventChannel<GrimCheckEvent<?>, Handler> {
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Channel() {
            super((Class<GrimCheckEvent<?>>) (Class) GrimCheckEvent.class, Handler.class);
        }

        public void onCheck(@NotNull GrimPlugin plugin, @NotNull Handler handler) {
            subscribeAbstract(handler, 0, false, plugin);
        }

        public void onCheck(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority) {
            subscribeAbstract(handler, priority, false, plugin);
        }

        public void onCheck(@NotNull GrimPlugin plugin, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            subscribeAbstract(handler, priority, ignoreCancelled, plugin);
        }

        /** @deprecated resolve your context once at plugin enable — {@code api.getGrimPlugin(this)} — and call the {@link GrimPlugin}-taking overload. */
        @Deprecated
        public void onCheck(@NotNull Object pluginContext, @NotNull Handler handler) {
            subscribeAbstractResolving(pluginContext, handler, 0, false);
        }

        /** @deprecated see {@link #onCheck(Object, Handler)}. */
        @Deprecated
        public void onCheck(@NotNull Object pluginContext, @NotNull Handler handler, int priority) {
            subscribeAbstractResolving(pluginContext, handler, priority, false);
        }

        /** @deprecated see {@link #onCheck(Object, Handler)}. */
        @Deprecated
        public void onCheck(@NotNull Object pluginContext, @NotNull Handler handler, int priority, boolean ignoreCancelled) {
            subscribeAbstractResolving(pluginContext, handler, priority, ignoreCancelled);
        }
    }
}
