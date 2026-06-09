package ac.grim.grimac.api.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.events.VerboseSuppliers;
import lombok.Getter;
import org.bukkit.event.Cancellable;
import org.bukkit.event.Event;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

@Deprecated(since = "1.2.1.0", forRemoval = true)
public class FlagEvent extends Event implements GrimUserEvent, Cancellable {

    private static final HandlerList handlers = new HandlerList();
    @Getter private final GrimUser user;
    @Getter private final AbstractCheck check;
    private final Supplier<String> verboseSupplier;
    private boolean cancelled;

    public FlagEvent(GrimUser user, AbstractCheck check, String verbose) {
        this(user, check, VerboseSuppliers.constant(verbose));
    }

    public FlagEvent(GrimUser user, AbstractCheck check, Supplier<String> verboseSupplier) {
        super(true); // Async!
        this.user = user;
        this.check = check;
        this.verboseSupplier = VerboseSuppliers.memoize(verboseSupplier);
    }

    public String getVerbose() {
        return verboseSupplier.get();
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setCancelled(boolean cancel) {
        cancelled = cancel;
    }

    public double getViolations() {
        return check.getViolations();
    }

    @NotNull
    @Override
    public HandlerList getHandlers() {
        return handlers;
    }

    public static HandlerList getHandlerList() {
        return handlers;
    }

    public boolean isSetback() {
        return check.getViolations() > check.getSetbackVL();
    }

}
