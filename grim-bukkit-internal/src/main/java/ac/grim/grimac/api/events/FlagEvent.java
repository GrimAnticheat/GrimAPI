package ac.grim.grimac.api.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
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
        this(user, check, constant(verbose));
    }

    public FlagEvent(GrimUser user, AbstractCheck check, Supplier<String> verboseSupplier) {
        super(true); // Async!
        this.user = user;
        this.check = check;
        this.verboseSupplier = memoize(verboseSupplier == null ? () -> "" : verboseSupplier);
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

    private static @NotNull Supplier<String> constant(String verbose) {
        String value = verbose == null ? "" : verbose;
        return () -> value;
    }

    private static @NotNull Supplier<String> memoize(@NotNull Supplier<String> supplier) {
        return new Supplier<>() {
            private String value;
            private boolean computed;

            @Override
            public String get() {
                if (!computed) {
                    try {
                        value = supplier.get();
                        if (value == null) value = "";
                    } catch (Throwable ignored) {
                        value = "";
                    }
                    computed = true;
                }
                return value;
            }
        };
    }

}
