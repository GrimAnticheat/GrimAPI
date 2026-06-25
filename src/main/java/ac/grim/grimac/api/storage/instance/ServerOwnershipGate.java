package ac.grim.grimac.api.storage.instance;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Local guard for persistent writes. The authoritative state lives in the
 * backend ownership row; this gate prevents stale local writers from accepting
 * new persistence work after ownership is lost or the local lease deadline has
 * passed.
 */
@ApiStatus.Experimental
public final class ServerOwnershipGate {

    private final boolean enforced;
    private volatile boolean open;
    private volatile long deadlineNanos;
    private volatile @Nullable UUID startupId;
    private volatile @Nullable UUID fence;
    private volatile @NotNull String closeReason = "not-open";

    public ServerOwnershipGate(boolean enforced) {
        this.enforced = enforced;
        this.open = !enforced;
        this.deadlineNanos = Long.MAX_VALUE;
    }

    public static @NotNull ServerOwnershipGate disabled() {
        return new ServerOwnershipGate(false);
    }

    public boolean enforced() {
        return enforced;
    }

    public void open(@NotNull UUID startupId, @NotNull UUID fence, long ttlMs, long safetyMarginMs) {
        if (!enforced) return;
        this.startupId = startupId;
        this.fence = fence;
        this.deadlineNanos = deadlineFromNow(ttlMs, safetyMarginMs);
        this.closeReason = "open";
        this.open = true;
    }

    public void extend(@NotNull UUID startupId, @NotNull UUID fence, long ttlMs, long safetyMarginMs) {
        if (!enforced) return;
        if (!startupId.equals(this.startupId) || !fence.equals(this.fence)) {
            close("ownership-token-mismatch");
            return;
        }
        this.deadlineNanos = deadlineFromNow(ttlMs, safetyMarginMs);
        this.closeReason = "open";
        this.open = true;
    }

    public void close(@NotNull String reason) {
        if (!enforced) return;
        this.open = false;
        this.deadlineNanos = 0L;
        this.closeReason = reason;
    }

    public boolean allowWrites() {
        if (!enforced) return true;
        if (!open) return false;
        if (System.nanoTime() <= deadlineNanos) return true;
        close("local-lease-deadline-expired");
        return false;
    }

    public @NotNull String closeReason() {
        return closeReason;
    }

    private static long deadlineFromNow(long ttlMs, long safetyMarginMs) {
        long usableMs = Math.max(1L, ttlMs - Math.max(0L, safetyMarginMs));
        long now = System.nanoTime();
        long delta;
        try {
            delta = Math.multiplyExact(usableMs, 1_000_000L);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
        long deadline = now + delta;
        return deadline < 0L ? Long.MAX_VALUE : deadline;
    }
}
