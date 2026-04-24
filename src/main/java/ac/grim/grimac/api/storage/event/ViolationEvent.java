package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Mutable write-path slot for the {@code VIOLATION} category. Instances are
 * pre-allocated inside the Disruptor ring and recycled across publishes; producers
 * get a slot, populate fields, and publish. Never retain references past publish.
 * <p>
 * The immutable read-side counterpart is {@link ViolationRecord}, which backends
 * materialise on read from their native storage — never from an event.
 */
@ApiStatus.Experimental
public final class ViolationEvent {

    private UUID sessionId;
    private UUID playerUuid;
    private int checkId;
    private double vl;
    private long occurredEpochMs;
    private @Nullable String verbose;
    private VerboseFormat verboseFormat = VerboseFormat.TEXT;

    public @NotNull UUID sessionId() { return sessionId; }
    public @NotNull ViolationEvent sessionId(@NotNull UUID v) { this.sessionId = v; return this; }

    public @NotNull UUID playerUuid() { return playerUuid; }
    public @NotNull ViolationEvent playerUuid(@NotNull UUID v) { this.playerUuid = v; return this; }

    public int checkId() { return checkId; }
    public @NotNull ViolationEvent checkId(int v) { this.checkId = v; return this; }

    public double vl() { return vl; }
    public @NotNull ViolationEvent vl(double v) { this.vl = v; return this; }

    public long occurredEpochMs() { return occurredEpochMs; }
    public @NotNull ViolationEvent occurredEpochMs(long v) { this.occurredEpochMs = v; return this; }

    public @Nullable String verbose() { return verbose; }
    public @NotNull ViolationEvent verbose(@Nullable String v) { this.verbose = v; return this; }

    public @NotNull VerboseFormat verboseFormat() { return verboseFormat; }
    public @NotNull ViolationEvent verboseFormat(@NotNull VerboseFormat v) { this.verboseFormat = v; return this; }

    /**
     * Reset to neutral state so the Disruptor can hand the slot to the next producer
     * without leaking fields from the previous publish.
     */
    public void reset() {
        sessionId = null;
        playerUuid = null;
        checkId = 0;
        vl = 0.0;
        occurredEpochMs = 0L;
        verbose = null;
        verboseFormat = VerboseFormat.TEXT;
    }
}
