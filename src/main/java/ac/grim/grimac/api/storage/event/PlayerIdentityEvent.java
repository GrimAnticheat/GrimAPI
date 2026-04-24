package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.model.PlayerIdentity;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Mutable write-path slot for the {@code PLAYER_IDENTITY} category. Immutable read
 * counterpart is {@link PlayerIdentity}. Identity upserts merge {@code first_seen}
 * via {@code MIN} and {@code last_seen} via {@code MAX} at the backend level, so
 * producers can publish their current view without reading first.
 */
@ApiStatus.Experimental
public final class PlayerIdentityEvent {

    private UUID uuid;
    private @Nullable String currentName;
    private long firstSeenEpochMs;
    private long lastSeenEpochMs;

    public @NotNull UUID uuid() { return uuid; }
    public @NotNull PlayerIdentityEvent uuid(@NotNull UUID v) { this.uuid = v; return this; }

    public @Nullable String currentName() { return currentName; }
    public @NotNull PlayerIdentityEvent currentName(@Nullable String v) { this.currentName = v; return this; }

    public long firstSeenEpochMs() { return firstSeenEpochMs; }
    public @NotNull PlayerIdentityEvent firstSeenEpochMs(long v) { this.firstSeenEpochMs = v; return this; }

    public long lastSeenEpochMs() { return lastSeenEpochMs; }
    public @NotNull PlayerIdentityEvent lastSeenEpochMs(long v) { this.lastSeenEpochMs = v; return this; }

    public void reset() {
        uuid = null;
        currentName = null;
        firstSeenEpochMs = 0L;
        lastSeenEpochMs = 0L;
    }
}
