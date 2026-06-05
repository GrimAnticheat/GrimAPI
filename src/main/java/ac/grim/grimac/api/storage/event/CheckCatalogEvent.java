package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.model.CheckCatalogRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Mutable write-path slot for the {@code CHECK} category. Check-catalog
 * writes are idempotent upserts keyed on {@code stableKey}; producers
 * usually invoke them at plugin load to declare each check exists
 * (insert-on-first-load, no-op on subsequent loads since the row is
 * already there). Immutable read counterpart is
 * {@link CheckCatalogRecord}.
 * <p>
 * {@code checkId} is the surrogate integer key kept around for legacy
 * compatibility — produces and renderers reference checks by stable key,
 * not by int id, in v2. The int id is allocated once at first insert
 * (the consumer maintains a counter elsewhere); subsequent upserts of
 * the same stableKey should keep the same checkId.
 */
@ApiStatus.Experimental
public final class CheckCatalogEvent {

    private @Nullable String stableKey;
    private int checkId;
    private @Nullable String display;
    private @Nullable String description;
    private @Nullable String introducedVersion;
    private long introducedAt;

    public @Nullable String stableKey() { return stableKey; }
    public @NotNull CheckCatalogEvent stableKey(@NotNull String v) { this.stableKey = v; return this; }

    public int checkId() { return checkId; }
    public @NotNull CheckCatalogEvent checkId(int v) { this.checkId = v; return this; }

    public @Nullable String display() { return display; }
    public @NotNull CheckCatalogEvent display(@Nullable String v) { this.display = v; return this; }

    public @Nullable String description() { return description; }
    public @NotNull CheckCatalogEvent description(@Nullable String v) { this.description = v; return this; }

    public @Nullable String introducedVersion() { return introducedVersion; }
    public @NotNull CheckCatalogEvent introducedVersion(@Nullable String v) { this.introducedVersion = v; return this; }

    public long introducedAt() { return introducedAt; }
    public @NotNull CheckCatalogEvent introducedAt(long v) { this.introducedAt = v; return this; }

    /** Reset the slot for ring-buffer reuse. Mirrors {@link SessionEvent#reset()}'s contract. */
    public void reset() {
        stableKey = null;
        checkId = 0;
        display = null;
        description = null;
        introducedVersion = null;
        introducedAt = 0L;
    }
}
