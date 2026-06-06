package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.model.VerboseSchemaRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Mutable write-path slot for {@link VerboseSchemaRecord} upserts.
 */
@ApiStatus.Experimental
public final class VerboseSchemaEvent {

    private @Nullable String schemaKey;
    private int flavor;
    private int checkId;
    private int version;
    private byte @Nullable [] layout;
    private long introducedAt;

    public @Nullable String schemaKey() { return schemaKey; }
    public @NotNull VerboseSchemaEvent schemaKey(@NotNull String v) { this.schemaKey = v; return this; }

    public int flavor() { return flavor; }
    public @NotNull VerboseSchemaEvent flavor(int v) { this.flavor = v; return this; }

    public int checkId() { return checkId; }
    public @NotNull VerboseSchemaEvent checkId(int v) { this.checkId = v; return this; }

    public int version() { return version; }
    public @NotNull VerboseSchemaEvent version(int v) {
        if (v < 1) throw new IllegalArgumentException("version");
        this.version = v;
        return this;
    }

    public byte @Nullable [] layout() { return layout; }
    public @NotNull VerboseSchemaEvent layout(byte @NotNull [] v) { this.layout = v; return this; }

    public long introducedAt() { return introducedAt; }
    public @NotNull VerboseSchemaEvent introducedAt(long v) { this.introducedAt = v; return this; }

    public void reset() {
        schemaKey = null;
        flavor = 0;
        checkId = 0;
        version = 0;
        layout = null;
        introducedAt = 0L;
    }
}
