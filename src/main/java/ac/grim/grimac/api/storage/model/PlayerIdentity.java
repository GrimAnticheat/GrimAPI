package ac.grim.grimac.api.storage.model;

import ac.grim.grimac.api.storage.codec.Id;
import ac.grim.grimac.api.storage.codec.Indexed;
import ac.grim.grimac.api.storage.codec.MergeMax;
import ac.grim.grimac.api.storage.codec.MergeMin;
import ac.grim.grimac.api.storage.codec.Name;
import ac.grim.grimac.api.storage.codec.Nullable;
import ac.grim.grimac.api.storage.codec.Persistent;
import ac.grim.grimac.api.storage.codec.Value;
import org.jetbrains.annotations.ApiStatus;

import java.util.UUID;

/**
 * Immutable read-side record for one player's identity. One row per UUID.
 * {@code currentName} is the last known display name; carries an
 * {@link Indexed} role so name-prefix lookups work — name-lookup
 * convention is to write the lowercased form to a parallel store at the
 * adapter level (Mongo: companion {@code current_name_lower} index on
 * the same field via {@link ac.grim.grimac.api.storage.kind.IndexSpec}'s
 * {@code caseInsensitivePrefix}).
 */
@ApiStatus.Experimental
@Persistent
public record PlayerIdentity(
        @Id                                          UUID uuid,
        @Indexed @Nullable                           String currentName,
        // Use @Name to publish the short Mongo/SQL column names that
        // the legacy backends already write (first_seen / last_seen),
        // matching the same pattern SessionRecord uses for started_at
        // / last_activity / closed_at. Without @Name the codec auto-
        // snake-cases to first_seen_epoch_ms and v2 reads of legacy
        // rows would miss.
        @Value @MergeMin @Name("first_seen")         long firstSeenEpochMs,
        @Value @MergeMax @Name("last_seen")          long lastSeenEpochMs) {

    public PlayerIdentity {
        if (uuid == null) throw new IllegalArgumentException("uuid");
    }
}
