package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Public, format-agnostic codec handle for a {@link Persistent} record class.
 * Carries the encoded {@link #shape()} (drives index creation in backend
 * adapters), the {@link #version()} (drives migration), and an
 * {@link #indexField} extractor (used by adapters that need indexable values
 * without materializing the full encoded form).
 * <p>
 * Format-specific encode/decode methods (BSON, JDBC, Redis) live on separate
 * SPIs in {@code grim-internal} so this API layer takes no database-driver
 * dependency. Each backend resolves the format codec it needs via the
 * generator installed at startup.
 *
 * @param <R> the immutable record type
 */
@ApiStatus.Experimental
public interface Codec<R> {

    @NotNull Class<R> recordType();

    @NotNull EncodeShape shape();

    int version();

    /**
     * Extract one indexable / partition / timestamp / id field from a record
     * without materializing the full encoded form. Returns the boxed value
     * (or {@code null} for absent nullable fields).
     *
     * @throws IllegalArgumentException if {@code fieldName} is not a declared field
     */
    @Nullable Object indexField(@NotNull R record, @NotNull String fieldName);
}
