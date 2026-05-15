package ac.grim.grimac.api.storage.check;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Durable check-name catalog used to resolve stored violation check ids back to
 * stable keys and display names.
 */
@ApiStatus.Experimental
public interface CheckCatalogPersistence {

    Iterable<CheckCatalogRow> loadAll();

    /**
     * Insert a new row for {@code stableKey} and return its assigned
     * {@code check_id}. Implementations must handle concurrent inserts for the
     * same stable key by returning the existing row id.
     */
    int insert(String stableKey,
               @Nullable String display,
               @Nullable String description,
               @Nullable String introducedVersion,
               long introducedAt);

    /**
     * Copy/import a catalog row while preserving its existing {@code check_id}.
     * Implementations must throw {@link IllegalStateException} if either the
     * row's {@code check_id} or {@code stable_key} is already claimed by a
     * different mapping.
     */
    void upsert(CheckCatalogRow row);

    void updateDisplayAndDescription(int checkId,
                                     @Nullable String display,
                                     @Nullable String description);
}
