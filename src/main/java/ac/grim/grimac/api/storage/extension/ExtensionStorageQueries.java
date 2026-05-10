package ac.grim.grimac.api.storage.extension;

import ac.grim.grimac.api.storage.model.ExtensionStorageRecord;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Backend-friendly query shapes for extension binary categories. The category
 * itself is still passed separately to {@code DataStore.query}; these queries
 * only carry the category-local predicate.
 */
@ApiStatus.Experimental
public final class ExtensionStorageQueries {
    private ExtensionStorageQueries() {}

    public static GetLatest getLatest(String key) {
        return new GetLatest(key);
    }

    public static ListByPrefix listByPrefix(String keyPrefix, int pageSize, @Nullable Cursor cursor) {
        return new ListByPrefix(keyPrefix, pageSize, cursor);
    }

    public static ListCreatedAfter listCreatedAfter(long createdAtEpochMs, int pageSize, @Nullable Cursor cursor) {
        return new ListCreatedAfter(createdAtEpochMs, pageSize, cursor);
    }

    public record GetLatest(String key) implements Query<ExtensionStorageRecord> {}

    public record ListByPrefix(String keyPrefix, int pageSize, @Nullable Cursor cursor)
            implements Query<ExtensionStorageRecord> {}

    public record ListCreatedAfter(long createdAtEpochMs, int pageSize, @Nullable Cursor cursor)
            implements Query<ExtensionStorageRecord> {}
}
