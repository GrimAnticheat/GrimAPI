package ac.grim.grimac.api.storage.backend;

import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.api.storage.search.SearchResult;
import ac.grim.grimac.api.storage.search.SearchSpec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;

/**
 * Per-backend search SPI. Composed into {@link KindAdapter#ensureStore} for
 * categories that declare {@code @Searchable} fields; consulted by
 * {@code DataStore.search} for query execution.
 */
@ApiStatus.Experimental
public interface SearchAdapter {

    @NotNull EnumSet<Capability> searchCapabilities();

    /** Create the search index for a store, given its codec shape. Idempotent. */
    void ensureSearchIndex(@NotNull StoreId id, @NotNull EncodeShape shape) throws BackendException;

    void rebuildIndex(@NotNull StoreId id) throws BackendException;

    <R> @NotNull SearchResult<R> execute(@NotNull StoreId id, @NotNull EncodeShape shape,
                                         @NotNull SearchSpec spec) throws BackendException;
}
