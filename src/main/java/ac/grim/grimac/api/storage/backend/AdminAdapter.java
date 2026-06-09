package ac.grim.grimac.api.storage.backend;

import ac.grim.grimac.api.storage.admin.CollectionStats;
import ac.grim.grimac.api.storage.admin.ExplainPlan;
import ac.grim.grimac.api.storage.admin.IndexStats;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.api.storage.search.SearchSpec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

/**
 * Per-backend admin SPI. Synchronous; called from the reader pool by the
 * {@code AdminAccess} facade.
 */
@ApiStatus.Experimental
public interface AdminAdapter {

    @NotNull ExplainPlan explain(@NotNull StoreId id, @NotNull Operation<?> op) throws BackendException;

    @NotNull ExplainPlan explainSearch(@NotNull StoreId id, @NotNull SearchSpec spec) throws BackendException;

    void reindex(@NotNull StoreId id) throws BackendException;

    void rebuildSearchIndex(@NotNull StoreId id) throws BackendException;

    void setRetention(@NotNull StoreId id, @NotNull Duration retention) throws BackendException;

    @NotNull IndexStats indexStats(@NotNull StoreId id) throws BackendException;

    @NotNull CollectionStats collectionStats(@NotNull StoreId id) throws BackendException;
}
