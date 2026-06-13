package ac.grim.grimac.internal.storage.checks;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.check.CheckCatalogPersistence;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Check-catalog persistence backed by the normal datastore write path.
 * <p>
 * The existing catalog is loaded before construction. Subsequent inserts and
 * metadata updates update the local view synchronously, then enqueue an
 * idempotent {@link Categories#CHECK_CATALOG} write. This keeps lazy check /
 * verbose registration out of backend-specific JDBC writers and on the same
 * routed storage plane as other v2 metadata.
 */
@ApiStatus.Internal
public final class DataStoreCheckCatalogPersistence implements CheckCatalogPersistence {

    private final @NotNull DataStore store;
    private final @NotNull Logger logger;
    private final Map<String, CheckCatalogRow> byStableKey = new LinkedHashMap<>();
    private final Map<Integer, CheckCatalogRow> byId = new LinkedHashMap<>();
    private int nextCheckId = 1;

    public DataStoreCheckCatalogPersistence(
            @NotNull Iterable<CheckCatalogRow> initialRows,
            @NotNull DataStore store,
            @NotNull Logger logger) {
        this.store = store;
        this.logger = logger;
        for (CheckCatalogRow row : initialRows) {
            putLocal(row);
            nextCheckId = Math.max(nextCheckId, row.checkId() + 1);
        }
    }

    @Override
    public synchronized Iterable<CheckCatalogRow> loadAll() {
        return List.copyOf(byStableKey.values());
    }

    @Override
    public synchronized int insert(
            String stableKey,
            @Nullable String display,
            @Nullable String description,
            @Nullable String introducedVersion,
            long introducedAt) {
        CheckCatalogRow existing = byStableKey.get(stableKey);
        if (existing != null) return existing.checkId();

        int checkId = nextAvailableId();
        CheckCatalogRow row = new CheckCatalogRow(
                checkId, stableKey, display, description, introducedVersion, introducedAt);
        putLocal(row);
        enqueue(row);
        return checkId;
    }

    @Override
    public synchronized void upsert(CheckCatalogRow row) {
        CheckCatalogRow existingByKey = byStableKey.get(row.stableKey());
        if (existingByKey != null && existingByKey.checkId() != row.checkId()) {
            throw new IllegalStateException("stable key " + row.stableKey()
                    + " already maps to check_id " + existingByKey.checkId());
        }
        CheckCatalogRow existingById = byId.get(row.checkId());
        if (existingById != null && !existingById.stableKey().equals(row.stableKey())) {
            throw new IllegalStateException("check_id " + row.checkId()
                    + " already maps to stable key " + existingById.stableKey());
        }
        putLocal(row);
        enqueue(row);
    }

    @Override
    public synchronized void updateDisplayAndDescription(
            int checkId,
            @Nullable String display,
            @Nullable String description) {
        CheckCatalogRow existing = byId.get(checkId);
        if (existing == null) {
            logger.warning("[grim-datastore] attempted check-catalog metadata update for unknown check_id "
                    + checkId);
            return;
        }
        CheckCatalogRow updated = new CheckCatalogRow(
                existing.checkId(),
                existing.stableKey(),
                display,
                description,
                existing.introducedVersion(),
                existing.introducedAt());
        putLocal(updated);
        enqueue(updated);
    }

    private int nextAvailableId() {
        while (byId.containsKey(nextCheckId)) nextCheckId++;
        return nextCheckId++;
    }

    private void putLocal(@NotNull CheckCatalogRow row) {
        byStableKey.put(row.stableKey(), row);
        byId.put(row.checkId(), row);
    }

    private void enqueue(@NotNull CheckCatalogRow row) {
        try {
            store.submit(Categories.CHECK_CATALOG, event -> event
                    .stableKey(row.stableKey())
                    .checkId(row.checkId())
                    .display(row.display())
                    .description(row.description())
                    .introducedVersion(row.introducedVersion())
                    .introducedAt(row.introducedAt()));
        } catch (RuntimeException e) {
            logger.log(Level.WARNING,
                    "[grim-datastore] failed to enqueue check-catalog row for " + row.stableKey(), e);
        }
    }
}
