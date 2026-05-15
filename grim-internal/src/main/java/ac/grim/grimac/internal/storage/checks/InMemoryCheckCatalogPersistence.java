package ac.grim.grimac.internal.storage.checks;

import ac.grim.grimac.api.storage.check.CheckCatalogPersistence;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@ApiStatus.Internal
public final class InMemoryCheckCatalogPersistence implements CheckCatalogPersistence {

    private final Map<Integer, CheckCatalogRow> rows = new ConcurrentHashMap<>();
    private final Map<String, Integer> byStableKey = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger(1);

    @Override
    public Iterable<CheckCatalogRow> loadAll() {
        return new ArrayList<>(rows.values());
    }

    @Override
    public synchronized int insert(String stableKey,
                                   @Nullable String display,
                                   @Nullable String description,
                                   @Nullable String introducedVersion,
                                   long introducedAt) {
        Integer existing = byStableKey.get(stableKey);
        if (existing != null) return existing;
        int id = nextId.getAndIncrement();
        rows.put(id, new CheckCatalogRow(id, stableKey, display, description, introducedVersion, introducedAt));
        byStableKey.put(stableKey, id);
        return id;
    }

    @Override
    public synchronized void upsert(CheckCatalogRow row) {
        Integer existingId = byStableKey.get(row.stableKey());
        if (existingId != null && existingId != row.checkId()) {
            throw new IllegalStateException("stable key " + row.stableKey()
                    + " already maps to check_id " + existingId + ", cannot import as " + row.checkId());
        }
        CheckCatalogRow existingRow = rows.get(row.checkId());
        if (existingRow != null && !existingRow.stableKey().equals(row.stableKey())) {
            throw new IllegalStateException("check_id " + row.checkId()
                    + " already maps to stable key " + existingRow.stableKey() + ", cannot import " + row.stableKey());
        }
        rows.put(row.checkId(), row);
        byStableKey.put(row.stableKey(), row.checkId());
        nextId.updateAndGet(current -> Math.max(current, row.checkId() + 1));
    }

    @Override
    public synchronized void updateDisplayAndDescription(int checkId,
                                                         @Nullable String display,
                                                         @Nullable String description) {
        CheckCatalogRow row = rows.get(checkId);
        if (row == null) return;
        rows.put(checkId, new CheckCatalogRow(
                row.checkId(), row.stableKey(), display, description,
                row.introducedVersion(), row.introducedAt()));
    }
}
