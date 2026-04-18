package ac.grim.grimac.internal.storage.checks;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory stable_key ↔ check_id registry backed by a pluggable
 * {@link CheckPersistence} (SQLite in phase 1, in-memory for tests).
 * <p>
 * {@link #intern(String, String)} is atomic: if two threads interleave on the same
 * stable_key, one wins, the other sees the winner's check_id via a second load.
 */
@ApiStatus.Internal
public final class CheckRegistry {

    private final CheckPersistence persistence;
    private final Map<String, CheckRow> byStableKey = new ConcurrentHashMap<>();
    private final Map<Integer, CheckRow> byId = new ConcurrentHashMap<>();

    public CheckRegistry(CheckPersistence persistence) {
        this.persistence = persistence;
    }

    public void reload() {
        byStableKey.clear();
        byId.clear();
        for (CheckRow row : persistence.loadAll()) {
            byStableKey.put(row.stableKey(), row);
            byId.put(row.checkId(), row);
        }
    }

    /**
     * Get or insert a (stable_key, display) row. If a row exists and its display
     * differs, updates the stored display to the caller's value (renames take effect).
     */
    public synchronized int intern(String stableKey, @Nullable String display) {
        CheckRow existing = byStableKey.get(stableKey);
        if (existing != null) {
            if (display != null && !display.equals(existing.display())) {
                persistence.updateDisplay(existing.checkId(), display);
                CheckRow renamed = new CheckRow(existing.checkId(), existing.stableKey(), display);
                byStableKey.put(stableKey, renamed);
                byId.put(existing.checkId(), renamed);
            }
            return existing.checkId();
        }
        int id = persistence.insert(stableKey, display);
        CheckRow row = new CheckRow(id, stableKey, display);
        byStableKey.put(stableKey, row);
        byId.put(id, row);
        return id;
    }

    public Optional<Integer> getId(String stableKey) {
        CheckRow r = byStableKey.get(stableKey);
        return r == null ? Optional.empty() : Optional.of(r.checkId());
    }

    public Optional<String> displayFor(int checkId) {
        CheckRow r = byId.get(checkId);
        return r == null ? Optional.empty() : Optional.ofNullable(r.display());
    }

    public Optional<String> stableKeyFor(int checkId) {
        CheckRow r = byId.get(checkId);
        return r == null ? Optional.empty() : Optional.of(r.stableKey());
    }

    public int size() {
        return byStableKey.size();
    }

    public interface CheckPersistence {

        Iterable<CheckRow> loadAll();

        /** Insert a new row, return the assigned check_id. */
        int insert(String stableKey, @Nullable String display);

        void updateDisplay(int checkId, @Nullable String display);
    }

    public record CheckRow(int checkId, String stableKey, @Nullable String display) {}
}
