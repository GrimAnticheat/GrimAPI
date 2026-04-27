package ac.grim.grimac.internal.storage.checks;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * In-memory {@code stable_key ↔ check_id} registry backed by a pluggable
 * {@link CheckPersistence}. The current built-in implementations are a
 * SQLite-backed one (for production) and an in-memory stub (for tests and
 * for setups without SQLite in their routing).
 * <p>
 * {@link #intern(String, String, String)} is atomic: if two threads interleave
 * on the same stable_key, one wins and the other sees the winner's check_id
 * via a second load.
 * <p>
 * Collision-prefix: when a new stable_key is inserted and its display name is
 * already in use by an unrelated stable_key, the existing older row's display
 * gets prefixed so the letter is freed for the new check. The prefix is
 * resolved by a {@link #collisionPrefixResolver}, which looks at the older
 * row's introduced_version to produce something like {@code "V2/"}. The
 * plugin wires this up once at startup; tests can supply a static resolver.
 */
@ApiStatus.Internal
public final class CheckRegistry {

    private final CheckPersistence persistence;
    private final Map<String, CheckRow> byStableKey = new ConcurrentHashMap<>();
    private final Map<Integer, CheckRow> byId = new ConcurrentHashMap<>();
    /**
     * Secondary index for collision detection. A display name maps to the
     * CheckRow that currently owns it. Updated in lock-step with the other
     * two maps; collisions are resolved under the intern synchronized lock.
     */
    private final Map<String, CheckRow> byDisplay = new ConcurrentHashMap<>();
    /**
     * Resolves the prefix applied to an older row's display when a new
     * stable_key collides on display. Returns e.g. {@code "V2/"} for a row
     * whose introduced_version is {@code "2.3.61"}, or a static fallback if
     * the row has no recorded introduced_version.
     */
    private final Function<CheckRow, String> collisionPrefixResolver;

    public CheckRegistry(CheckPersistence persistence) {
        this(persistence, DEFAULT_COLLISION_PREFIX);
    }

    public CheckRegistry(CheckPersistence persistence, Function<CheckRow, String> collisionPrefixResolver) {
        this.persistence = persistence;
        this.collisionPrefixResolver = collisionPrefixResolver;
    }

    public void reload() {
        byStableKey.clear();
        byId.clear();
        byDisplay.clear();
        for (CheckRow row : persistence.loadAll()) {
            byStableKey.put(row.stableKey(), row);
            byId.put(row.checkId(), row);
            if (row.display() != null) {
                byDisplay.put(row.display(), row);
            }
        }
    }

    /**
     * Get or insert a (stable_key, display, description) row.
     * <ul>
     *   <li>Same stable_key as an existing row: display and description get
     *       updated to the caller's values if they differ (auto-unify across
     *       cross-letter renames).</li>
     *   <li>New stable_key whose display is already owned by a different
     *       row: the existing row's display gets prefixed (e.g. to
     *       {@code "V2/BadPacketsB"}) so the letter is freed; the new row
     *       lands with the clean display.</li>
     *   <li>New stable_key with a free display: clean insert.</li>
     * </ul>
     *
     * @param introducedVersion plugin version at time of first intern;
     *                          persisted on the row for future collision-prefix
     *                          resolution. Pass {@code null} on reloads /
     *                          lookups that don't know.
     *
     * TODO: explore a lock-free rewrite. Held by `synchronized` because the
     * body is a multi-map RMW (byStableKey + byId + byDisplay) plus a DB
     * write inside the collision-prefix path; making it lock-free needs an
     * outer retry wrapping a multi-step write with rollback semantics on
     * the DB side. Cold path in practice — caller's local cache absorbs the
     * hot path — so the lock cost is amortized to ~zero, but worth doing if
     * we ever need this on a hot path.
     */
    public synchronized int intern(String stableKey,
                                   @Nullable String display,
                                   @Nullable String description,
                                   @Nullable String introducedVersion) {
        CheckRow existing = byStableKey.get(stableKey);
        if (existing != null) {
            boolean displayChanged = display != null && !display.equals(existing.display());
            boolean descriptionChanged = description != null && !description.equals(existing.description());
            if (displayChanged || descriptionChanged) {
                String newDisplay = displayChanged ? display : existing.display();
                String newDescription = descriptionChanged ? description : existing.description();
                persistence.updateDisplayAndDescription(existing.checkId(), newDisplay, newDescription);
                CheckRow renamed = new CheckRow(
                        existing.checkId(), existing.stableKey(),
                        newDisplay, newDescription, existing.introducedVersion(), existing.introducedAt());
                byStableKey.put(stableKey, renamed);
                byId.put(existing.checkId(), renamed);
                if (existing.display() != null) byDisplay.remove(existing.display(), existing);
                if (newDisplay != null) byDisplay.put(newDisplay, renamed);
            }
            return existing.checkId();
        }

        // New stable_key. Check for display collision before inserting.
        if (display != null) {
            CheckRow collider = byDisplay.get(display);
            if (collider != null) {
                // A different stable_key already owns this display. Prefix the
                // older row so the letter is free. The prefix reflects when
                // the older row was introduced — e.g. V2-era rows become
                // "V2/BadPacketsB" when V3 introduces a different meaning at
                // the same letter.
                String prefix = collisionPrefixResolver.apply(collider);
                String prefixed = prefix + collider.display();
                persistence.updateDisplayAndDescription(collider.checkId(), prefixed, collider.description());
                CheckRow updated = new CheckRow(
                        collider.checkId(), collider.stableKey(),
                        prefixed, collider.description(),
                        collider.introducedVersion(), collider.introducedAt());
                byStableKey.put(collider.stableKey(), updated);
                byId.put(collider.checkId(), updated);
                byDisplay.remove(display, collider);
                byDisplay.put(prefixed, updated);
            }
        }

        long now = System.currentTimeMillis();
        int id = persistence.insert(stableKey, display, description, introducedVersion, now);
        CheckRow row = new CheckRow(id, stableKey, display, description, introducedVersion, now);
        byStableKey.put(stableKey, row);
        byId.put(id, row);
        if (display != null) byDisplay.put(display, row);
        return id;
    }

    /**
     * Back-compat entry point for callers that don't yet pass description
     * and introduced_version. Forwards to the full form with nulls.
     */
    public int intern(String stableKey, @Nullable String display) {
        return intern(stableKey, display, null, null);
    }

    public Optional<Integer> getId(String stableKey) {
        CheckRow r = byStableKey.get(stableKey);
        return r == null ? Optional.empty() : Optional.of(r.checkId());
    }

    public Optional<String> displayFor(int checkId) {
        CheckRow r = byId.get(checkId);
        return r == null ? Optional.empty() : Optional.ofNullable(r.display());
    }

    public Optional<String> descriptionFor(int checkId) {
        CheckRow r = byId.get(checkId);
        return r == null ? Optional.empty() : Optional.ofNullable(r.description());
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
        int insert(String stableKey,
                   @Nullable String display,
                   @Nullable String description,
                   @Nullable String introducedVersion,
                   long introducedAt);

        /** Single-row update covering both display and description. */
        void updateDisplayAndDescription(int checkId,
                                        @Nullable String display,
                                        @Nullable String description);
    }

    public record CheckRow(
            int checkId,
            String stableKey,
            @Nullable String display,
            @Nullable String description,
            @Nullable String introducedVersion,
            long introducedAt) {
    }

    /**
     * Default collision-prefix resolver: reads the major-version component
     * of {@code introducedVersion} and returns {@code "V<major>/"}. Falls
     * back to {@code "legacy/"} when the version is missing or unparseable.
     */
    public static final Function<CheckRow, String> DEFAULT_COLLISION_PREFIX = row -> {
        String v = row.introducedVersion();
        if (v == null || v.isBlank()) return "legacy/";
        String trimmed = v.trim();
        int dot = trimmed.indexOf('.');
        String majorPart = dot < 0 ? trimmed : trimmed.substring(0, dot);
        // Strip any leading non-digit prefix (e.g. "v2.3" → "2", "Grim-3.0" → "3").
        int i = 0;
        while (i < majorPart.length() && !Character.isDigit(majorPart.charAt(i))) i++;
        int j = i;
        while (j < majorPart.length() && Character.isDigit(majorPart.charAt(j))) j++;
        if (i == j) return "legacy/";
        return "V" + majorPart.substring(i, j).toUpperCase(Locale.ROOT) + "/";
    };

    /**
     * Fixed-string collision-prefix resolver for tests and for operators
     * who want a version-independent marker like {@code "(legacy) "}.
     */
    public static Function<CheckRow, String> staticPrefix(String prefix) {
        return row -> prefix;
    }
}
