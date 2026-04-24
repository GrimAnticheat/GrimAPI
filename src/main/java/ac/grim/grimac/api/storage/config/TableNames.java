package ac.grim.grimac.api.storage.config;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Per-backend override for the six logical storage names the DataStore writes to.
 * <p>
 * Lives as a field on each concrete {@link ac.grim.grimac.api.storage.backend.BackendConfig}
 * so an operator can rename any of them (to share a DB with another plugin, to
 * match a corporate naming scheme, to keep two Grim instances side-by-side in
 * one database) without recompiling. V0 hardcoded the single
 * {@code violations} table name; V1 honours whatever is in this record.
 * <p>
 * The interpretation depends on the backend:
 * <ul>
 *   <li><strong>SQL backends</strong> (SQLite, MySQL, Postgres): each name is a
 *       table name. Defaults are {@code grim_meta}, {@code grim_checks},
 *       {@code grim_players}, {@code grim_sessions}, {@code grim_violations},
 *       {@code grim_settings}.</li>
 *   <li><strong>MongoDB</strong>: each name is a collection name within the
 *       backend's configured database. Defaults match the SQL defaults.</li>
 *   <li><strong>Redis</strong>: each name is the key-prefix segment used when
 *       building keys (e.g. {@code grim_violations:<sessionId>}). Operators
 *       who prefer colon-namespaced Redis keys can set values like
 *       {@code grim:violations}; nothing in this record forbids colons.</li>
 * </ul>
 * <p>
 * Changing a name on an already-initialised datastore is <em>not</em>
 * supported; there is no automatic rename. Either drop the old tables /
 * collections / keys first, or run {@code /grim history copy} from a backend
 * configured with the old names into one configured with the new ones.
 *
 * @param meta       Schema-version metadata store (SQL: singleton row; Mongo:
 *                   one document; Redis: one hash). Backends that don't need
 *                   an on-disk schema marker may ignore this.
 * @param checks     Check-identity catalog (stable_key ↔ check_id mapping,
 *                   display name, description, introduced_version,
 *                   introduced_at).
 * @param players    Player-identity records (UUID ↔ last-known name cache).
 * @param sessions   Session records (one per connected play session — joined,
 *                   played, disconnected).
 * @param violations Violation history rows (append-only time series).
 * @param settings   Per-scope key/value settings store (server-scoped and
 *                   player-scoped, used by the migration marker and similar
 *                   plumbing that needs persistent key/value storage).
 */
@ApiStatus.Experimental
public record TableNames(
        @NotNull String meta,
        @NotNull String checks,
        @NotNull String players,
        @NotNull String sessions,
        @NotNull String violations,
        @NotNull String settings) {

    public static final String DEFAULT_META = "grim_meta";
    public static final String DEFAULT_CHECKS = "grim_checks";
    public static final String DEFAULT_PLAYERS = "grim_players";
    public static final String DEFAULT_SESSIONS = "grim_sessions";
    public static final String DEFAULT_VIOLATIONS = "grim_violations";
    public static final String DEFAULT_SETTINGS = "grim_settings";

    /** Shared defaults — all six backends ship with these unless overridden. */
    public static final TableNames DEFAULTS = new TableNames(
            DEFAULT_META,
            DEFAULT_CHECKS,
            DEFAULT_PLAYERS,
            DEFAULT_SESSIONS,
            DEFAULT_VIOLATIONS,
            DEFAULT_SETTINGS);

    public TableNames {
        if (meta == null || meta.isBlank()) meta = DEFAULT_META;
        if (checks == null || checks.isBlank()) checks = DEFAULT_CHECKS;
        if (players == null || players.isBlank()) players = DEFAULT_PLAYERS;
        if (sessions == null || sessions.isBlank()) sessions = DEFAULT_SESSIONS;
        if (violations == null || violations.isBlank()) violations = DEFAULT_VIOLATIONS;
        if (settings == null || settings.isBlank()) settings = DEFAULT_SETTINGS;
    }

    /**
     * Read the six names from a {@link ac.grim.grimac.api.storage.backend.BackendConfigSource}
     * using the canonical key prefix {@code tables.<name>}. Callers wire this
     * from their {@code BackendProvider.readConfig} so every backend parses
     * the same shape without re-typing it.
     */
    public static @NotNull TableNames readFrom(
            @NotNull ac.grim.grimac.api.storage.backend.BackendConfigSource source) {
        return new TableNames(
                source.getString("tables.meta", DEFAULT_META),
                source.getString("tables.checks", DEFAULT_CHECKS),
                source.getString("tables.players", DEFAULT_PLAYERS),
                source.getString("tables.sessions", DEFAULT_SESSIONS),
                source.getString("tables.violations", DEFAULT_VIOLATIONS),
                source.getString("tables.settings", DEFAULT_SETTINGS));
    }
}
