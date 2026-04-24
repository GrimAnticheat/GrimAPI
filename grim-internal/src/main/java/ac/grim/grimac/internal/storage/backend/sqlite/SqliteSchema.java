package ac.grim.grimac.internal.storage.backend.sqlite;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Initial v1 schema for the SQLite backend. Creates tables + indexes + the meta
 * singleton row on first touch. Idempotent: safe to re-invoke on an initialized DB.
 * <p>
 * Schema evolution is linear and inline — versions are numbered, each {@code applyV<N>}
 * runs in sequence when a pre-N db is opened by post-N code.
 * <p>
 * All table names come from {@link TableNames}; index names are derived from
 * the table name (e.g. {@code idx_<players>_name_lower}) so custom names don't
 * collide when two datastores share a SQLite file.
 */
@ApiStatus.Internal
public final class SqliteSchema {

    public static final int CURRENT_VERSION = 5;

    private SqliteSchema() {}

    public static void ensureInitialized(Connection c, String grimCoreVersion, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE IF NOT EXISTS " + t.meta() + " ("
                    + "id INTEGER PRIMARY KEY CHECK (id = 0), "
                    + "schema_version INTEGER NOT NULL, "
                    + "grim_core_version TEXT, "
                    + "initialized_at INTEGER NOT NULL, "
                    + "last_migration_at INTEGER NOT NULL"
                    + ")");
        }

        int existing = readSchemaVersion(c, t);
        if (existing < 0) {
            applyV1(c, t);
            applyV2(c, t);
            applyV3(c, t);
            applyV4(c, t);
            applyV5(c, t);
            long now = System.currentTimeMillis();
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + t.meta() + "(id, schema_version, grim_core_version, initialized_at, last_migration_at) "
                            + "VALUES (0, ?, ?, ?, ?)")) {
                ps.setInt(1, CURRENT_VERSION);
                ps.setString(2, grimCoreVersion);
                ps.setLong(3, now);
                ps.setLong(4, now);
                ps.executeUpdate();
            }
            return;
        }

        if (existing > CURRENT_VERSION) {
            throw new SQLException("[grim-datastore] SQLite schema is version " + existing
                    + " but this core supports up to " + CURRENT_VERSION
                    + "; refusing to downgrade. Roll Grim forward.");
        }

        if (existing < 2) applyV2(c, t);
        if (existing < 3) applyV3(c, t);
        if (existing < 4) applyV4(c, t);
        if (existing < 5) applyV5(c, t);
        if (existing < CURRENT_VERSION) {
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE " + t.meta() + " SET schema_version=?, last_migration_at=?, grim_core_version=? WHERE id=0")) {
                ps.setInt(1, CURRENT_VERSION);
                ps.setLong(2, System.currentTimeMillis());
                ps.setString(3, grimCoreVersion);
                ps.executeUpdate();
            }
        }
    }

    public static int readSchemaVersion(Connection c, TableNames t) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT schema_version FROM " + t.meta() + " WHERE id = 0");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
            return -1;
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage();
            if (msg.contains("no such table")) return -1;
            throw e;
        }
    }

    private static void applyV1(Connection c, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE " + t.checks() + " ("
                    + "check_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    + "stable_key TEXT NOT NULL UNIQUE, "
                    + "display TEXT, "
                    + "introduced_version TEXT, "
                    + "removed_version TEXT"
                    + ")");

            s.executeUpdate("CREATE TABLE " + t.players() + " ("
                    + "uuid BLOB PRIMARY KEY, "
                    + "current_name TEXT, "
                    + "first_seen INTEGER NOT NULL, "
                    + "last_seen INTEGER NOT NULL"
                    + ")");
            s.executeUpdate("CREATE INDEX idx_" + t.players() + "_name_lower ON " + t.players() + "(lower(current_name))");

            s.executeUpdate("CREATE TABLE " + t.sessions() + " ("
                    + "session_id BLOB PRIMARY KEY, "
                    + "player_uuid BLOB NOT NULL, "
                    + "server_name TEXT, "
                    + "started_at INTEGER NOT NULL, "
                    + "last_activity INTEGER NOT NULL, "
                    + "grim_version TEXT, "
                    + "client_brand TEXT, "
                    // v3 replaces this column with client_version_pvn INTEGER. The v1
                    // shape is kept here only because existing databases we migrate
                    // out of had this column; pristine v3 dbs apply v3 immediately
                    // and the column never appears.
                    + "client_version TEXT, "
                    + "server_version TEXT, "
                    + "replay_clips_json TEXT"
                    + ")");
            s.executeUpdate("CREATE INDEX idx_" + t.sessions() + "_player_started ON " + t.sessions() + "(player_uuid, started_at DESC)");

            s.executeUpdate("CREATE TABLE " + t.violations() + " ("
                    + "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    + "session_id BLOB NOT NULL, "
                    + "player_uuid BLOB NOT NULL, "
                    + "check_id INTEGER NOT NULL, "
                    + "vl REAL NOT NULL, "
                    + "occurred_at INTEGER NOT NULL, "
                    + "verbose TEXT, "
                    + "verbose_format TEXT NOT NULL"
                    + ")");
            s.executeUpdate("CREATE INDEX idx_" + t.violations() + "_session_time ON " + t.violations() + "(session_id, occurred_at)");
            s.executeUpdate("CREATE INDEX idx_" + t.violations() + "_player ON " + t.violations() + "(player_uuid)");

            s.executeUpdate("CREATE TABLE " + t.settings() + " ("
                    + "scope TEXT NOT NULL, "
                    + "scope_key TEXT NOT NULL, "
                    + "key TEXT NOT NULL, "
                    + "value BLOB NOT NULL, "
                    + "updated_at INTEGER NOT NULL, "
                    + "PRIMARY KEY (scope, scope_key, key)"
                    + ")");

            // Internal scratch table for V0 → V1 migration. Dropped by applyV2.
            // Not exposed via TableNames — it's a migration-window artefact.
            s.executeUpdate("CREATE TABLE grim_migration_state ("
                    + "id INTEGER PRIMARY KEY CHECK (id = 0), "
                    + "last_migrated_violation_id INTEGER NOT NULL DEFAULT 0, "
                    + "state TEXT NOT NULL DEFAULT 'PENDING', "
                    + "started_at INTEGER, "
                    + "completed_at INTEGER"
                    + ")");
        }
    }

    /**
     * v2: promote migration state from its dedicated one-row SQLite table into
     * the settings store so backends without native support for that table
     * (future MySQL / Postgres impls) can be migration targets too. Ports any
     * existing row losslessly.
     */
    private static void applyV2(Connection c, TableNames t) throws SQLException {
        // Test for table presence — fresh DBs that ran v1 via this code still
        // have it; upgrades from earlier snapshots also have it.
        boolean hadLegacyTable;
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT name FROM sqlite_master WHERE type='table' AND name='grim_migration_state'")) {
            hadLegacyTable = rs.next();
        }
        if (hadLegacyTable) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT last_migrated_violation_id, state, started_at, completed_at "
                            + "FROM grim_migration_state WHERE id=0");
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long lastId = rs.getLong(1);
                    String state = rs.getString(2);
                    long startedAt = rs.getLong(3);
                    long completedAt = rs.getLong(4);
                    String packed = lastId + "|" + (state == null ? "PENDING" : state)
                            + "|" + startedAt + "|" + completedAt;
                    try (PreparedStatement ins = c.prepareStatement(
                            "INSERT OR REPLACE INTO " + t.settings() + "(scope, scope_key, key, value, updated_at) "
                                    + "VALUES ('SERVER', 'grim-core', 'legacy_v0_migration_state', ?, ?)")) {
                        ins.setBytes(1, packed.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        ins.setLong(2, System.currentTimeMillis());
                        ins.executeUpdate();
                    }
                }
            }
            try (Statement s = c.createStatement()) {
                s.executeUpdate("DROP TABLE grim_migration_state");
            }
        }
    }

    /**
     * v3: replace {@code <sessions>.client_version TEXT} with
     * {@code client_version_pvn INTEGER NOT NULL DEFAULT -1}. The record
     * shape now stores PacketEvents protocol-version numbers instead of
     * release-name strings — display conversion happens at the plugin-side
     * render layer, where PE is on the classpath. Existing string data is
     * discarded (set to -1); the conversion would need a string→PVN lookup
     * that belongs with PE, not in this module.
     * <p>
     * Uses the SQLite table-rewrite pattern: create new table, copy rows
     * from old, drop old, rename new, recreate index. Portable across
     * SQLite versions that don't support {@code ALTER TABLE DROP COLUMN}.
     */
    private static void applyV3(Connection c, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            // If the target shape already exists (fresh v3 db produced by applyV1
            // then applyV3 with no pre-existing v1 artefacts), nothing to do.
            boolean hasPvnColumn = false;
            try (ResultSet rs = s.executeQuery("PRAGMA table_info(" + t.sessions() + ")")) {
                while (rs.next()) {
                    if ("client_version_pvn".equalsIgnoreCase(rs.getString("name"))) {
                        hasPvnColumn = true;
                        break;
                    }
                }
            }
            if (hasPvnColumn) return;

            String scratch = t.sessions() + "_v3";
            s.executeUpdate("DROP INDEX IF EXISTS idx_" + t.sessions() + "_player_started");
            s.executeUpdate("CREATE TABLE " + scratch + " ("
                    + "session_id BLOB PRIMARY KEY, "
                    + "player_uuid BLOB NOT NULL, "
                    + "server_name TEXT, "
                    + "started_at INTEGER NOT NULL, "
                    + "last_activity INTEGER NOT NULL, "
                    + "grim_version TEXT, "
                    + "client_brand TEXT, "
                    + "client_version_pvn INTEGER NOT NULL DEFAULT -1, "
                    + "server_version TEXT, "
                    + "replay_clips_json TEXT"
                    + ")");
            s.executeUpdate("INSERT INTO " + scratch + " "
                    + "(session_id, player_uuid, server_name, started_at, last_activity, "
                    + " grim_version, client_brand, client_version_pvn, server_version, replay_clips_json) "
                    + "SELECT session_id, player_uuid, server_name, started_at, last_activity, "
                    + "       grim_version, client_brand, -1, server_version, replay_clips_json "
                    + "FROM " + t.sessions());
            s.executeUpdate("DROP TABLE " + t.sessions());
            s.executeUpdate("ALTER TABLE " + scratch + " RENAME TO " + t.sessions());
            s.executeUpdate("CREATE INDEX idx_" + t.sessions() + "_player_started ON " + t.sessions() + "(player_uuid, started_at DESC)");
        }
    }

    /**
     * v4: three additions to support the new check-identity contract.
     * <ul>
     *   <li>{@code <checks>.description} — short one-liner for hover
     *       disambiguation, sourced from each Check's declared description.
     *       Nullable; empty / null means the check predates the contract
     *       and the renderer falls back to display-name only.</li>
     *   <li>{@code <checks>.introduced_at} — epoch ms of first intern.
     *       Feeds the collision-prefix template
     *       ({@code V{introduced_major}/}) via grim_version lookup.</li>
     *   <li>{@code <violations>.verbose_format} promoted from TEXT to
     *       INTEGER. New values are {@link ac.grim.grimac.api.storage.model.VerboseFormat#code()}
     *       codes: 0=TEXT, 1=STRUCTURED_V1, with 2 reserved for a future
     *       DEDUP_REF hash-intern scheme. Existing rows migrate by
     *       name→code lookup.</li>
     *   <li>{@code <violations>.metadata} — nullable forward-compat
     *       JSON slot so future event fields (replay clip refs,
     *       client/server tick ids, etc.) don't need a schema bump.</li>
     * </ul>
     */
    private static void applyV4(Connection c, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            // checks: additive columns, no rewrite needed.
            boolean hasDescription = false, hasIntroducedAt = false;
            try (ResultSet rs = s.executeQuery("PRAGMA table_info(" + t.checks() + ")")) {
                while (rs.next()) {
                    String col = rs.getString("name");
                    if ("description".equalsIgnoreCase(col)) hasDescription = true;
                    if ("introduced_at".equalsIgnoreCase(col)) hasIntroducedAt = true;
                }
            }
            if (!hasDescription) {
                s.executeUpdate("ALTER TABLE " + t.checks() + " ADD COLUMN description TEXT");
            }
            if (!hasIntroducedAt) {
                s.executeUpdate("ALTER TABLE " + t.checks() + " ADD COLUMN introduced_at INTEGER");
            }

            // violations: verbose_format TEXT → INTEGER, plus metadata
            // column. Check first — idempotent against re-run.
            boolean formatIsInteger = false, hasMetadata = false;
            try (ResultSet rs = s.executeQuery("PRAGMA table_info(" + t.violations() + ")")) {
                while (rs.next()) {
                    String col = rs.getString("name");
                    String type = rs.getString("type");
                    if ("verbose_format".equalsIgnoreCase(col) && "INTEGER".equalsIgnoreCase(type)) {
                        formatIsInteger = true;
                    }
                    if ("metadata".equalsIgnoreCase(col)) hasMetadata = true;
                }
            }
            if (!hasMetadata) {
                s.executeUpdate("ALTER TABLE " + t.violations() + " ADD COLUMN metadata TEXT");
            }
            if (!formatIsInteger) {
                String scratch = t.violations() + "_v4";
                s.executeUpdate("DROP INDEX IF EXISTS idx_" + t.violations() + "_session_time");
                s.executeUpdate("DROP INDEX IF EXISTS idx_" + t.violations() + "_player");
                s.executeUpdate("CREATE TABLE " + scratch + " ("
                        + "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                        + "session_id BLOB NOT NULL, "
                        + "player_uuid BLOB NOT NULL, "
                        + "check_id INTEGER NOT NULL, "
                        + "vl REAL NOT NULL, "
                        + "occurred_at INTEGER NOT NULL, "
                        + "verbose TEXT, "
                        + "verbose_format INTEGER NOT NULL DEFAULT 0, "
                        + "metadata TEXT"
                        + ")");
                // Map TEXT format names to their new codes. Unknown string
                // values fall back to 0 (TEXT) which matches fromCode's
                // behaviour for forward-compat robustness.
                s.executeUpdate("INSERT INTO " + scratch + " "
                        + "(id, session_id, player_uuid, check_id, vl, occurred_at, verbose, verbose_format, metadata) "
                        + "SELECT id, session_id, player_uuid, check_id, vl, occurred_at, verbose, "
                        + "       CASE verbose_format "
                        + "           WHEN 'TEXT' THEN 0 "
                        + "           WHEN 'STRUCTURED_V1' THEN 1 "
                        + "           ELSE 0 "
                        + "       END, "
                        + "       metadata "
                        + "FROM " + t.violations());
                s.executeUpdate("DROP TABLE " + t.violations());
                s.executeUpdate("ALTER TABLE " + scratch + " RENAME TO " + t.violations());
                s.executeUpdate("CREATE INDEX idx_" + t.violations() + "_session_time ON " + t.violations() + "(session_id, occurred_at)");
                s.executeUpdate("CREATE INDEX idx_" + t.violations() + "_player ON " + t.violations() + "(player_uuid)");
            }
        }
    }

    /**
     * v5: namespace every {@code <checks>.stable_key} under the {@code grim.}
     * family so core keys share one prefix and 3rd-party extensions can claim
     * their own {@code <extension-id>.} prefix without collision.
     * <ul>
     *   <li>{@code legacy:X} → {@code grim.legacy.X} (migration-fallback marker
     *       preserved, just moved under the new namespace).</li>
     *   <li>{@code family.name} → {@code grim.family.name}.</li>
     *   <li>Already-prefixed keys left alone — the update is idempotent.</li>
     * </ul>
     * Aborts loudly if a rename would collide with an existing row; that would
     * only happen on a half-migrated database and wants manual merge rather
     * than silent corruption.
     */
    private static void applyV5(Connection c, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            // Conflict detection: find rows where the post-rename key already
            // exists under a different check_id. Manual merge needed there —
            // re-point <violations>.check_id to the surviving row, then
            // DELETE the duplicate. No automated merge; too much judgment.
            try (ResultSet rs = s.executeQuery(
                    "SELECT a.stable_key AS src, b.stable_key AS dst "
                            + "FROM " + t.checks() + " a JOIN " + t.checks() + " b "
                            + "ON a.check_id <> b.check_id "
                            + "AND b.stable_key = CASE "
                            + "  WHEN a.stable_key LIKE 'legacy:%' THEN 'grim.legacy.' || substr(a.stable_key, 8) "
                            + "  ELSE 'grim.' || a.stable_key "
                            + "END "
                            + "WHERE a.stable_key NOT LIKE 'grim.%'")) {
                if (rs.next()) {
                    throw new SQLException("[grim-datastore] v5 migration aborted: "
                            + "'" + rs.getString("src") + "' would rename to '"
                            + rs.getString("dst") + "' which already exists with a different check_id. "
                            + "Manual merge required before upgrade can proceed.");
                }
            }
            s.executeUpdate(
                    "UPDATE " + t.checks() + " SET stable_key = CASE "
                            + "  WHEN stable_key LIKE 'legacy:%' THEN 'grim.legacy.' || substr(stable_key, 8) "
                            + "  ELSE 'grim.' || stable_key "
                            + "END "
                            + "WHERE stable_key NOT LIKE 'grim.%'");
        }
    }
}
