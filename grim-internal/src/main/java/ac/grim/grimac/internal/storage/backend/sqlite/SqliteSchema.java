package ac.grim.grimac.internal.storage.backend.sqlite;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Creates tables + indexes + the meta singleton row on first touch. Idempotent:
 * safe to re-invoke on an initialized DB. All table names come from
 * {@link TableNames}; index names are derived from the table name (e.g.
 * {@code idx_<players>_name_lower}) so custom names don't collide when two
 * datastores share a SQLite file.
 * <p>
 * The schema is created in one pass. V0 (the legacy flatfile store) migrates
 * to this shape via {@link ac.grim.grimac.internal.storage.migrate.LegacyMigrator},
 * which writes through {@link ac.grim.grimac.api.storage.backend.Backend#bulkImport}.
 * No intermediate SQLite schema versions are supported.
 * <p>
 * {@code CURRENT_VERSION} is retained as a forward-compat anchor: future
 * additive changes bump the version and gate themselves behind an {@code
 * existing < N} check, same pattern but starting from 1 again.
 */
@ApiStatus.Internal
public final class SqliteSchema {

    public static final int CURRENT_VERSION = 2;

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
            applyCurrent(c, t);
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

        // Forward migrations — additive only; each step is idempotent and
        // gated on its target version.
        if (existing < 2) migrateV1ToV2(c, t);

        if (existing < CURRENT_VERSION) {
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE " + t.meta() + " SET schema_version = ?, last_migration_at = ? WHERE id = 0")) {
                ps.setInt(1, CURRENT_VERSION);
                ps.setLong(2, System.currentTimeMillis());
                ps.executeUpdate();
            }
        }
    }

    /**
     * v1 → v2: add {@code closed_at} to the sessions table. Set when a
     * session ends — gracefully on disconnect, or set on the next startup's
     * crash sweep using {@code last_activity} as the best estimate. Null
     * means "the session is alive on the live tracker".
     */
    private static void migrateV1ToV2(Connection c, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("ALTER TABLE " + t.sessions() + " ADD COLUMN closed_at INTEGER");
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

    private static void applyCurrent(Connection c, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE " + t.checks() + " ("
                    + "check_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    + "stable_key TEXT NOT NULL UNIQUE, "
                    + "display TEXT, "
                    + "description TEXT, "
                    + "introduced_version TEXT, "
                    + "introduced_at INTEGER, "
                    + "removed_version TEXT"
                    + ")");

            // current_name_lower is a materialised lowercase copy of current_name,
            // maintained by every writer. Using a plain-column index instead of
            // an expression index keeps the schema portable across SQLite 3.7+
            // (expression indexes need 3.9+; legacy bundled servers go back
            // further than that).
            s.executeUpdate("CREATE TABLE " + t.players() + " ("
                    + "uuid BLOB PRIMARY KEY, "
                    + "current_name TEXT, "
                    + "current_name_lower TEXT, "
                    + "first_seen INTEGER NOT NULL, "
                    + "last_seen INTEGER NOT NULL"
                    + ")");
            s.executeUpdate("CREATE INDEX idx_" + t.players() + "_name_lower ON " + t.players() + "(current_name_lower)");

            s.executeUpdate("CREATE TABLE " + t.sessions() + " ("
                    + "session_id BLOB PRIMARY KEY, "
                    + "player_uuid BLOB NOT NULL, "
                    + "server_name TEXT, "
                    + "started_at INTEGER NOT NULL, "
                    + "last_activity INTEGER NOT NULL, "
                    + "closed_at INTEGER, "
                    + "grim_version TEXT, "
                    + "client_brand TEXT, "
                    + "client_version_pvn INTEGER NOT NULL DEFAULT -1, "
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
                    + "verbose_format INTEGER NOT NULL DEFAULT 0, "
                    + "metadata TEXT"
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
        }
    }
}
