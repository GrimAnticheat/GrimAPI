package ac.grim.grimac.internal.storage.backend.sqlite;

import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Initial v1 schema for the SQLite backend. Creates tables + indexes + the grim_meta
 * singleton row on first touch. Idempotent: safe to re-invoke on an initialized DB.
 * <p>
 * Future v2+ migrations live in sibling classes and are dispatched by schema_version.
 */
@ApiStatus.Internal
public final class SqliteSchema {

    public static final int CURRENT_VERSION = 1;

    private SqliteSchema() {}

    public static void ensureInitialized(Connection c, String grimCoreVersion) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE IF NOT EXISTS grim_meta ("
                    + "id INTEGER PRIMARY KEY CHECK (id = 0), "
                    + "schema_version INTEGER NOT NULL, "
                    + "grim_core_version TEXT, "
                    + "initialized_at INTEGER NOT NULL, "
                    + "last_migration_at INTEGER NOT NULL"
                    + ")");
        }

        int existing = readSchemaVersion(c);
        if (existing < 0) {
            applyV1(c);
            long now = System.currentTimeMillis();
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO grim_meta(id, schema_version, grim_core_version, initialized_at, last_migration_at) "
                            + "VALUES (0, ?, ?, ?, ?)")) {
                ps.setInt(1, CURRENT_VERSION);
                ps.setString(2, grimCoreVersion);
                ps.setLong(3, now);
                ps.setLong(4, now);
                ps.executeUpdate();
            }
        } else if (existing > CURRENT_VERSION) {
            throw new SQLException("[grim-datastore] SQLite schema is version " + existing
                    + " but this core supports up to " + CURRENT_VERSION
                    + "; refusing to downgrade. Roll Grim forward.");
        } else if (existing < CURRENT_VERSION) {
            // phase 1: no mid-version migrations. First write that bumps the version
            // beyond 1 adds them here.
            throw new SQLException("[grim-datastore] SQLite schema is version " + existing
                    + " which needs migration to " + CURRENT_VERSION
                    + "; phase 1 ships only version 1.");
        }
    }

    public static int readSchemaVersion(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT schema_version FROM grim_meta WHERE id = 0");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
            return -1;
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage();
            if (msg.contains("no such table")) return -1;
            throw e;
        }
    }

    private static void applyV1(Connection c) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE grim_checks ("
                    + "check_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    + "stable_key TEXT NOT NULL UNIQUE, "
                    + "display TEXT, "
                    + "introduced_version TEXT, "
                    + "removed_version TEXT"
                    + ")");

            s.executeUpdate("CREATE TABLE grim_players ("
                    + "uuid BLOB PRIMARY KEY, "
                    + "current_name TEXT, "
                    + "first_seen INTEGER NOT NULL, "
                    + "last_seen INTEGER NOT NULL"
                    + ")");
            s.executeUpdate("CREATE INDEX idx_players_name_lower ON grim_players(lower(current_name))");

            s.executeUpdate("CREATE TABLE grim_sessions ("
                    + "session_id BLOB PRIMARY KEY, "
                    + "player_uuid BLOB NOT NULL, "
                    + "server_name TEXT, "
                    + "started_at INTEGER NOT NULL, "
                    + "last_activity INTEGER NOT NULL, "
                    + "grim_version TEXT, "
                    + "client_brand TEXT, "
                    + "client_version TEXT, "
                    + "server_version TEXT, "
                    + "replay_clips_json TEXT"
                    + ")");
            s.executeUpdate("CREATE INDEX idx_sessions_player_started ON grim_sessions(player_uuid, started_at DESC)");

            s.executeUpdate("CREATE TABLE grim_violations ("
                    + "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    + "session_id BLOB NOT NULL, "
                    + "player_uuid BLOB NOT NULL, "
                    + "check_id INTEGER NOT NULL, "
                    + "vl REAL NOT NULL, "
                    + "occurred_at INTEGER NOT NULL, "
                    + "verbose TEXT, "
                    + "verbose_format TEXT NOT NULL"
                    + ")");
            s.executeUpdate("CREATE INDEX idx_violations_session_time ON grim_violations(session_id, occurred_at)");
            s.executeUpdate("CREATE INDEX idx_violations_player ON grim_violations(player_uuid)");

            s.executeUpdate("CREATE TABLE grim_settings ("
                    + "scope TEXT NOT NULL, "
                    + "scope_key TEXT NOT NULL, "
                    + "key TEXT NOT NULL, "
                    + "value BLOB NOT NULL, "
                    + "updated_at INTEGER NOT NULL, "
                    + "PRIMARY KEY (scope, scope_key, key)"
                    + ")");

            s.executeUpdate("CREATE TABLE grim_migration_state ("
                    + "id INTEGER PRIMARY KEY CHECK (id = 0), "
                    + "last_migrated_violation_id INTEGER NOT NULL DEFAULT 0, "
                    + "state TEXT NOT NULL DEFAULT 'PENDING', "
                    + "started_at INTEGER, "
                    + "completed_at INTEGER"
                    + ")");
        }
    }
}
