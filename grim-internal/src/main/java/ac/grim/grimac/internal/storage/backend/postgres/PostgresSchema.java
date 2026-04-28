package ac.grim.grimac.internal.storage.backend.postgres;

import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.checks.LegacyKeyRenames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Postgres 14+ schema for the Postgres backend. Born at the v5 baseline — see
 * {@code MysqlSchema} for the same rationale; there are no pre-v5 Postgres
 * databases to migrate from.
 */
@ApiStatus.Internal
public final class PostgresSchema {

    public static final int CURRENT_VERSION = 7;

    private PostgresSchema() {}

    public static void ensureInitialized(Connection c, String grimCoreVersion, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE IF NOT EXISTS " + quoteId(t.meta()) + " ("
                    + "id SMALLINT PRIMARY KEY CHECK (id = 0), "
                    + "schema_version INTEGER NOT NULL, "
                    + "grim_core_version TEXT, "
                    + "initialized_at BIGINT NOT NULL, "
                    + "last_migration_at BIGINT NOT NULL"
                    + ")");
        }

        int existing = readSchemaVersion(c, t);
        if (existing < 0) {
            applyBaseline(c, t);
            long now = System.currentTimeMillis();
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + quoteId(t.meta()) + "(id, schema_version, grim_core_version, initialized_at, last_migration_at) "
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
            throw new SQLException("[grim-datastore] Postgres schema is version " + existing
                    + " but this core supports up to " + CURRENT_VERSION
                    + "; refusing to downgrade. Roll Grim forward.");
        }

        if (existing < 6) migrateV5ToV6(c, t);
        if (existing < 7) migrateV6ToV7(c, t);

        if (existing < CURRENT_VERSION) {
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE " + quoteId(t.meta()) + " SET schema_version = ?, last_migration_at = ? WHERE id = 0")) {
                ps.setInt(1, CURRENT_VERSION);
                ps.setLong(2, System.currentTimeMillis());
                ps.executeUpdate();
            }
        }
    }

    /** v5 → v6: add {@code closed_at} to the sessions table. */
    private static void migrateV5ToV6(Connection c, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("ALTER TABLE " + quoteId(t.sessions()) + " ADD COLUMN closed_at BIGINT");
        }
    }

    /** v6 → v7: rewrite {@code grim.legacy.*} stable_keys onto descriptive ones. See {@link LegacyKeyRenames}. */
    private static void migrateV6ToV7(Connection c, TableNames t) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE " + quoteId(t.checks()) + " SET stable_key = ? WHERE stable_key = ?")) {
            for (Map.Entry<String, String> e : LegacyKeyRenames.OLD_TO_NEW.entrySet()) {
                ps.setString(1, e.getValue());
                ps.setString(2, e.getKey());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public static int readSchemaVersion(Connection c, TableNames t) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT schema_version FROM " + quoteId(t.meta()) + " WHERE id = 0");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
            return -1;
        } catch (SQLException e) {
            // Postgres SQLSTATE 42P01 = undefined_table.
            if ("42P01".equals(e.getSQLState())) return -1;
            throw e;
        }
    }

    private static void applyBaseline(Connection c, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE " + quoteId(t.checks()) + " ("
                    + "check_id SERIAL PRIMARY KEY, "
                    + "stable_key TEXT NOT NULL UNIQUE, "
                    + "display TEXT, "
                    + "description TEXT, "
                    + "introduced_version TEXT, "
                    + "removed_version TEXT, "
                    + "introduced_at BIGINT"
                    + ")");

            s.executeUpdate("CREATE TABLE " + quoteId(t.players()) + " ("
                    + "uuid BYTEA PRIMARY KEY, "
                    + "current_name TEXT, "
                    + "first_seen BIGINT NOT NULL, "
                    + "last_seen BIGINT NOT NULL"
                    + ")");
            s.executeUpdate("CREATE INDEX " + quoteId("idx_" + t.players() + "_name_lower")
                    + " ON " + quoteId(t.players()) + " (lower(current_name))");

            s.executeUpdate("CREATE TABLE " + quoteId(t.sessions()) + " ("
                    + "session_id BYTEA PRIMARY KEY, "
                    + "player_uuid BYTEA NOT NULL, "
                    + "server_name TEXT, "
                    + "started_at BIGINT NOT NULL, "
                    + "last_activity BIGINT NOT NULL, "
                    + "closed_at BIGINT, "
                    + "grim_version TEXT, "
                    + "client_brand TEXT, "
                    + "client_version_pvn INTEGER NOT NULL DEFAULT -1, "
                    + "server_version TEXT, "
                    + "replay_clips_json TEXT"
                    + ")");
            s.executeUpdate("CREATE INDEX " + quoteId("idx_" + t.sessions() + "_player_started")
                    + " ON " + quoteId(t.sessions()) + " (player_uuid, started_at DESC)");

            s.executeUpdate("CREATE TABLE " + quoteId(t.violations()) + " ("
                    + "id BIGSERIAL PRIMARY KEY, "
                    + "session_id BYTEA NOT NULL, "
                    + "player_uuid BYTEA NOT NULL, "
                    + "check_id INTEGER NOT NULL, "
                    + "vl DOUBLE PRECISION NOT NULL, "
                    + "occurred_at BIGINT NOT NULL, "
                    // "verbose" is a reserved keyword in Postgres (context-dependent
                    // with EXPLAIN ... VERBOSE); quote it as an identifier.
                    + "\"verbose\" TEXT, "
                    + "verbose_format INTEGER NOT NULL DEFAULT 0, "
                    + "metadata TEXT"
                    + ")");
            s.executeUpdate("CREATE INDEX " + quoteId("idx_" + t.violations() + "_session_time")
                    + " ON " + quoteId(t.violations()) + " (session_id, occurred_at)");
            s.executeUpdate("CREATE INDEX " + quoteId("idx_" + t.violations() + "_player")
                    + " ON " + quoteId(t.violations()) + " (player_uuid)");

            s.executeUpdate("CREATE TABLE " + quoteId(t.settings()) + " ("
                    + "scope TEXT NOT NULL, "
                    + "scope_key TEXT NOT NULL, "
                    + "key TEXT NOT NULL, "
                    + "value BYTEA NOT NULL, "
                    + "updated_at BIGINT NOT NULL, "
                    + "PRIMARY KEY (scope, scope_key, key)"
                    + ")");
        }
    }

    /** Quote an identifier for Postgres. Doubles any embedded quote. */
    static String quoteId(String id) {
        return "\"" + id.replace("\"", "\"\"") + "\"";
    }
}
