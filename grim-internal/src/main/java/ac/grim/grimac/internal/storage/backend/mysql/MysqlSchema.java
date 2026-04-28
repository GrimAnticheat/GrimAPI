package ac.grim.grimac.internal.storage.backend.mysql;

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
 * MySQL 8.x schema for the MySQL backend. New backends are born at the
 * {@link #CURRENT_VERSION} baseline (the shape SQLite reached at v5) rather
 * than replaying each migration — there are no pre-v5 MySQL databases to
 * upgrade from, so the linear applyVN pattern used by SQLite is overkill
 * here. Future bumps will add applyVN methods as normal.
 */
@ApiStatus.Internal
public final class MysqlSchema {

    public static final int CURRENT_VERSION = 7;

    private MysqlSchema() {}

    public static void ensureInitialized(Connection c, String grimCoreVersion, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            // MySQL check-constraint names are schema-global (not table-scoped),
            // so the name must be unique per meta table in the DB — otherwise two
            // datastores sharing one MySQL instance collide. Scope the name by
            // interpolating the (already-unique) meta table name.
            s.executeUpdate("CREATE TABLE IF NOT EXISTS " + t.meta() + " ("
                    + "id TINYINT PRIMARY KEY, "
                    + "schema_version INT NOT NULL, "
                    + "grim_core_version VARCHAR(64), "
                    + "initialized_at BIGINT NOT NULL, "
                    + "last_migration_at BIGINT NOT NULL, "
                    + "CONSTRAINT " + t.meta() + "_singleton CHECK (id = 0)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        }

        int existing = readSchemaVersion(c, t);
        if (existing < 0) {
            applyBaseline(c, t);
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
            throw new SQLException("[grim-datastore] MySQL schema is version " + existing
                    + " but this core supports up to " + CURRENT_VERSION
                    + "; refusing to downgrade. Roll Grim forward.");
        }

        // Forward migrations — additive only.
        if (existing < 6) migrateV5ToV6(c, t);
        if (existing < 7) migrateV6ToV7(c, t);

        if (existing < CURRENT_VERSION) {
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE " + t.meta() + " SET schema_version = ?, last_migration_at = ? WHERE id = 0")) {
                ps.setInt(1, CURRENT_VERSION);
                ps.setLong(2, System.currentTimeMillis());
                ps.executeUpdate();
            }
        }
    }

    /** v5 → v6: add {@code closed_at} to the sessions table. */
    private static void migrateV5ToV6(Connection c, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("ALTER TABLE " + t.sessions() + " ADD COLUMN closed_at BIGINT NULL");
        }
    }

    /** v6 → v7: rewrite {@code grim.legacy.*} stable_keys onto descriptive ones. See {@link LegacyKeyRenames}. */
    private static void migrateV6ToV7(Connection c, TableNames t) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE " + t.checks() + " SET stable_key = ? WHERE stable_key = ?")) {
            for (Map.Entry<String, String> e : LegacyKeyRenames.OLD_TO_NEW.entrySet()) {
                ps.setString(1, e.getValue());
                ps.setString(2, e.getKey());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public static int readSchemaVersion(Connection c, TableNames t) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT schema_version FROM " + t.meta() + " WHERE id = 0");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
            return -1;
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage();
            // MySQL "Table 'foo.bar' doesn't exist" — error code 1146
            if (msg.contains("doesn't exist") || e.getErrorCode() == 1146) return -1;
            throw e;
        }
    }

    private static void applyBaseline(Connection c, TableNames t) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE " + t.checks() + " ("
                    + "check_id INT AUTO_INCREMENT PRIMARY KEY, "
                    + "stable_key VARCHAR(255) NOT NULL UNIQUE, "
                    + "display VARCHAR(255), "
                    + "description VARCHAR(1024), "
                    + "introduced_version VARCHAR(64), "
                    + "removed_version VARCHAR(64), "
                    + "introduced_at BIGINT"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

            s.executeUpdate("CREATE TABLE " + t.players() + " ("
                    + "uuid BINARY(16) PRIMARY KEY, "
                    + "current_name VARCHAR(32), "
                    + "first_seen BIGINT NOT NULL, "
                    + "last_seen BIGINT NOT NULL, "
                    + "INDEX idx_" + t.players() + "_name_lower ((LOWER(current_name)))"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

            s.executeUpdate("CREATE TABLE " + t.sessions() + " ("
                    + "session_id BINARY(16) PRIMARY KEY, "
                    + "player_uuid BINARY(16) NOT NULL, "
                    + "server_name VARCHAR(64), "
                    + "started_at BIGINT NOT NULL, "
                    + "last_activity BIGINT NOT NULL, "
                    + "closed_at BIGINT NULL, "
                    + "grim_version VARCHAR(64), "
                    + "client_brand VARCHAR(64), "
                    + "client_version_pvn INT NOT NULL DEFAULT -1, "
                    + "server_version VARCHAR(64), "
                    + "replay_clips_json MEDIUMTEXT, "
                    + "INDEX idx_" + t.sessions() + "_player_started (player_uuid, started_at DESC)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

            s.executeUpdate("CREATE TABLE " + t.violations() + " ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
                    + "session_id BINARY(16) NOT NULL, "
                    + "player_uuid BINARY(16) NOT NULL, "
                    + "check_id INT NOT NULL, "
                    + "vl DOUBLE NOT NULL, "
                    + "occurred_at BIGINT NOT NULL, "
                    + "verbose MEDIUMTEXT, "
                    + "verbose_format INT NOT NULL DEFAULT 0, "
                    + "metadata MEDIUMTEXT, "
                    + "INDEX idx_" + t.violations() + "_session_time (session_id, occurred_at), "
                    + "INDEX idx_" + t.violations() + "_player (player_uuid)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

            s.executeUpdate("CREATE TABLE " + t.settings() + " ("
                    + "scope VARCHAR(16) NOT NULL, "
                    + "scope_key VARCHAR(128) NOT NULL, "
                    + "`key` VARCHAR(128) NOT NULL, "
                    + "value LONGBLOB NOT NULL, "
                    + "updated_at BIGINT NOT NULL, "
                    + "PRIMARY KEY (scope, scope_key, `key`)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        }
    }
}
