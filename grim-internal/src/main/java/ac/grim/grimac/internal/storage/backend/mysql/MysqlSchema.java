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
 * MySQL-family schema for the MySQL backend. New backends are born at the
 * {@link #CURRENT_VERSION} baseline (the shape SQLite reached at v5) rather
 * than replaying each migration — there are no pre-v5 MySQL databases to
 * upgrade from, so the linear applyVN pattern used by SQLite is overkill
 * for the v0 → v5 range. Per-version forward migrations land here.
 * <p>
 * Baseline DDL and per-version migrations may be delegated to the
 * {@link MysqlDialect} the {@link MysqlBackend} probed at init when the
 * change is flavor-specific — currently {@code applyBaseline} (formerly
 * different between MySQL functional indexes and MariaDB STORED gen cols;
 * unified at v8 onto the gen col shape) and {@code migrateV7ToV8} (the
 * MySQL-side ALTER that brings v7 deployments onto the v8 shape; no-op on
 * MariaDB whose v7 schema already matched).
 */
@ApiStatus.Internal
public final class MysqlSchema {

    public static final int CURRENT_VERSION = 8;

    private MysqlSchema() {}

    public static void ensureInitialized(Connection c, String grimCoreVersion, TableNames t, MysqlDialect dialect) throws SQLException {
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
            applyBaseline(c, t, dialect);
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
        if (existing < 8) migrateV7ToV8(c, t, dialect);

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

    /**
     * v7 → v8: unify the players table on {@code current_name_lower} as a
     * STORED generated column with a plain B-tree index. v7 had a flavor
     * split — MySQL used a functional index, MariaDB used the gen col
     * shape — but MySQL's optimiser refused to use the functional index
     * for prefix LIKE patterns, leaving {@code listPlayersByNamePrefix}
     * on MySQL as an O(table-size) full scan. Delegated to the dialect
     * because only the MySQL path needs ALTER work; on MariaDB this is
     * a no-op (its v7 schema already matched the v8 shape).
     */
    private static void migrateV7ToV8(Connection c, TableNames t, MysqlDialect dialect) throws SQLException {
        try (Statement s = c.createStatement()) {
            dialect.migrateV7ToV8(s, t);
        }
    }

    private static void applyBaseline(Connection c, TableNames t, MysqlDialect dialect) throws SQLException {
        try (Statement s = c.createStatement()) {
            dialect.applyBaseline(s, t);
        }
    }
}
