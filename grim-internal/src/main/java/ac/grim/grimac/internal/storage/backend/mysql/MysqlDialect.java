package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Per-flavor SQL strings and DDL for the MySQL-family backend.
 * <p>
 * Both impls share identical baseline DDL ({@code current_name_lower} as a
 * STORED generated column with a plain B-tree index — used to be a flavor
 * split via MySQL functional indexes vs MariaDB STORED gen cols, but MySQL's
 * functional-index path didn't actually use the index for prefix LIKE so the
 * shapes converged on the gen col). Both impls also share identical reads.
 * The only divergence left is the upsert syntax:
 * <ul>
 *   <li>{@link MysqlEightDialect} — MySQL 8.0+. Uses the aliased-row upsert
 *       ({@code INSERT … AS new ON DUPLICATE KEY UPDATE col = new.col}, MySQL
 *       8.0.19+) which avoids the deprecated {@code VALUES()} reference.</li>
 *   <li>{@link MariaDbDialect} — MariaDB 10.6+. MariaDB never adopted the
 *       MySQL 8.0.19 aliased-row form, so it uses the legacy
 *       {@code … ON DUPLICATE KEY UPDATE col = VALUES(col)} (deprecated in
 *       MySQL 8.0.20 but never deprecated in MariaDB).</li>
 * </ul>
 * The dialect is selected at {@link MysqlBackend#init} time by probing
 * {@code SELECT VERSION()} — same shape as the SQLite backend's
 * {@link ac.grim.grimac.internal.storage.backend.sqlite.writers.UpserterFactory}
 * version-driven dialect split.
 */
@ApiStatus.Internal
public interface MysqlDialect {

    /**
     * Fresh-database baseline DDL. Creates checks, players, sessions,
     * violations, settings tables for a brand-new database. Identical
     * across both flavors as of v8 — they share the gen col shape on
     * the players table. The meta table is created independently in
     * {@link MysqlSchema}.
     */
    default void applyBaseline(Statement s, TableNames t) throws SQLException {
        s.executeUpdate("CREATE TABLE " + t.checks() + " ("
                + "check_id INT AUTO_INCREMENT PRIMARY KEY, "
                + "stable_key VARCHAR(255) NOT NULL UNIQUE, "
                + "display VARCHAR(255), "
                + "description VARCHAR(1024), "
                + "introduced_version VARCHAR(64), "
                + "removed_version VARCHAR(64), "
                + "introduced_at BIGINT"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        // STORED generated column auto-populated on INSERT/UPDATE; the write
        // path stays narrow (only uuid/current_name/first_seen/last_seen
        // bound). The plain B-tree index on current_name_lower carries both
        // exact lookups (WHERE current_name_lower = ?) and prefix range scans
        // (WHERE current_name_lower LIKE 'x%') on both MySQL 8.x and MariaDB
        // 10.6+ — verified via EXPLAIN.
        //
        // Used to be a MySQL-only functional index ((LOWER(current_name)))
        // here, but MySQL's optimiser refuses to use functional indexes for
        // prefix LIKE patterns (only equality is sargable), making
        // listPlayersByNamePrefix an O(table size) full scan regardless of
        // selectivity. v7→v8 migration unifies on the gen col shape.
        s.executeUpdate("CREATE TABLE " + t.players() + " ("
                + "uuid BINARY(16) PRIMARY KEY, "
                + "current_name VARCHAR(32), "
                + "current_name_lower VARCHAR(32) AS (LOWER(current_name)) STORED, "
                + "first_seen BIGINT NOT NULL, "
                + "last_seen BIGINT NOT NULL, "
                + "INDEX idx_" + t.players() + "_name_lower (current_name_lower)"
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

    String upsertSessions(TableNames t);

    String upsertIdentities(TableNames t);

    String upsertSettings(TableNames t);

    /** Single-row case-insensitive name lookup, latest {@code last_seen} first. */
    default String selectIdentityByName(TableNames t) {
        return "SELECT uuid, current_name, first_seen, last_seen FROM " + t.players() + " "
                + "WHERE current_name_lower = ? ORDER BY last_seen DESC LIMIT 1";
    }

    /** Case-insensitive prefix lookup for the player-name autocomplete query. */
    default String selectIdentitiesByNamePrefix(TableNames t) {
        return "SELECT uuid, current_name, first_seen, last_seen FROM " + t.players() + " "
                + "WHERE current_name_lower LIKE ? ESCAPE '\\\\' "
                + "ORDER BY last_seen DESC LIMIT ?";
    }

    /**
     * v7 → v8 migration step. v7 was the last shape that had a flavor split
     * on the players table — MySQL used a functional index on
     * {@code LOWER(current_name)}, MariaDB used a STORED generated
     * {@code current_name_lower} column. v8 unifies on the MariaDB shape.
     * <p>
     * Default no-op: MariaDB v7 deployments already had the v8 column
     * layout (its dialect was always at this shape), so this hook only
     * needs work for MySQL — see {@link MysqlEightDialect}'s override.
     */
    default void migrateV7ToV8(Statement s, TableNames t) throws SQLException {
        // no-op
    }
}
