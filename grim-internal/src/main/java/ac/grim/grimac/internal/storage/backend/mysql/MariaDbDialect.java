package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * MariaDB 10.6+ dialect. MariaDB doesn't implement either of the two MySQL-8
 * features that {@link MysqlEightDialect} relies on:
 * <ul>
 *   <li>Functional indexes ({@code INDEX … ((LOWER(col)))}) — MariaDB tracks
 *       this as <a href="https://jira.mariadb.org/browse/MDEV-22597">MDEV-22597</a>
 *       (still open at the time of writing). The portable MariaDB equivalent
 *       is a {@code STORED} generated column with a plain B-tree index.</li>
 *   <li>The aliased-row upsert form ({@code … AS new ON DUPLICATE KEY UPDATE
 *       col = new.col}) — MariaDB never adopted MySQL 8.0.19's syntax. The
 *       legacy {@code … ON DUPLICATE KEY UPDATE col = VALUES(col)} form,
 *       deprecated in MySQL 8.0.20 but never deprecated in MariaDB, works on
 *       both engines and is what we emit here.</li>
 * </ul>
 * Floor of 10.6: that's the oldest MariaDB LTS still in upstream support,
 * comfortably above the {@code STORED} generated-column floor (10.2.1) and
 * the enforced {@code CHECK} floor (10.2.1) we depend on. 11.x and 12.x are
 * supersets of the same DDL — no further version splits needed.
 */
@ApiStatus.Internal
final class MariaDbDialect implements MysqlDialect {

    @Override
    public void applyBaseline(Statement s, TableNames t) throws SQLException {
        s.executeUpdate("CREATE TABLE " + t.checks() + " ("
                + "check_id INT AUTO_INCREMENT PRIMARY KEY, "
                + "stable_key VARCHAR(255) NOT NULL UNIQUE, "
                + "display VARCHAR(255), "
                + "description VARCHAR(1024), "
                + "introduced_version VARCHAR(64), "
                + "removed_version VARCHAR(64), "
                + "introduced_at BIGINT"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        // STORED generated column — auto-populated on INSERT, write path stays
        // identical (only uuid/current_name/first_seen/last_seen get bound).
        // The plain B-tree index on current_name_lower carries WHERE
        // current_name_lower = ? and WHERE current_name_lower LIKE 'x%' the
        // same way MySQL's functional index does — verified via EXPLAIN on
        // 10.11. Note: MariaDB does NOT rewrite WHERE LOWER(current_name)=?
        // to use this index (full table scan), so the read queries below
        // reference current_name_lower directly.
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

    @Override
    public String upsertSessions(TableNames t) {
        return "INSERT INTO " + t.sessions() + "(session_id, player_uuid, server_name, started_at, last_activity, closed_at, "
                + "grim_version, client_brand, client_version_pvn, server_version, replay_clips_json) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE "
                + "server_name=VALUES(server_name), "
                + "last_activity=VALUES(last_activity), "
                // closed_at: NULL → set transitions only; never overwrite an
                // already-closed row with NULL on a late heartbeat.
                + "closed_at=COALESCE(" + t.sessions() + ".closed_at, VALUES(closed_at)), "
                + "grim_version=VALUES(grim_version), "
                + "client_brand=VALUES(client_brand), "
                + "client_version_pvn=VALUES(client_version_pvn), "
                + "server_version=VALUES(server_version), "
                + "replay_clips_json=VALUES(replay_clips_json)";
    }

    @Override
    public String upsertIdentities(TableNames t) {
        return "INSERT INTO " + t.players() + "(uuid, current_name, first_seen, last_seen) "
                + "VALUES (?, ?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE "
                + "current_name = VALUES(current_name), "
                + "first_seen = LEAST(" + t.players() + ".first_seen, VALUES(first_seen)), "
                + "last_seen = GREATEST(" + t.players() + ".last_seen, VALUES(last_seen))";
    }

    @Override
    public String upsertSettings(TableNames t) {
        return "INSERT INTO " + t.settings() + "(scope, scope_key, `key`, value, updated_at) "
                + "VALUES (?, ?, ?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE "
                + "value = VALUES(value), "
                + "updated_at = VALUES(updated_at)";
    }

    @Override
    public String selectIdentityByName(TableNames t) {
        return "SELECT uuid, current_name, first_seen, last_seen FROM " + t.players() + " "
                + "WHERE current_name_lower = ? ORDER BY last_seen DESC LIMIT 1";
    }

    @Override
    public String selectIdentitiesByNamePrefix(TableNames t) {
        return "SELECT uuid, current_name, first_seen, last_seen FROM " + t.players() + " "
                + "WHERE current_name_lower LIKE ? ESCAPE '\\\\' "
                + "ORDER BY last_seen DESC LIMIT ?";
    }
}
