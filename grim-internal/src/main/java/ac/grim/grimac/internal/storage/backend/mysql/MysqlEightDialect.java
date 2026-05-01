package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * MySQL 8.0+ dialect. Uses the functional index
 * ({@code INDEX … ((LOWER(current_name)))}) introduced in MySQL 8.0.13 and the
 * aliased-row upsert form ({@code … AS new ON DUPLICATE KEY UPDATE col = new.col})
 * introduced in MySQL 8.0.19.
 * <p>
 * The minimum effective MySQL version is therefore 8.0.19 — the
 * {@link MysqlBackend} version probe doesn't enforce that explicitly because
 * pre-8.0.19 MySQL is older than the connector-j we ship and effectively
 * extinct in production.
 */
@ApiStatus.Internal
final class MysqlEightDialect implements MysqlDialect {

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

        // Functional index — MySQL 8.0.13+. Lookups via WHERE LOWER(current_name)
        // = ? are rewritten by the optimiser to use this index directly; no
        // generated column needed.
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

    @Override
    public String upsertSessions(TableNames t) {
        return "INSERT INTO " + t.sessions() + "(session_id, player_uuid, server_name, started_at, last_activity, closed_at, "
                + "grim_version, client_brand, client_version_pvn, server_version, replay_clips_json) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS new "
                + "ON DUPLICATE KEY UPDATE "
                + "server_name=new.server_name, "
                + "last_activity=new.last_activity, "
                // closed_at: NULL → set transitions only; never overwrite an
                // already-closed row with NULL on a late heartbeat.
                + "closed_at=COALESCE(" + t.sessions() + ".closed_at, new.closed_at), "
                + "grim_version=new.grim_version, "
                + "client_brand=new.client_brand, "
                + "client_version_pvn=new.client_version_pvn, "
                + "server_version=new.server_version, "
                + "replay_clips_json=new.replay_clips_json";
    }

    @Override
    public String upsertIdentities(TableNames t) {
        return "INSERT INTO " + t.players() + "(uuid, current_name, first_seen, last_seen) "
                + "VALUES (?, ?, ?, ?) AS new "
                + "ON DUPLICATE KEY UPDATE "
                + "current_name = new.current_name, "
                + "first_seen = LEAST(" + t.players() + ".first_seen, new.first_seen), "
                + "last_seen = GREATEST(" + t.players() + ".last_seen, new.last_seen)";
    }

    @Override
    public String upsertSettings(TableNames t) {
        return "INSERT INTO " + t.settings() + "(scope, scope_key, `key`, value, updated_at) "
                + "VALUES (?, ?, ?, ?, ?) AS new "
                + "ON DUPLICATE KEY UPDATE "
                + "value = new.value, "
                + "updated_at = new.updated_at";
    }

    @Override
    public String selectIdentityByName(TableNames t) {
        return "SELECT uuid, current_name, first_seen, last_seen FROM " + t.players() + " "
                + "WHERE LOWER(current_name) = ? ORDER BY last_seen DESC LIMIT 1";
    }

    @Override
    public String selectIdentitiesByNamePrefix(TableNames t) {
        return "SELECT uuid, current_name, first_seen, last_seen FROM " + t.players() + " "
                + "WHERE LOWER(current_name) LIKE ? ESCAPE '\\\\' "
                + "ORDER BY last_seen DESC LIMIT ?";
    }
}
