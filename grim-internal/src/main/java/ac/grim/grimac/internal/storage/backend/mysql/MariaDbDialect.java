package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

/**
 * MariaDB 10.6+ dialect. Inherits the v8 baseline DDL and read SQL from
 * {@link MysqlDialect}; the only divergence from {@link MysqlEightDialect}
 * is the upsert syntax, which uses the legacy {@code VALUES()} reference
 * because MariaDB never adopted the MySQL 8.0.19 aliased-row form.
 * <p>
 * Inherits the no-op {@code migrateV7ToV8} from {@link MysqlDialect} —
 * MariaDB v7 deployments already had the v8 column layout (its dialect was
 * always at the gen col shape), so no schema rewrite is needed; only the
 * meta table's schema_version row gets bumped.
 * <p>
 * Floor of 10.6: oldest MariaDB LTS still in upstream support, comfortably
 * above the {@code STORED} generated-column floor (10.2.1) and the enforced
 * {@code CHECK} floor (10.2.1) we depend on. 11.x and 12.x are supersets of
 * the same DDL — no further version splits needed.
 */
@ApiStatus.Internal
final class MariaDbDialect implements MysqlDialect {

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
}
