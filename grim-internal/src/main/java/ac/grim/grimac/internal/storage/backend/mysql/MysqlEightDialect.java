package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * MySQL 8.0+ dialect. Inherits the v8 baseline DDL and read SQL from
 * {@link MysqlDialect}; the only divergence from {@link MariaDbDialect} is
 * the upsert syntax, which uses MySQL 8.0.19's aliased-row form
 * ({@code … AS new ON DUPLICATE KEY UPDATE col = new.col}) to avoid the
 * deprecated {@code VALUES()} reference.
 * <p>
 * The minimum effective MySQL version is 8.0.19 — pre-8.0.19 MySQL is older
 * than the connector-j we ship and effectively extinct in production, so the
 * version probe in {@link MysqlBackend} doesn't enforce that explicitly.
 */
@ApiStatus.Internal
final class MysqlEightDialect implements MysqlDialect {

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

    /**
     * v7 → v8 migration on MySQL: replace the functional-index shape with
     * the gen col shape so {@code listPlayersByNamePrefix} actually uses
     * an index. MySQL 8 doesn't support {@code ALGORITHM=INSTANT} or
     * {@code INPLACE} for adding STORED generated columns — falls back to
     * {@code ALGORITHM=COPY}, which rewrites the table and blocks writes
     * for the duration. Acceptable on alpha-only v7 tables; for any
     * future post-GA migration, an external online tool
     * (gh-ost / pt-online-schema-change) would be the move.
     * <p>
     * The DROP+ADD INDEX is one ALTER so the table never has zero
     * lower-case-name index between the two operations. ANALYZE TABLE
     * is run last so the optimiser sees fresh stats on the new index.
     */
    @Override
    public void migrateV7ToV8(Statement s, TableNames t) throws SQLException {
        s.executeUpdate("ALTER TABLE " + t.players()
                + " ADD COLUMN current_name_lower VARCHAR(32) AS (LOWER(current_name)) STORED");
        s.executeUpdate("ALTER TABLE " + t.players()
                + " DROP INDEX idx_" + t.players() + "_name_lower"
                + ", ADD INDEX idx_" + t.players() + "_name_lower (current_name_lower)");
        // ANALYZE TABLE returns a one-row result set (Op/Msg_type/Msg_text)
        // rather than an update count, so executeUpdate() rejects it. Use
        // execute() and discard the resultset cursor.
        s.execute("ANALYZE TABLE " + t.players());
    }
}
