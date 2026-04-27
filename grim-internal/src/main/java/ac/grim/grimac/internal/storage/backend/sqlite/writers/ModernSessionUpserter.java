package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.util.UuidCodec;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

@ApiStatus.Internal
final class ModernSessionUpserter implements SessionUpserter {

    private final PreparedStatement upsert;

    ModernSessionUpserter(Connection c, TableNames t) throws SQLException {
        this.upsert = c.prepareStatement(
                "INSERT INTO " + t.sessions() + "(session_id, player_uuid, server_name, started_at, last_activity, closed_at, "
                        + "grim_version, client_brand, client_version_pvn, server_version, replay_clips_json) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        + "ON CONFLICT(session_id) DO UPDATE SET "
                        + "server_name=excluded.server_name, "
                        + "last_activity=excluded.last_activity, "
                        // closed_at is only ever transitioned NULL → set; never overwrite
                        // a non-null close timestamp with a NULL heartbeat (otherwise a
                        // late heartbeat after close() would re-open the row).
                        + "closed_at=COALESCE(" + t.sessions() + ".closed_at, excluded.closed_at), "
                        + "grim_version=excluded.grim_version, "
                        + "client_brand=excluded.client_brand, "
                        + "client_version_pvn=excluded.client_version_pvn, "
                        + "server_version=excluded.server_version, "
                        + "replay_clips_json=excluded.replay_clips_json");
    }

    @Override
    public void addBatch(UUID sessionId,
                         UUID playerUuid,
                         String serverName,
                         long startedEpochMs,
                         long lastActivityEpochMs,
                         Long closedAtEpochMs,
                         String grimVersion,
                         String clientBrand,
                         int clientVersionPvn,
                         String serverVersionString,
                         String replayClipsJson) throws SQLException {
        upsert.setBytes(1, UuidCodec.toBytes(sessionId));
        upsert.setBytes(2, UuidCodec.toBytes(playerUuid));
        upsert.setString(3, serverName);
        upsert.setLong(4, startedEpochMs);
        upsert.setLong(5, lastActivityEpochMs);
        if (closedAtEpochMs == null) upsert.setNull(6, java.sql.Types.BIGINT);
        else upsert.setLong(6, closedAtEpochMs);
        upsert.setString(7, grimVersion);
        upsert.setString(8, clientBrand);
        upsert.setInt(9, clientVersionPvn);
        upsert.setString(10, serverVersionString);
        upsert.setString(11, replayClipsJson);
        upsert.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        upsert.executeBatch();
    }

    @Override
    public void close() throws SQLException {
        upsert.close();
    }
}
