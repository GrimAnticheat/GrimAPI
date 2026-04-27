package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.util.UuidCodec;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Pre-3.24 SQLite session upsert via {@code INSERT OR IGNORE} + {@code UPDATE}.
 * The two statements share the caller's open transaction (connection stays in
 * {@code autoCommit=false}), so the pair is observed atomically.
 */
@ApiStatus.Internal
final class LegacySessionUpserter implements SessionUpserter {

    private final PreparedStatement insertIgnore;
    private final PreparedStatement update;

    LegacySessionUpserter(Connection c, TableNames t) throws SQLException {
        this.insertIgnore = c.prepareStatement(
                "INSERT OR IGNORE INTO " + t.sessions() + "(session_id, player_uuid, server_name, started_at, last_activity, closed_at, "
                        + "grim_version, client_brand, client_version_pvn, server_version, replay_clips_json) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        // closed_at uses COALESCE on the existing column so a heartbeat with
        // null closed_at can't overwrite a previously-set close timestamp.
        this.update = c.prepareStatement(
                "UPDATE " + t.sessions() + " SET "
                        + "server_name = ?, "
                        + "last_activity = ?, "
                        + "closed_at = COALESCE(closed_at, ?), "
                        + "grim_version = ?, "
                        + "client_brand = ?, "
                        + "client_version_pvn = ?, "
                        + "server_version = ?, "
                        + "replay_clips_json = ? "
                        + "WHERE session_id = ?");
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
        byte[] sessionIdBytes = UuidCodec.toBytes(sessionId);

        insertIgnore.setBytes(1, sessionIdBytes);
        insertIgnore.setBytes(2, UuidCodec.toBytes(playerUuid));
        insertIgnore.setString(3, serverName);
        insertIgnore.setLong(4, startedEpochMs);
        insertIgnore.setLong(5, lastActivityEpochMs);
        if (closedAtEpochMs == null) insertIgnore.setNull(6, java.sql.Types.BIGINT);
        else insertIgnore.setLong(6, closedAtEpochMs);
        insertIgnore.setString(7, grimVersion);
        insertIgnore.setString(8, clientBrand);
        insertIgnore.setInt(9, clientVersionPvn);
        insertIgnore.setString(10, serverVersionString);
        insertIgnore.setString(11, replayClipsJson);
        insertIgnore.addBatch();

        update.setString(1, serverName);
        update.setLong(2, lastActivityEpochMs);
        if (closedAtEpochMs == null) update.setNull(3, java.sql.Types.BIGINT);
        else update.setLong(3, closedAtEpochMs);
        update.setString(4, grimVersion);
        update.setString(5, clientBrand);
        update.setInt(6, clientVersionPvn);
        update.setString(7, serverVersionString);
        update.setString(8, replayClipsJson);
        update.setBytes(9, sessionIdBytes);
        update.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        insertIgnore.executeBatch();
        update.executeBatch();
    }

    @Override
    public void close() throws SQLException {
        SQLException first = null;
        try { insertIgnore.close(); } catch (SQLException e) { first = e; }
        try { update.close(); } catch (SQLException e) { if (first == null) first = e; else first.addSuppressed(e); }
        if (first != null) throw first;
    }
}
