package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.util.UuidCodec;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

@ApiStatus.Internal
final class ModernIdentityUpserter implements IdentityUpserter {

    private final PreparedStatement upsert;

    ModernIdentityUpserter(Connection c, TableNames t) throws SQLException {
        this.upsert = c.prepareStatement(
                "INSERT INTO " + t.players() + "(uuid, current_name, first_seen, last_seen) "
                        + "VALUES (?, ?, ?, ?) "
                        + "ON CONFLICT(uuid) DO UPDATE SET "
                        + "current_name = excluded.current_name, "
                        + "first_seen = min(first_seen, excluded.first_seen), "
                        + "last_seen = max(last_seen, excluded.last_seen)");
    }

    @Override
    public void addBatch(UUID uuid,
                         String currentName,
                         long firstSeenEpochMs,
                         long lastSeenEpochMs) throws SQLException {
        upsert.setBytes(1, UuidCodec.toBytes(uuid));
        upsert.setString(2, currentName);
        upsert.setLong(3, firstSeenEpochMs);
        upsert.setLong(4, lastSeenEpochMs);
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
