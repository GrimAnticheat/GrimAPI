package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.util.UuidCodec;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;
import java.util.UUID;

/**
 * Pre-3.24 SQLite identity upsert. first_seen/last_seen semantics
 * (min/max merge) are preserved via the UPDATE's own aggregate expressions.
 */
@ApiStatus.Internal
final class LegacyIdentityUpserter implements IdentityUpserter {

    private final PreparedStatement insertIgnore;
    private final PreparedStatement update;

    LegacyIdentityUpserter(Connection c, TableNames t) throws SQLException {
        this.insertIgnore = c.prepareStatement(
                "INSERT OR IGNORE INTO " + t.players() + "(uuid, current_name, current_name_lower, first_seen, last_seen) "
                        + "VALUES (?, ?, ?, ?, ?)");
        this.update = c.prepareStatement(
                "UPDATE " + t.players() + " SET "
                        + "current_name = ?, "
                        + "current_name_lower = ?, "
                        + "first_seen = min(first_seen, ?), "
                        + "last_seen = max(last_seen, ?) "
                        + "WHERE uuid = ?");
    }

    @Override
    public void addBatch(UUID uuid,
                         String currentName,
                         long firstSeenEpochMs,
                         long lastSeenEpochMs) throws SQLException {
        byte[] uuidBytes = UuidCodec.toBytes(uuid);
        String currentNameLower = currentName == null ? null : currentName.toLowerCase(Locale.ROOT);

        insertIgnore.setBytes(1, uuidBytes);
        insertIgnore.setString(2, currentName);
        insertIgnore.setString(3, currentNameLower);
        insertIgnore.setLong(4, firstSeenEpochMs);
        insertIgnore.setLong(5, lastSeenEpochMs);
        insertIgnore.addBatch();

        update.setString(1, currentName);
        update.setString(2, currentNameLower);
        update.setLong(3, firstSeenEpochMs);
        update.setLong(4, lastSeenEpochMs);
        update.setBytes(5, uuidBytes);
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
