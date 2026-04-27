package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@ApiStatus.Internal
final class LegacySettingsUpserter implements SettingsUpserter {

    private final PreparedStatement insertIgnore;
    private final PreparedStatement update;

    LegacySettingsUpserter(Connection c, TableNames t) throws SQLException {
        this.insertIgnore = c.prepareStatement(
                "INSERT OR IGNORE INTO " + t.settings() + "(scope, scope_key, key, value, updated_at) "
                        + "VALUES (?, ?, ?, ?, ?)");
        this.update = c.prepareStatement(
                "UPDATE " + t.settings() + " SET "
                        + "value = ?, "
                        + "updated_at = ? "
                        + "WHERE scope = ? AND scope_key = ? AND key = ?");
    }

    @Override
    public void addBatch(String scope,
                         String scopeKey,
                         String key,
                         byte[] value,
                         long updatedEpochMs) throws SQLException {
        insertIgnore.setString(1, scope);
        insertIgnore.setString(2, scopeKey);
        insertIgnore.setString(3, key);
        insertIgnore.setBytes(4, value);
        insertIgnore.setLong(5, updatedEpochMs);
        insertIgnore.addBatch();

        update.setBytes(1, value);
        update.setLong(2, updatedEpochMs);
        update.setString(3, scope);
        update.setString(4, scopeKey);
        update.setString(5, key);
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
