package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@ApiStatus.Internal
final class ModernSettingsUpserter implements SettingsUpserter {

    private final PreparedStatement upsert;

    ModernSettingsUpserter(Connection c, TableNames t) throws SQLException {
        this.upsert = c.prepareStatement(
                "INSERT INTO " + t.settings() + "(scope, scope_key, key, value, updated_at) "
                        + "VALUES (?, ?, ?, ?, ?) "
                        + "ON CONFLICT(scope, scope_key, key) DO UPDATE SET "
                        + "value = excluded.value, "
                        + "updated_at = excluded.updated_at");
    }

    @Override
    public void addBatch(String scope,
                         String scopeKey,
                         String key,
                         byte[] value,
                         long updatedEpochMs) throws SQLException {
        upsert.setString(1, scope);
        upsert.setString(2, scopeKey);
        upsert.setString(3, key);
        upsert.setBytes(4, value);
        upsert.setLong(5, updatedEpochMs);
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
