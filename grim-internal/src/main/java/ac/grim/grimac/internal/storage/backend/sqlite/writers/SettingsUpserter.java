package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import org.jetbrains.annotations.ApiStatus;

import java.sql.SQLException;

/**
 * Dialect-neutral settings upsert keyed by (scope, scopeKey, key).
 */
@ApiStatus.Internal
public interface SettingsUpserter extends AutoCloseable {

    void addBatch(String scope,
                  String scopeKey,
                  String key,
                  byte[] value,
                  long updatedEpochMs) throws SQLException;

    void executeBatch() throws SQLException;

    @Override
    void close() throws SQLException;
}
