package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import org.jetbrains.annotations.ApiStatus;

import java.sql.SQLException;
import java.util.UUID;

/**
 * Dialect-neutral player-identity upsert. Semantics: first_seen takes the
 * minimum, last_seen takes the maximum, current_name overwrites.
 */
@ApiStatus.Internal
public interface IdentityUpserter extends AutoCloseable {

    void addBatch(UUID uuid,
                  String currentName,
                  long firstSeenEpochMs,
                  long lastSeenEpochMs) throws SQLException;

    void executeBatch() throws SQLException;

    @Override
    void close() throws SQLException;
}
