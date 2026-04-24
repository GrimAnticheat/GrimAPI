package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import org.jetbrains.annotations.ApiStatus;

import java.sql.SQLException;
import java.util.UUID;

/**
 * Dialect-neutral session upsert over a single {@link java.sql.Connection}.
 * Implementations own one or more {@link java.sql.PreparedStatement}s and
 * accumulate rows via JDBC batching. A modern impl backs this with one
 * INSERT ... ON CONFLICT DO UPDATE; a legacy impl with INSERT OR IGNORE
 * followed by UPDATE — same external semantics, picked once at init.
 */
@ApiStatus.Internal
public interface SessionUpserter extends AutoCloseable {

    void addBatch(UUID sessionId,
                  UUID playerUuid,
                  String serverName,
                  long startedEpochMs,
                  long lastActivityEpochMs,
                  String grimVersion,
                  String clientBrand,
                  int clientVersionPvn,
                  String serverVersionString,
                  String replayClipsJson) throws SQLException;

    void executeBatch() throws SQLException;

    @Override
    void close() throws SQLException;
}
