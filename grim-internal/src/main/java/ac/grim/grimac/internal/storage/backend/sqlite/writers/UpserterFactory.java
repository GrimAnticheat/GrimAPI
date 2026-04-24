package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Builds dialect-appropriate upserters for a given connection. Selected once
 * at backend init from the live {@code sqlite_version()} — modern path uses
 * single-statement {@code ON CONFLICT DO UPDATE} (SQLite 3.24+), legacy path
 * uses two-step {@code INSERT OR IGNORE} + {@code UPDATE} for older engines.
 */
@ApiStatus.Internal
public interface UpserterFactory {

    SessionUpserter newSessionUpserter(Connection c, TableNames t) throws SQLException;

    IdentityUpserter newIdentityUpserter(Connection c, TableNames t) throws SQLException;

    SettingsUpserter newSettingsUpserter(Connection c, TableNames t) throws SQLException;

    UpserterFactory MODERN = new ModernUpserterFactory();
}
