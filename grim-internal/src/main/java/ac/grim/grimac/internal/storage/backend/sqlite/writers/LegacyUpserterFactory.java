package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.SQLException;

@ApiStatus.Internal
final class LegacyUpserterFactory implements UpserterFactory {

    @Override
    public SessionUpserter newSessionUpserter(Connection c, TableNames t) throws SQLException {
        return new LegacySessionUpserter(c, t);
    }

    @Override
    public IdentityUpserter newIdentityUpserter(Connection c, TableNames t) throws SQLException {
        return new LegacyIdentityUpserter(c, t);
    }

    @Override
    public SettingsUpserter newSettingsUpserter(Connection c, TableNames t) throws SQLException {
        return new LegacySettingsUpserter(c, t);
    }
}
