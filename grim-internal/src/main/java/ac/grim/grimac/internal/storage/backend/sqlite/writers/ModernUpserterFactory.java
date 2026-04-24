package ac.grim.grimac.internal.storage.backend.sqlite.writers;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.SQLException;

@ApiStatus.Internal
final class ModernUpserterFactory implements UpserterFactory {

    @Override
    public SessionUpserter newSessionUpserter(Connection c, TableNames t) throws SQLException {
        return new ModernSessionUpserter(c, t);
    }

    @Override
    public IdentityUpserter newIdentityUpserter(Connection c, TableNames t) throws SQLException {
        return new ModernIdentityUpserter(c, t);
    }

    @Override
    public SettingsUpserter newSettingsUpserter(Connection c, TableNames t) throws SQLException {
        return new ModernSettingsUpserter(c, t);
    }
}
