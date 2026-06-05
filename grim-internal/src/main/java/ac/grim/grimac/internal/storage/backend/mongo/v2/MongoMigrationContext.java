package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.registry.MigrationContext;
import com.mongodb.client.MongoDatabase;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.logging.Logger;

/**
 * Mongo-specific {@link MigrationContext} passed to migrations registered
 * with the Mongo backend's KindAdapters. Hands the migration the live
 * database handle and a logger; concrete migration implementations cast
 * the abstract context back to this type.
 */
@ApiStatus.Internal
public final class MongoMigrationContext implements MigrationContext {

    private final @NotNull MongoDatabase database;
    private final @NotNull Logger logger;

    public MongoMigrationContext(@NotNull MongoDatabase database, @NotNull Logger logger) {
        this.database = database;
        this.logger = logger;
    }

    public @NotNull MongoDatabase database() { return database; }
    public @NotNull Logger logger()          { return logger; }
}
