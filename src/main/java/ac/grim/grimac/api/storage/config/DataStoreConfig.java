package ac.grim.grimac.api.storage.config;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.category.Category;
import org.jetbrains.annotations.ApiStatus;

import java.util.List;
import java.util.Map;

/**
 * Input contract for building a {@code DataStore}. Platform-agnostic — the
 * host assembles an instance from whatever config source it has (a YAML file,
 * an in-code builder, a database, …) and hands it to the storage wiring.
 * <p>
 * {@code routing} maps each category to a backend id; {@code backends} holds
 * the per-backend {@link BackendConfig} the matching {@code BackendProvider}
 * produced from its own settings source.
 */
@ApiStatus.Experimental
public record DataStoreConfig(
        Map<Category<?>, String> routing,
        Map<String, BackendConfig> backends,
        SessionConfig session,
        WritePathConfig writePath,
        Map<Category<?>, RetentionRule> retention,
        MigrationConfig migration,
        List<String> nameResolutionChain,
        HistoryConfig history,
        String serverName) {

    public DataStoreConfig {
        routing = routing == null ? Map.of() : Map.copyOf(routing);
        backends = backends == null ? Map.of() : Map.copyOf(backends);
        session = session == null ? SessionConfig.defaults() : session;
        writePath = writePath == null ? WritePathConfig.defaults() : writePath;
        retention = retention == null ? Map.of() : Map.copyOf(retention);
        migration = migration == null ? MigrationConfig.defaults() : migration;
        nameResolutionChain = nameResolutionChain == null ? List.of() : List.copyOf(nameResolutionChain);
        history = history == null ? HistoryConfig.defaults() : history;
        serverName = serverName == null ? "Unknown" : serverName;
    }
}
