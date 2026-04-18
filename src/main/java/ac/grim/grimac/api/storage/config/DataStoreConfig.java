package ac.grim.grimac.api.storage.config;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.category.Category;
import org.jetbrains.annotations.ApiStatus;

import java.util.List;
import java.util.Map;

/**
 * Input contract for building a {@code DataStore} in Layer 2. Platform-agnostic:
 * Layer 3 glue builds this from whatever config source it has (YAML for 2.0,
 * generator for 3.0).
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
