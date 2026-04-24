package ac.grim.grimac.api.storage.backend;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * SPI for plugging a {@link Backend} into a DataStore.
 * <p>
 * Each implementation owns one backend id (e.g. {@code "sqlite"}, {@code "mysql"},
 * {@code "mongo"}). External addons register an instance with the
 * {@link BackendRegistry} obtained from the platform's public API; the storage
 * wiring then asks each registered provider to read its own settings and build
 * the backend.
 */
@ApiStatus.Experimental
public interface BackendProvider {

    /**
     * The stable string id this provider handles. Used as the key in routing
     * config ({@code routing.violation: <id>}) and in {@link BackendRegistry}.
     */
    @NotNull String id();

    /**
     * The concrete {@link BackendConfig} subtype this provider produces and
     * accepts. {@link #create(BackendConfig)} will be called with an instance
     * of this type.
     */
    @NotNull Class<? extends BackendConfig> configType();

    /**
     * Materialise this provider's {@link BackendConfig} from a read-only
     * source of settings. The platform opens the source (from a per-backend
     * file, an in-memory map, a database — the provider does not care) and
     * calls this method once per distinct backend id at startup.
     * <p>
     * Implementations should apply their own defaults via the default-valued
     * getters on {@link BackendConfigSource}. Throwing is reserved for invalid
     * combinations that no default recovers (e.g. an unknown enum value).
     */
    @NotNull BackendConfig readConfig(@NotNull BackendConfigSource source);

    /**
     * Construct a new, uninitialised {@link Backend} from the given config.
     * The caller invokes {@link Backend#init(BackendContext)} before use.
     */
    @NotNull Backend create(@NotNull BackendConfig config);
}
