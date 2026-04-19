package ac.grim.grimac.api.storage.backend;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * SPI for constructing a {@link Backend} from its {@link BackendConfig}. External
 * extensions implement this interface and register the instance with the
 * {@link BackendRegistry} obtained from the platform's public API so the storage
 * layer can build their backend on startup.
 * <p>
 * Each backend id maps to exactly one provider in a given registry.
 */
@ApiStatus.Experimental
public interface BackendProvider {

    /**
     * The stable string id this provider handles (e.g. {@code "sqlite"}, {@code "mysql"}).
     * Used as the key in configuration and in the {@link BackendRegistry}.
     */
    @NotNull String id();

    /**
     * The concrete {@link BackendConfig} subtype this provider accepts. Callers should
     * match the config passed to {@link #create(BackendConfig)} against this type.
     */
    @NotNull Class<? extends BackendConfig> configType();

    /**
     * Constructs a new, uninitialised {@link Backend}. The caller is expected to invoke
     * {@link Backend#init(BackendContext)} before use.
     */
    @NotNull Backend create(@NotNull BackendConfig config);
}
