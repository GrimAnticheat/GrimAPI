package ac.grim.grimac.api.storage.backend;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

/**
 * Mutable registry of {@link BackendProvider}s keyed by {@link BackendProvider#id()}.
 * <p>
 * The platform exposes a shared instance through the plugin's public API so external
 * extensions can contribute their own storage engines (e.g. a Postgres, Redis, or
 * in-memory test backend) without forking the platform. A provider must be registered
 * before the data store is started; registrations made after startup have no effect
 * on the currently-built routing.
 * <p>
 * Implementations must be safe for concurrent access.
 */
@ApiStatus.Experimental
public interface BackendRegistry {

    /**
     * Registers {@code provider} under its {@link BackendProvider#id() id}, replacing any
     * previously-registered provider with the same id.
     *
     * @throws IllegalArgumentException if {@code provider.id()} is blank.
     */
    void register(@NotNull BackendProvider provider);

    /**
     * @return the provider registered under {@code id}, or {@code null} if none.
     */
    @Nullable BackendProvider lookup(@NotNull String id);

    /**
     * @return an immutable snapshot of currently-registered ids.
     */
    @NotNull Set<@NotNull String> registeredIds();
}
