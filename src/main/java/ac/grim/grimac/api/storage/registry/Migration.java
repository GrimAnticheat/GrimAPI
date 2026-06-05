package ac.grim.grimac.api.storage.registry;

import ac.grim.grimac.api.storage.kind.DataKind;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * A single forward-only schema migration step for a registered store.
 * Run by the {@link StoreRegistry} when an on-disk store's version is below
 * the current declared version of its Kind.
 *
 * @param <K> the {@link DataKind} type this migration applies to
 */
@ApiStatus.Experimental
public interface Migration<K extends DataKind<?, ?>> {

    int fromVersion();

    int toVersion();

    /**
     * Apply this migration step to the store identified by {@code id} on
     * whichever backend currently hosts it. Implementations read the existing
     * data via {@code ctx} and write the upgraded shape back through the same
     * context — no direct backend access.
     */
    void apply(@NotNull MigrationContext ctx, @NotNull StoreId id, @NotNull K kind) throws Exception;
}
