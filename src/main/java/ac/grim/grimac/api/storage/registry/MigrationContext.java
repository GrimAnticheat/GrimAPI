package ac.grim.grimac.api.storage.registry;

import org.jetbrains.annotations.ApiStatus;

/**
 * Marker / placeholder for the per-backend migration context object passed to
 * {@link Migration#apply}. Concrete shape lands in Phase 1; for now this is a
 * sealed-by-convention interface so backends can supply their own implementation.
 */
@ApiStatus.Experimental
public interface MigrationContext {
}
