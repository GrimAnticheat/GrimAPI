package ac.grim.grimac.api.storage.query;

import org.jetbrains.annotations.ApiStatus;

/**
 * Marker for a typed, category-scoped query. Concrete subtypes live in {@link Queries}.
 * <p>
 * Not general predicate language — each Query subtype names a specific backend-friendly
 * access pattern that every backend can implement without a full query planner.
 */
@ApiStatus.Experimental
public interface Query<R> {}
