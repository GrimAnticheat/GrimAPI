package ac.grim.grimac.api.storage.query;

import org.jetbrains.annotations.ApiStatus;

/**
 * Marker for a typed, category-scoped query. Concrete subtypes live in {@link Queries}.
 * <p>
 * Not general predicate language — each Query subtype names a specific backend-friendly
 * access pattern that every backend can implement without a full query planner.
 *
 * @deprecated Replaced by {@link ac.grim.grimac.api.storage.kind.Operation} plus the
 * per-Kind operation menus on {@link ac.grim.grimac.api.storage.category.EventStreamCategory},
 * {@link ac.grim.grimac.api.storage.category.EntityCategory}, etc. The redesign moves
 * dispatch from "switch on Query record" to "execute Operation on the routed KindAdapter".
 * See {@code .docs/storage-redesign/01-data-kinds.md}.
 */
@ApiStatus.Experimental
@Deprecated(forRemoval = true, since = "phase0")
public interface Query<R> {}
