package ac.grim.grimac.api.storage.kind;

import ac.grim.grimac.api.storage.category.Category;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Read or mutation operation against a registered store. Constructed by the
 * per-Kind operation menu (e.g. {@code stream.page(...)}), dispatched by the
 * {@code DataStore} to the routed backend's adapter.
 * <p>
 * Operations are immutable value records — loggable, replayable, queueable.
 * Concrete records per Kind live in
 * {@code ac.grim.grimac.api.storage.kind.ops}.
 *
 * @param <R> the result type
 */
@ApiStatus.Experimental
public interface Operation<R> {

    /** Category this operation targets. Routes to a backend via the active config. */
    @NotNull Category<?> category();
}
