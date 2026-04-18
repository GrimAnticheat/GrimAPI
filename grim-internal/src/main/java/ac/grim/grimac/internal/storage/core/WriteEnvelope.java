package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.category.Category;
import org.jetbrains.annotations.ApiStatus;

/**
 * Envelope placed on the MPSC queue. Category is erased to {@code Category<?>} here so
 * the queue can be heterogeneous; the WriterLoop recovers the type parameter when
 * dispatching to {@link ac.grim.grimac.api.storage.backend.Backend#writeBatch}.
 */
@ApiStatus.Internal
public record WriteEnvelope(Category<?> category, Object record) {

    public WriteEnvelope {
        if (category == null) throw new IllegalArgumentException("category");
        if (record == null) throw new IllegalArgumentException("record");
    }
}
