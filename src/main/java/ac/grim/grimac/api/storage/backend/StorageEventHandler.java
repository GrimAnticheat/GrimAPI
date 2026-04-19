package ac.grim.grimac.api.storage.backend;

import ac.grim.grimac.api.storage.category.Category;
import org.jetbrains.annotations.ApiStatus;

/**
 * Hot-path write consumer for a single {@link Category}. Mirrors the shape of an
 * LMAX Disruptor {@code EventHandler} without leaking {@code com.lmax.*} types into
 * the public API — Layer 2 adapts between this interface and the shaded Disruptor
 * types so plugins (and addons) can target a stable contract even when packaging
 * relocates Disruptor to dodge Log4j's bundled copy.
 * <p>
 * {@code endOfBatch} is a hint to commit: batch work inside the handler (prepared
 * statements, in-memory buffers) and flush when the flag is {@code true}. Handlers
 * run on a single dedicated thread per category ring; no external synchronization
 * is required.
 */
@ApiStatus.Experimental
@FunctionalInterface
public interface StorageEventHandler<E> {

    /**
     * Invoked for each published event in sequence. The event instance is owned by
     * the ring — do not retain references past the call; copy fields you need.
     *
     * @param event      the mutable event slot for this sequence
     * @param sequence   ring sequence number (monotonic, used for diagnostics)
     * @param endOfBatch {@code true} when the Disruptor has no further events
     *                   currently available — signal to commit pending work
     */
    void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException;
}
