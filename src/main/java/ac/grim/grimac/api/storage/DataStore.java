package ac.grim.grimac.api.storage;

import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Top-level storage facade. Built from a {@code DataStoreConfig} by Layer 2 and
 * consumed by Layer 3 platform glue and (in future) extension sandbox handles.
 */
@ApiStatus.Experimental
public interface DataStore {

    /**
     * Allocation-free hot-path write. The caller receives a pre-allocated mutable
     * event slot from the ring and populates its fields inside {@code configurer};
     * the ring publishes the slot as soon as the configurer returns. Do not retain
     * a reference to the event past the configurer's return — the slot is recycled
     * for the next publisher.
     * <p>
     * On a full ring the record is dropped and counted; producers never block.
     */
    <E> void submit(@NotNull Category<E> cat, @NotNull Consumer<E> configurer);

    /**
     * Asynchronous read. The {@code cat} argument is the routing key for the
     * storage engine; the record type {@code R} comes from the {@link Query}.
     */
    @NotNull <R> CompletionStage<Page<R>> query(@NotNull Category<?> cat, @NotNull Query<R> query);

    @NotNull <E> CompletionStage<Void> delete(@NotNull Category<E> cat, @NotNull DeleteCriteria criteria);

    @NotNull CompletionStage<DeletionReport> forgetPlayer(@NotNull UUID uuid);

    @NotNull CompletionStage<Long> countViolationsInSession(@NotNull UUID sessionId);

    @NotNull CompletionStage<Long> countUniqueChecksInSession(@NotNull UUID sessionId);

    @NotNull CompletionStage<Long> countSessionsByPlayer(@NotNull UUID player);

    @NotNull DataStoreMetrics metrics();

    /**
     * Blocks until each category's ring drains or the timeout elapses. Anything left
     * is dropped with a final warn. Used by Layer 3 shutdown paths.
     */
    void flushAndClose(long drainTimeoutMs);
}
