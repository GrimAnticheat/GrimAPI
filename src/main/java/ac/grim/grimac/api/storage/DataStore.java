package ac.grim.grimac.api.storage;

import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Top-level storage facade. Built from a {@code DataStoreConfig} by Layer 2 and
 * consumed by Layer 3 platform glue and (in future) extension sandbox handles.
 */
@ApiStatus.Experimental
public interface DataStore {

    /**
     * Non-async write. Most callers use {@code ViolationSink.record} or category-specific
     * services instead; direct submit is for less-hot-path writers (e.g. session updates,
     * identity upserts) where the MPSC ceremony is overkill.
     */
    <R> void submit(@NotNull Category<R> cat, @NotNull R record);

    @NotNull <R> CompletionStage<Page<R>> query(@NotNull Category<R> cat, @NotNull Query<R> query);

    @NotNull <R> CompletionStage<Void> delete(@NotNull Category<R> cat, @NotNull DeleteCriteria criteria);

    @NotNull CompletionStage<DeletionReport> forgetPlayer(@NotNull UUID uuid);

    @NotNull CompletionStage<Long> countViolationsInSession(@NotNull UUID sessionId);

    @NotNull DataStoreMetrics metrics();

    /**
     * Blocks until the MPSC queue drains or the timeout elapses. Anything left is dropped
     * with a final warn. Used by Layer 3 shutdown paths.
     */
    void flushAndClose(long drainTimeoutMs);
}
