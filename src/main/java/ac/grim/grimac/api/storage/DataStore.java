package ac.grim.grimac.api.storage;

import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;

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
    <R> void submit(Category<R> cat, R record);

    <R> CompletionStage<Page<R>> query(Category<R> cat, Query<R> query);

    <R> CompletionStage<Void> delete(Category<R> cat, DeleteCriteria criteria);

    CompletionStage<DeletionReport> forgetPlayer(UUID uuid);

    CompletionStage<Long> countViolationsInSession(UUID sessionId);

    DataStoreMetrics metrics();

    /**
     * Blocks until the MPSC queue drains or the timeout elapses. Anything left is dropped
     * with a final warn. Used by Layer 3 shutdown paths.
     */
    void flushAndClose(long drainTimeoutMs);
}
