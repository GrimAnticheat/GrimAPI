package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.DataStoreMetrics;
import org.jetbrains.annotations.ApiStatus;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aggregates per-backend queue counters + per-backend writer-loop counters into the
 * public {@link DataStoreMetrics} view. A single read-only facade.
 */
@ApiStatus.Internal
public final class AggregateDataStoreMetrics implements DataStoreMetrics {

    private final List<BoundedMpscQueue<?>> queues;
    private final List<WriterLoop> loops;
    private final AtomicLong readLatencyMsEma = new AtomicLong();

    public AggregateDataStoreMetrics(List<BoundedMpscQueue<?>> queues, List<WriterLoop> loops) {
        this.queues = List.copyOf(queues);
        this.loops = List.copyOf(loops);
    }

    public void observeReadLatencyMs(long observed) {
        long prev = readLatencyMsEma.get();
        long next = prev == 0 ? observed : (prev * 7 + observed) / 8;
        readLatencyMsEma.lazySet(next);
    }

    @Override
    public long queuedCount() {
        long sum = 0;
        for (BoundedMpscQueue<?> q : queues) sum += q.size();
        return sum;
    }

    @Override
    public long submittedTotal() {
        long sum = 0;
        for (BoundedMpscQueue<?> q : queues) sum += q.submittedTotal();
        return sum;
    }

    @Override
    public long droppedOnOverflowTotal() {
        long sum = 0;
        for (BoundedMpscQueue<?> q : queues) sum += q.droppedTotal();
        return sum;
    }

    @Override
    public long droppedOnErrorTotal() {
        long sum = 0;
        for (WriterLoop l : loops) sum += l.droppedOnErrorTotal();
        return sum;
    }

    @Override
    public long writeBatchLatencyMsEma() {
        long max = 0;
        for (WriterLoop l : loops) max = Math.max(max, l.writeBatchLatencyMsEma());
        return max;
    }

    @Override
    public long readLatencyMsEma() {
        return readLatencyMsEma.get();
    }
}
