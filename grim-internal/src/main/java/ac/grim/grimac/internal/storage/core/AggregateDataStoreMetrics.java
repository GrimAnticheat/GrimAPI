package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.DataStoreMetrics;
import org.jetbrains.annotations.ApiStatus;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Aggregates per-category ring counters + adapter counters into the public
 * {@link DataStoreMetrics} view. Single read-only facade over a {@link RingRegistry}.
 */
@ApiStatus.Internal
public final class AggregateDataStoreMetrics implements DataStoreMetrics {

    private final RingRegistry rings;
    private final AtomicLong readLatencyMsEma = new AtomicLong();

    public AggregateDataStoreMetrics(RingRegistry rings) {
        this.rings = rings;
    }

    public void observeReadLatencyMs(long observed) {
        long prev = readLatencyMsEma.get();
        long next = prev == 0 ? observed : (prev * 7 + observed) / 8;
        readLatencyMsEma.lazySet(next);
    }

    @Override public long queuedCount() { return rings.queuedCountTotal(); }
    @Override public long submittedTotal() { return rings.submittedTotal(); }
    @Override public long droppedOnOverflowTotal() { return rings.droppedOnOverflowTotal(); }
    @Override public long droppedOnErrorTotal() { return rings.droppedOnErrorTotal(); }
    @Override public long writeBatchLatencyMsEma() { return rings.writeBatchLatencyMsEmaMax(); }
    @Override public long readLatencyMsEma() { return readLatencyMsEma.get(); }
}
