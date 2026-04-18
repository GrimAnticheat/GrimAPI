package ac.grim.grimac.internal.storage.core;

import org.jetbrains.annotations.ApiStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded multi-producer single-consumer queue with drop-on-overflow semantics.
 * <p>
 * The producer side is {@link #offer}: non-blocking, returns {@code false} on a full
 * queue and increments the drop counter. The consumer side is
 * {@link #drainUpTo}: blocking-with-timeout followed by non-blocking drain up to a
 * batch limit.
 * <p>
 * Backed by {@link ArrayBlockingQueue}. "MPSC" here describes how we use it (one
 * WriterLoop thread consumes), not a lock-free implementation — JDK's ABQ is plenty
 * fast for phase-1 workloads and keeps semantics simple.
 */
@ApiStatus.Internal
public final class BoundedMpscQueue<T> {

    private final ArrayBlockingQueue<T> queue;
    private final AtomicLong submittedTotal = new AtomicLong();
    private final AtomicLong droppedTotal = new AtomicLong();

    public BoundedMpscQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /** Non-blocking offer. Returns false if the queue was full (record dropped). */
    public boolean offer(T item) {
        if (queue.offer(item)) {
            submittedTotal.incrementAndGet();
            return true;
        }
        droppedTotal.incrementAndGet();
        return false;
    }

    /**
     * Block up to {@code timeoutMs} for the first element, then non-blocking drain up to
     * {@code batchSize} total. Returns an empty list if no element arrived in time.
     */
    public List<T> drainUpTo(int batchSize, long timeoutMs) throws InterruptedException {
        List<T> out = new ArrayList<>(batchSize);
        T first = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (first == null) return out;
        out.add(first);
        queue.drainTo(out, batchSize - 1);
        return out;
    }

    public int size() {
        return queue.size();
    }

    public int capacity() {
        return queue.remainingCapacity() + queue.size();
    }

    public long submittedTotal() {
        return submittedTotal.get();
    }

    public long droppedTotal() {
        return droppedTotal.get();
    }
}
