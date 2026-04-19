package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One Disruptor ring per routed {@link Category}. Each ring is a MultiProducer
 * buffer of pre-allocated mutable events with a single {@link BatchEventProcessor}
 * that dispatches to the backend's {@link StorageEventHandler}.
 * <p>
 * Per-category rings (rather than one ring per backend) keep the SPI ready for
 * Phase 3+ extension-declared categories — the consumer shape does not need to
 * grow a {@code switch (category)} as new categories come online.
 */
@ApiStatus.Internal
public final class RingRegistry {

    private final Map<Category<?>, Entry<?>> entries = new LinkedHashMap<>();
    private final WritePathConfig cfg;
    private final Logger logger;
    private final AtomicLong submittedTotal = new AtomicLong();
    private final AtomicLong droppedOnOverflowTotal = new AtomicLong();

    public RingRegistry(@NotNull WritePathConfig cfg, @NotNull Logger logger) {
        this.cfg = cfg;
        this.logger = logger;
    }

    public <E> void register(@NotNull Category<E> cat, @NotNull Backend backend) throws BackendException {
        if (entries.containsKey(cat)) {
            throw new IllegalStateException("ring already registered for category " + cat.id());
        }
        StorageEventHandler<E> handler = backend.eventHandlerFor(cat);
        DisruptorEventHandlerAdapter<E> adapter = new DisruptorEventHandlerAdapter<>(
                cat.id(), handler, cfg.warnRateMs(), logger);

        ThreadFactory tf = namedDaemon("grim-datastore-" + backend.id() + "-" + cat.id());
        Disruptor<E> disruptor = new Disruptor<>(
                cat.newEvent()::get,
                cfg.queueCapacity(),
                tf,
                ProducerType.MULTI,
                WaitStrategies.resolve(cfg.waitStrategy(), Math.max(1L, cfg.flushIntervalMs())));
        disruptor.handleEventsWith(adapter);
        RingBuffer<E> ring = disruptor.start();
        entries.put(cat, new Entry<>(cat, backend, disruptor, ring, adapter));
    }

    /**
     * Claim a slot on the category's ring and pass it to {@code configurer} for
     * population. Drops and accounts on full ring; never blocks.
     * <p>
     * Uses Disruptor's {@code EventTranslatorOneArg} form to pass the configurer
     * through without capturing any local state — the translator instance is a
     * stateless JIT-cacheable singleton.
     */
    public <E> boolean submit(@NotNull Category<E> cat, @NotNull java.util.function.Consumer<E> configurer) {
        @SuppressWarnings("unchecked")
        Entry<E> e = (Entry<E>) entries.get(cat);
        if (e == null) {
            throw new IllegalArgumentException("no ring registered for category " + cat.id());
        }
        boolean ok = e.ring.tryPublishEvent(PUBLISH_TRANSLATOR, configurer);
        if (ok) {
            submittedTotal.incrementAndGet();
        } else {
            droppedOnOverflowTotal.incrementAndGet();
        }
        return ok;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final EventTranslatorOneArg PUBLISH_TRANSLATOR = (event, sequence, cfg) -> {
        Object slot = resetIfKnown(event);
        ((java.util.function.Consumer) cfg).accept(slot);
    };

    /**
     * Events stay live between publishes, so before handing the slot back to the
     * producer we clear state from the last tenant. Known Layer 1 event types
     * carry {@code reset()}; unknown types (extension-declared categories) are
     * trusted to overwrite every field.
     */
    @SuppressWarnings("unchecked")
    private static <E> E resetIfKnown(E event) {
        if (event instanceof ac.grim.grimac.api.storage.event.ViolationEvent v) { v.reset(); return (E) v; }
        if (event instanceof ac.grim.grimac.api.storage.event.SessionEvent s) { s.reset(); return (E) s; }
        if (event instanceof ac.grim.grimac.api.storage.event.PlayerIdentityEvent p) { p.reset(); return (E) p; }
        if (event instanceof ac.grim.grimac.api.storage.event.SettingEvent s) { s.reset(); return (E) s; }
        return event;
    }

    public int queuedCountFor(@NotNull Category<?> cat) {
        Entry<?> e = entries.get(cat);
        return e == null ? 0 : queuedCount(e);
    }

    public int queuedCountTotal() {
        int sum = 0;
        for (Entry<?> e : entries.values()) sum += queuedCount(e);
        return sum;
    }

    private static int queuedCount(Entry<?> e) {
        return (int) (e.ring.getBufferSize() - e.ring.remainingCapacity());
    }

    public long submittedTotal() { return submittedTotal.get(); }
    public long droppedOnOverflowTotal() { return droppedOnOverflowTotal.get(); }

    public long droppedOnErrorTotal() {
        long sum = 0;
        for (Entry<?> e : entries.values()) sum += e.adapter.droppedOnErrorTotal();
        return sum;
    }

    public long writeBatchLatencyMsEmaMax() {
        long max = 0;
        for (Entry<?> e : entries.values()) max = Math.max(max, e.adapter.batchCommitLatencyMsEma());
        return max;
    }

    /**
     * Halt all rings, waiting up to {@code drainTimeoutMs} per ring. Returns the
     * total count of events still pending when the timeout elapsed.
     */
    public long shutdown(long drainTimeoutMs) {
        long leftover = 0;
        for (Entry<?> e : entries.values()) leftover += shutdownOne(e, drainTimeoutMs);
        return leftover;
    }

    private long shutdownOne(Entry<?> e, long drainTimeoutMs) {
        try {
            e.disruptor.shutdown(drainTimeoutMs, TimeUnit.MILLISECONDS);
            return 0L;
        } catch (com.lmax.disruptor.TimeoutException te) {
            long leftover = queuedCount(e);
            logger.log(Level.WARNING,
                    "[grim-datastore] ring " + e.category.id() + " shutdown timed out with "
                            + leftover + " events unwritten");
            try {
                e.disruptor.halt();
            } catch (RuntimeException ignore) {}
            return leftover;
        }
    }

    public @Nullable Backend backendFor(@NotNull Category<?> cat) {
        Entry<?> e = entries.get(cat);
        return e == null ? null : e.backend;
    }

    private static ThreadFactory namedDaemon(String prefix) {
        AtomicInteger seq = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    private record Entry<E>(
            Category<E> category,
            Backend backend,
            Disruptor<E> disruptor,
            RingBuffer<E> ring,
            DisruptorEventHandlerAdapter<E> adapter) {}
}
