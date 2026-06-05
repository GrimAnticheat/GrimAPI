package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.event.PlayerIdentityEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.event.SettingEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.registry.StoreId;
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

    /**
     * Register a category's ring against the LEGACY {@link Backend} SPI.
     * Used for categories that don't have a v2 route installed —
     * writeHandler comes from {@code backend.eventHandlerFor(cat)}.
     */
    public <E> void register(@NotNull Category<E> cat, @NotNull Backend backend) throws BackendException {
        registerInternal(cat, backend.id(), backend, backend.eventHandlerFor(cat), false);
    }

    /**
     * Register a category's ring against the v2 {@link KindAdapter} SPI.
     * Used for categories that have a v2 route — writeHandler comes from
     * {@code adapter.writeHandler(storeId, kind, cat)} so writes land in
     * the v7 physical layout the v2 migrations produced. The legacy
     * {@link Backend} for this category (if any) is dormant for writes
     * once a v2 route is installed.
     *
     * <p>Uses raw types internally because the caller (DataStoreImpl)
     * sources from {@link V2Routes.Route} which exposes wildcard-captured
     * generics. Type safety is guaranteed at route-construction time by
     * {@link V2Routes.Builder#register}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E> void register(
            @NotNull Category<E> cat,
            @NotNull BackendV2 backend,
            @NotNull KindAdapter adapter,
            @NotNull StoreId storeId,
            @NotNull DataKind kind) throws BackendException {
        StorageEventHandler<E> handler = adapter.writeHandler(storeId, kind, cat);
        boolean singleWriter = backend.writerThreads(cat) <= 1
            && !backend.capabilities().contains(Capability.MULTI_WRITER);
        if (singleWriter) {
            // Wrap the handler to submit work to a shared single-thread
            // executor per backend. The Disruptor thread does the fast
            // submit; the actual I/O runs on the executor thread. No
            // locks — the executor IS the serialization.
            java.util.concurrent.ExecutorService exec = singleWriterExecutors.computeIfAbsent(
                backend.id(), id -> java.util.concurrent.Executors.newSingleThreadExecutor(
                    namedDaemon("grim-datastore-" + id + "-writer")));
            StorageEventHandler<E> original = handler;
            handler = (event, sequence, endOfBatch) -> {
                // Snapshot the event state before submitting — the
                // Disruptor reuses the event slot after onEvent returns.
                // For now, run synchronously on the executor to avoid
                // the snapshot complexity. The Disruptor thread blocks
                // briefly but there's only one consumer per ring anyway.
                try {
                    exec.submit(() -> {
                        try { original.onEvent(event, sequence, endOfBatch); }
                        catch (Exception e) { logger.log(Level.WARNING,
                            "[grim-datastore] single-writer handler failed", e); }
                    }).get(); // block until I/O completes
                } catch (Exception e) {
                    throw new ac.grim.grimac.api.storage.backend.BackendException(
                        "single-writer submit failed", e);
                }
            };
        }
        registerInternal(cat, backend.id(), backend, handler, false);
    }

    /**
     * Per-backend single-thread executors for backends where
     * writerThreads == 1. Each category's Disruptor handler submits
     * work to this executor instead of running it directly — the
     * executor IS the serialization. No locks needed.
     */
    private final Map<String, java.util.concurrent.ExecutorService> singleWriterExecutors = new LinkedHashMap<>();

    private <E> void registerInternal(@NotNull Category<E> cat,
                                      @NotNull String backendId,
                                      @NotNull Object backendRef,
                                      @NotNull StorageEventHandler<E> handler,
                                      boolean unused) throws BackendException {
        if (entries.containsKey(cat)) {
            throw new IllegalStateException("ring already registered for category " + cat.id());
        }
        DisruptorEventHandlerAdapter<E> adapter = new DisruptorEventHandlerAdapter<>(
                cat.id(), handler, cfg.warnRateMs(), logger);

        ThreadFactory tf = namedDaemon("grim-datastore-" + backendId + "-" + cat.id());
        Disruptor<E> disruptor = new Disruptor<>(
                cat.newEvent()::get,
                cfg.queueCapacity(),
                tf,
                ProducerType.MULTI,
                WaitStrategies.resolve(cfg.waitStrategy(), Math.max(1L, cfg.flushIntervalMs())));
        disruptor.handleEventsWith(adapter);
        RingBuffer<E> ring = disruptor.start();
        entries.put(cat, new Entry<>(cat, backendRef, disruptor, ring, adapter));
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
            // Category has no ring — either unrouted or skipped (e.g.
            // SETTING in v2-only mode before the KV kind is wired).
            // Silently drop rather than crash the write path.
            droppedOnOverflowTotal.incrementAndGet();
            return false;
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
     * Events stay live between publishes, so before handing the slot back to
     * the producer we clear state from the last tenant. The built-in event
     * types each expose {@code reset()}; event types declared by extensions
     * are trusted to overwrite every field.
     */
    @SuppressWarnings("unchecked")
    private static <E> E resetIfKnown(E event) {
        if (event instanceof ViolationEvent v) { v.reset(); return (E) v; }
        if (event instanceof SessionEvent s) { s.reset(); return (E) s; }
        if (event instanceof PlayerIdentityEvent p) { p.reset(); return (E) p; }
        if (event instanceof SettingEvent s) { s.reset(); return (E) s; }
        if (event instanceof ac.grim.grimac.api.storage.event.CheckCatalogEvent c) { c.reset(); return (E) c; }
        if (event instanceof ac.grim.grimac.api.storage.event.ServerInstanceEvent i) { i.reset(); return (E) i; }
        if (event instanceof ac.grim.grimac.api.storage.event.ServerStartupEvent s) { s.reset(); return (E) s; }
        if (event instanceof ac.grim.grimac.api.storage.kind.CounterEvent<?> co) { co.clear(); return (E) co; }
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
        // Shut down single-writer executors after all rings are drained
        for (java.util.concurrent.ExecutorService exec : singleWriterExecutors.values()) {
            exec.shutdown();
            try { exec.awaitTermination(2, TimeUnit.SECONDS); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); exec.shutdownNow(); }
        }
        singleWriterExecutors.clear();
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

    /**
     * @return the legacy {@link Backend} bound to this category's ring,
     *         or null if the category isn't routed OR is bound to a v2
     *         {@link BackendV2} (use {@link #backendV2For} for v2).
     *         Held for diagnostic / inspection use only — the write path
     *         dispatches through the ring, not this accessor.
     */
    public boolean hasRing(@NotNull Category<?> cat) {
        return entries.containsKey(cat);
    }

    public @Nullable Backend backendFor(@NotNull Category<?> cat) {
        Entry<?> e = entries.get(cat);
        return (e != null && e.backendRef instanceof Backend b) ? b : null;
    }

    /**
     * @return the {@link BackendV2} bound to this category's ring, or
     *         null if the category isn't routed OR is bound to a legacy
     *         {@link Backend}. Diagnostic only — write path doesn't use
     *         this.
     */
    public @Nullable BackendV2 backendV2For(@NotNull Category<?> cat) {
        Entry<?> e = entries.get(cat);
        return (e != null && e.backendRef instanceof BackendV2 b) ? b : null;
    }

    private static ThreadFactory namedDaemon(String prefix) {
        AtomicInteger seq = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * {@code backendRef} is held as Object so a single Entry record
     * can store either a legacy {@link Backend} or a v2 {@link BackendV2}.
     * The accessors {@link RingRegistry#backendFor} and
     * {@link RingRegistry#backendV2For} pattern-match to retrieve the
     * typed reference. Currently held for diagnostics only; the write
     * path dispatches through {@code disruptor}/{@code ring} which
     * captures the writeHandler at registration time.
     */
    private record Entry<E>(
            Category<E> category,
            Object backendRef,
            Disruptor<E> disruptor,
            RingBuffer<E> ring,
            DisruptorEventHandlerAdapter<E> adapter) {}
}
