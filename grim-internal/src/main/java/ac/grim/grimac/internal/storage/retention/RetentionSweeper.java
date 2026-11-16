package ac.grim.grimac.internal.storage.retention;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.RetentionRule;
import ac.grim.grimac.api.storage.query.Deletes;
import ac.grim.grimac.internal.storage.core.DataStoreImpl;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Deletes expired sessions and violations per {@link RetentionRule}.
 *
 * <p>Deleting is opt-in: the per-category rules only say how old is too old, and nothing
 * is deleted unless the host also passes {@code deletionEnabled = true} (Grim's
 * {@value #ENABLE_KEY} key, false when absent, because existing configs already carry
 * the bundled day values). Without it the sweeper logs one line and does nothing.
 *
 * <p>When deletion is enabled and at least one of those rules is enabled, constructing the sweeper schedules it:
 * the first sweep runs a few minutes after startup (with jitter, so servers sharing a
 * database do not all sweep at once) and then every {@link #DEFAULT_PERIOD_MS}. It
 * stops when the {@link DataStoreImpl} it sweeps closes, or on {@link #close()}.
 *
 * <p>Rows are deleted in batches of {@link #BATCH_SIZE}, so a large backlog (for example
 * the first sweep of a database that was never swept) never holds a table, or a
 * single-writer backend's writer thread, for long. A violation is old once it
 * {@code occurred_at} before the cutoff; a session once it both {@code started_at} and
 * was last active before it, so a session still in use is never removed. Deleting a
 * session does not delete its violations: those follow the violation rule.
 */
@ApiStatus.Internal
public final class RetentionSweeper implements AutoCloseable {

    /** The session columns that must all be older than the cutoff. */
    public static final List<String> SESSION_AGE_FIELDS = List.of("started_at", "last_activity");
    static final List<String> VIOLATION_AGE_FIELDS = List.of("occurred_at");

    /** The configuration key that turns deletion on, named in the startup log line. */
    public static final String ENABLE_KEY = "database.retention.enabled";

    static final int BATCH_SIZE = 5_000;
    static final long DEFAULT_INITIAL_DELAY_MS = TimeUnit.MINUTES.toMillis(5);
    static final long DEFAULT_PERIOD_MS = TimeUnit.HOURS.toMillis(6);
    private static final long BATCH_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);

    private final DataStore store;
    private final Map<Category<?>, RetentionRule> rules;
    private final Logger logger;
    private final @Nullable ScheduledExecutorService scheduler;
    private final boolean deletionEnabled;
    private volatile boolean closed;

    /** A sweeper that deletes nothing: retention left at its default, off. See {@link #RetentionSweeper(DataStore, Map, boolean, Logger)}. */
    public RetentionSweeper(DataStore store, Map<Category<?>, RetentionRule> rules, Logger logger) {
        this(store, rules, false, logger);
    }

    /**
     * Schedules the sweeper when {@code deletionEnabled} and at least one session or violation
     * rule is enabled; otherwise logs that retention is configured but off and does nothing.
     */
    public RetentionSweeper(DataStore store, Map<Category<?>, RetentionRule> rules, boolean deletionEnabled,
                            Logger logger) {
        this(store, rules, deletionEnabled, logger,
            DEFAULT_INITIAL_DELAY_MS + ThreadLocalRandom.current().nextLong(DEFAULT_INITIAL_DELAY_MS),
            DEFAULT_PERIOD_MS);
    }

    /** {@code periodMs <= 0} leaves scheduling to the caller. */
    RetentionSweeper(DataStore store, Map<Category<?>, RetentionRule> rules, boolean deletionEnabled, Logger logger,
                     long initialDelayMs, long periodMs) {
        this.store = store;
        this.rules = Map.copyOf(rules);
        this.logger = logger;
        this.deletionEnabled = deletionEnabled;
        if (!deletionEnabled) {
            this.scheduler = null;
            if (hasActiveRule()) {
                logger.info("[grim-datastore] History retention is configured but disabled; set " + ENABLE_KEY
                    + ": true to delete " + describeRules());
            }
        } else if (periodMs > 0 && hasActiveRule()) {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "grim-datastore-retention");
                t.setDaemon(true);
                return t;
            });
            this.scheduler = executor;
            executor.scheduleWithFixedDelay(this::scheduledSweep, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
            if (store instanceof DataStoreImpl impl) impl.onClose(this::close);
            logger.info("[grim-datastore] retention enabled: deleting " + describeRules() + "; first sweep in "
                + TimeUnit.MILLISECONDS.toMinutes(initialDelayMs) + " min, then every "
                + TimeUnit.MILLISECONDS.toHours(periodMs) + " h");
        } else {
            this.scheduler = null;
        }
    }

    /** True while a sweep is scheduled. */
    public boolean isScheduled() {
        return scheduler != null && !scheduler.isShutdown();
    }

    /** Stops the schedule. A sweep in progress stops after its current batch. */
    @Override
    public void close() {
        closed = true;
        if (scheduler != null) scheduler.shutdownNow();
    }

    private void scheduledSweep() {
        try {
            sweepOnce();
        } catch (RuntimeException e) {
            // A throwing task would cancel the schedule.
            logger.log(Level.WARNING, "[grim-datastore] retention sweep failed", e);
        }
    }

    /** Runs one sweep, unless deletion is off. Blocks until each category's delete completes. */
    public void sweepOnce() {
        if (!deletionEnabled) return;
        for (Map.Entry<Category<?>, RetentionRule> entry : rules.entrySet()) {
            Category<?> cat = entry.getKey();
            RetentionRule rule = entry.getValue();
            if (!active(cat, rule)) continue;
            if (stopped()) return;
            long cutoff = System.currentTimeMillis() - rule.maxAgeMs();
            try {
                long deleted = sweep(cat, rule, cutoff);
                if (deleted > 0) {
                    logger.info("[grim-datastore] retention deleted " + deleted + " " + cat.id()
                        + " row(s) older than " + rule.maxAgeDays() + " days");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                if (!closed) logger.log(Level.WARNING, "[grim-datastore] retention sweep interrupted");
                return;
            } catch (ExecutionException | TimeoutException | RuntimeException e) {
                if (stopped()) return;
                logger.log(Level.WARNING, "[grim-datastore] retention sweep failed for " + cat.id(), e);
            }
        }
    }

    /** Deletes in batches through the V2 route; falls back to one {@link DataStore#delete} without one. */
    private long sweep(Category<?> cat, RetentionRule rule, long cutoff)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<String> fields = cat == Categories.SESSION ? SESSION_AGE_FIELDS : VIOLATION_AGE_FIELDS;
        long total = 0;
        while (!stopped()) {
            Long deleted;
            try {
                deleted = store.execute(new DeleteOlderThanOp(cat, fields, cutoff, BATCH_SIZE))
                    .toCompletableFuture().get(BATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                if (total > 0 || !unsupported(e)) throw e;
                // No V2 adapter handles the batched delete (e.g. Redis): one plain delete instead.
                store.delete(cat, Deletes.olderThan(rule.maxAgeMs()))
                    .toCompletableFuture().get(BATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                return 0;
            }
            long n = deleted == null ? 0 : deleted;
            total += n;
            if (n < BATCH_SIZE) break;
        }
        return total;
    }

    private boolean stopped() {
        return closed || (store instanceof DataStoreImpl impl && impl.isClosed());
    }

    private boolean hasActiveRule() {
        for (Map.Entry<Category<?>, RetentionRule> entry : rules.entrySet()) {
            if (active(entry.getKey(), entry.getValue())) return true;
        }
        return false;
    }

    private static boolean active(Category<?> cat, RetentionRule rule) {
        // PlayerIdentity and Setting retention rules are parsed but not applied yet.
        return rule.enabled() && rule.maxAgeDays() > 0
            && (cat == Categories.SESSION || cat == Categories.VIOLATION);
    }

    private @NotNull String describeRules() {
        StringBuilder sb = new StringBuilder();
        for (Category<?> cat : List.of(Categories.SESSION, Categories.VIOLATION)) {
            RetentionRule rule = rules.get(cat);
            if (rule == null || !active(cat, rule)) continue;
            if (sb.length() > 0) sb.append(" and ");
            sb.append(cat == Categories.SESSION ? "sessions" : "violations")
                .append(" older than ").append(rule.maxAgeDays()).append(" days");
        }
        return sb.toString();
    }

    private static boolean unsupported(Throwable t) {
        for (int depth = 0; t != null && depth < 32; depth++, t = t.getCause()) {
            if (t instanceof UnsupportedOperationException) return true;
        }
        return false;
    }
}
