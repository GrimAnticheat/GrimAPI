package ac.grim.grimac.internal.storage.instance;

import ac.grim.grimac.api.storage.model.ServerInstanceRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Multi-server crash sweep — runs the "any session whose owning
 * instance is no longer in the live set gets closed_at = last_activity"
 * pass per {@code .docs/storage-redesign/07-crash-recovery.md}.
 *
 * <p>Intentionally abstract over the storage layer: callers wire in
 * three lambdas that handle the actual queries. This keeps the helper
 * usable from the legacy backend wiring (single-instance variant in
 * {@code DataStore.markCrashedSessions}) AND the v2 wiring once
 * Phase 1.4c is in place.
 *
 * <ul>
 *   <li>{@code liveInstances} — return the current set of live
 *       {@link ServerInstanceRecord#instanceId()} values (an Entity
 *       scan over {@code server_instances}, filtered by liveness).</li>
 *   <li>{@code orphanedSessions} — given the live set, return every
 *       session whose {@code instanceId} is NOT in the set AND whose
 *       {@code closedAtEpochMs == SessionRecord.OPEN}. Implementations
 *       choose between a single $nin query (Mongo) or two-step
 *       enumerate+filter (SQL).</li>
 *   <li>{@code stampClosedAt} — close one session (set its
 *       {@code closed_at} to its {@code last_activity}). Idempotent
 *       across concurrent sweepers.</li>
 * </ul>
 *
 * <p>Backstop: a slower stale-activity sweep can be run separately
 * via {@link #sweepStaleActivity}, which catches sessions whose
 * {@code instanceId} somehow doesn't match a registry row at all
 * (data corruption, manual DB edit, pre-registry rows). It uses a
 * configurable stale threshold (default 1 hour).
 */
@ApiStatus.Internal
public final class CrashSweep {

    private final @NotNull Logger logger;

    public CrashSweep(@NotNull Logger logger) {
        this.logger = logger;
    }

    /**
     * Run the instance-aware sweep. Returns the number of sessions
     * stamped closed_at on this pass. Idempotent across concurrent
     * sweepers — a session double-stamped with the same value is a
     * no-op.
     */
    public long sweepDeadInstances(
            @NotNull java.util.function.Supplier<Set<UUID>> liveInstances,
            @NotNull Function<Set<UUID>, Iterable<SessionRecord>> orphanedSessions,
            @NotNull LongConsumer recordCloseCount,
            @NotNull SessionCloseFn stampClosedAt) {
        try {
            Set<UUID> live = liveInstances.get();
            long count = 0;
            for (SessionRecord s : orphanedSessions.apply(live)) {
                if (s.isClosed()) continue; // already closed by a peer sweeper
                try {
                    if (stampClosedAt.close(s.sessionId(), s.lastActivityEpochMs())) count++;
                } catch (RuntimeException e) {
                    logger.log(Level.WARNING,
                        "crash sweep failed to close session " + s.sessionId(), e);
                }
            }
            recordCloseCount.accept(count);
            return count;
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "crash sweep query failed", e);
            return 0L;
        }
    }

    /**
     * Backstop sweep — finds sessions whose {@code closed_at IS OPEN}
     * AND whose {@code last_activity} is older than {@code stale},
     * regardless of registry state. Catches rows the instance-aware
     * sweep can't (no instanceId, never had one). Run on a slower
     * cadence (e.g. every 10 minutes) than the main sweep.
     */
    public long sweepStaleActivity(
            @NotNull Duration stale,
            @NotNull java.util.function.Supplier<Iterable<SessionRecord>> staleSessions,
            @NotNull SessionCloseFn stampClosedAt) {
        try {
            long staleCutoff = System.currentTimeMillis() - stale.toMillis();
            long count = 0;
            for (SessionRecord s : staleSessions.get()) {
                if (s.isClosed()) continue;
                if (s.lastActivityEpochMs() > staleCutoff) continue;
                try {
                    if (stampClosedAt.close(s.sessionId(), s.lastActivityEpochMs())) count++;
                } catch (RuntimeException e) {
                    logger.log(Level.WARNING,
                        "stale-activity sweep failed to close session " + s.sessionId(), e);
                }
            }
            return count;
        } catch (RuntimeException e) {
            // Matches sweepDeadInstances: a query-side failure inside a
            // fixed-rate scheduled executor would suppress all future
            // ticks per ScheduledExecutorService's contract. Log and
            // return 0; the next tick retries.
            logger.log(Level.WARNING, "stale-activity sweep query failed", e);
            return 0L;
        }
    }

    /**
     * Session close hook. Implementations issue the per-backend
     * update — Mongo {@code updateOne($set closed_at)}, SQL
     * {@code UPDATE ... SET closed_at = ?}. Idempotent: if
     * {@code closed_at} is already set, the implementation may either
     * no-op or overwrite (the value is deterministic from
     * {@code last_activity}).
     *
     * <p>Returns {@code true} when the row was actually updated by this
     * call (one row affected), {@code false} when a concurrent sweeper
     * had already closed it (zero rows affected). The {@code count}
     * returned by the sweep methods reflects actual closes, not
     * attempted closes — useful for honest metrics under concurrent
     * sweepers.
     */
    @FunctionalInterface
    public interface SessionCloseFn {
        boolean close(@NotNull UUID sessionId, long closedAtEpochMs);
    }
}
