package ac.grim.grimac.internal.storage.instance;

import ac.grim.grimac.api.storage.event.ServerStartupEvent;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Periodically publishes a {@link ServerStartupEvent} carrying this
 * JVM startup's identity and a fresh {@code lastHeartbeatEpochMs}. Underpins
 * the multi-server crash sweep per
 * {@code .docs/storage-redesign/07-crash-recovery.md}.
 *
 * <p>The scheduler is intentionally decoupled from any concrete write
 * path: callers pass a {@link Consumer Consumer&lt;ServerStartupEvent&gt;}
 * that publishes the event into whatever ring / category the
 * application has wired up. This keeps the helper usable from both the
 * legacy and v2 wiring paths without dragging the storage-routing
 * machinery in here.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #start()} schedules a heartbeat every {@code interval},
 *       fires the first one immediately, and returns. Idempotent — a
 *       second call is a no-op while running.</li>
 *   <li>Each tick allocates a fresh {@link ServerStartupEvent}, fills
 *       it with the current identity + system clock, and invokes the
 *       publish callback. The callback is expected to be cheap (a
 *       single ring publish); errors are logged and swallowed so a
 *       transient publish failure doesn't kill the scheduler. Per-tick
 *       allocation (rather than a reused slot) is intentional — see
 *       {@link #tick()}.</li>
 *   <li>{@link #markDrained()} flips the close reason — the next
 *       heartbeat publishes a graceful close hint.</li>
 *   <li>{@link #stop()} cancels the schedule and shuts down the
 *       executor. Does NOT publish a final heartbeat — callers that
 *       want a graceful "I'm gone" signal should
 *       {@link #markDrained()}, sleep one interval, then {@link #stop()},
 *       or call {@link #publishDrainImmediate()} which forces one final
 *       drain-flagged publish synchronously.</li>
 * </ol>
 *
 * <p>Thread model: a single daemon thread owns the schedule; the publish
 * callback is invoked on that thread. Producers must handle that
 * (no UI work, no blocking I/O without timeouts).
 */
@ApiStatus.Internal
public final class HeartbeatScheduler {

    private final @NotNull UUID startupId;
    private final @NotNull UUID instanceId;
    private final @NotNull String serverName;
    private final long startedEpochMs;
    private final @Nullable String hostname;
    private final @Nullable String grimVersion;
    private final @Nullable String serverVersionString;
    private final @NotNull Supplier<byte @Nullable []> verboseManifest;
    private final @NotNull Duration interval;
    private final @NotNull Consumer<ServerStartupEvent> publish;
    private final @NotNull Logger logger;

    private final Object lifecycleLock = new Object();
    private @Nullable ScheduledExecutorService executor;
    private @Nullable ScheduledFuture<?> task;
    private volatile @Nullable Thread schedulerThread;
    private volatile long closedAtEpochMs;
    private volatile @Nullable String closeReason;

    public HeartbeatScheduler(
            @NotNull UUID startupId,
            @NotNull UUID instanceId,
            @NotNull String serverName,
            long startedEpochMs,
            @Nullable String hostname,
            @Nullable String grimVersion,
            @Nullable String serverVersionString,
            @NotNull Duration interval,
            @NotNull Consumer<ServerStartupEvent> publish,
            @NotNull Logger logger) {
        this(startupId, instanceId, serverName, startedEpochMs, hostname,
                grimVersion, serverVersionString, (byte[]) null, interval, publish, logger);
    }

    public HeartbeatScheduler(
            @NotNull UUID startupId,
            @NotNull UUID instanceId,
            @NotNull String serverName,
            long startedEpochMs,
            @Nullable String hostname,
            @Nullable String grimVersion,
            @Nullable String serverVersionString,
            @Nullable byte[] verboseManifest,
            @NotNull Duration interval,
            @NotNull Consumer<ServerStartupEvent> publish,
            @NotNull Logger logger) {
        this(startupId, instanceId, serverName, startedEpochMs, hostname,
                grimVersion, serverVersionString, () -> verboseManifest, interval, publish, logger);
    }

    /**
     * Manifest-supplier variant: the verbose manifest is re-read on every
     * heartbeat so per-check codec versions interned after startup (templates
     * register lazily on first flag) reach the durable startup row.
     */
    public HeartbeatScheduler(
            @NotNull UUID startupId,
            @NotNull UUID instanceId,
            @NotNull String serverName,
            long startedEpochMs,
            @Nullable String hostname,
            @Nullable String grimVersion,
            @Nullable String serverVersionString,
            @NotNull Supplier<byte @Nullable []> verboseManifest,
            @NotNull Duration interval,
            @NotNull Consumer<ServerStartupEvent> publish,
            @NotNull Logger logger) {
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive, got " + interval);
        }
        this.startupId = startupId;
        this.instanceId = instanceId;
        this.serverName = serverName;
        this.startedEpochMs = startedEpochMs;
        this.hostname = hostname;
        this.grimVersion = grimVersion;
        this.serverVersionString = serverVersionString;
        this.verboseManifest = verboseManifest;
        this.interval = interval;
        this.publish = publish;
        this.logger = logger;
    }

    /** Begin periodic heartbeats. Idempotent. Fires the first heartbeat immediately. */
    public void start() {
        synchronized (lifecycleLock) {
            if (executor != null) return;
            executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "grim-storage-heartbeat-" + serverName);
                t.setDaemon(true);
                schedulerThread = t;
                return t;
            });
            long periodMs = interval.toMillis();
            task = executor.scheduleAtFixedRate(this::tick, 0, periodMs, TimeUnit.MILLISECONDS);
        }
    }

    /** Mark this startup as gracefully closed. Next heartbeat carries the close marker. */
    public void markDrained() {
        closedAtEpochMs = System.currentTimeMillis();
        closeReason = "graceful";
    }

    /**
     * Publish one final heartbeat with the graceful close marker on the
     * calling thread, regardless of schedule. Use this from a graceful
     * shutdown hook where you can't afford to wait an interval for the
     * normal tick.
     */
    public void publishDrainImmediate() {
        markDrained();
        tick();
    }

    /**
     * Publish one off-schedule heartbeat as soon as possible on the
     * scheduler thread. Used when the verbose manifest changes so rows
     * written immediately after a new template interned stay decodable.
     * No-op when the scheduler is not running.
     */
    public void publishNow() {
        synchronized (lifecycleLock) {
            if (executor != null) {
                executor.execute(this::tick);
            }
        }
    }

    /**
     * Publish one off-schedule heartbeat and wait until the publish callback
     * returns. Used for metadata barriers where a caller has just persisted a
     * verbose schema and needs the startup manifest row to reflect it before
     * the related violation row is submitted.
     */
    public void publishNowAndWait() {
        ScheduledExecutorService current;
        synchronized (lifecycleLock) {
            current = executor;
        }
        if (current == null) return;
        if (Thread.currentThread() == schedulerThread) {
            tick();
            return;
        }
        Future<?> future = current.submit(this::tick);
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.log(Level.WARNING, "heartbeat publish failed for startup " + startupId, e.getCause());
        }
    }

    /**
     * Cancel the schedule. Does not publish a final heartbeat — pair
     * with {@link #publishDrainImmediate()} for graceful shutdown.
     */
    public void stop() {
        synchronized (lifecycleLock) {
            if (task != null) {
                task.cancel(false);
                task = null;
            }
            if (executor != null) {
                executor.shutdownNow();
                executor = null;
                schedulerThread = null;
            }
        }
    }

    private void tick() {
        try {
            // Allocate a fresh event per tick rather than reusing a
            // shared slot. Two reasons: (1) publishDrainImmediate()
            // can run on the caller thread concurrently with the
            // scheduled thread's tick — a shared slot would race
            // through reset/mutate/publish; (2) consumers that
            // accidentally retain the event reference past the
            // publish callback would observe field mutation on the
            // next tick. Per-tick allocation is cheap at heartbeat
            // cadence (one event per ~30s).
            ServerStartupEvent event = new ServerStartupEvent()
                .startupId(startupId)
                .instanceId(instanceId)
                .serverName(serverName)
                .startedEpochMs(startedEpochMs)
                .lastHeartbeatEpochMs(System.currentTimeMillis())
                .hostname(hostname)
                .grimVersion(grimVersion)
                .serverVersionString(serverVersionString)
                .verboseManifest(verboseManifest.get())
                .closedAtEpochMs(closedAtEpochMs)
                .closeReason(closeReason);
            publish.accept(event);
        } catch (RuntimeException e) {
            // Swallow so a transient publish failure doesn't kill the
            // scheduler. The next tick will retry; if the problem
            // persists, the TTL on this row evicts the instance and
            // other servers' sweepers handle its sessions.
            logger.log(Level.WARNING, "heartbeat publish failed for startup " + startupId, e);
        }
    }
}
