package ac.grim.grimac.internal.storage.migrate;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackend;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import ac.grim.grimac.internal.storage.checks.StableKeyMapping;
import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Legacy v0 → v1 migrator. Reads the old {@code grim_history_*} tables in ascending
 * {@code (player_uuid, created_at, id)} order, feeds them through
 * {@link SessionReconstructor} to bucket by time-gap, and writes v1 records directly
 * through {@link SqliteBackend#writeBatch} (bypassing the async WriterLoop — this runs
 * synchronously at startup before accept-players per §13).
 * <p>
 * Resumable via the {@code grim_migration_state} singleton row on the v1 store: the
 * last-migrated v0 violation id is updated after each batch commit. Restart picks up
 * where the crash left off.
 */
@ApiStatus.Internal
public final class LegacyMigrator {

    public record Result(int sessionsWritten, int violationsWritten, long elapsedMs, boolean resumed) {}

    private final V0Reader v0;
    private final SqliteBackend v1;
    private final CheckRegistry checks;
    private final long sessionGapMs;
    private final Logger logger;
    private final int chunkSize;

    public LegacyMigrator(V0Reader v0, SqliteBackend v1, CheckRegistry checks, long sessionGapMs, Logger logger) {
        this(v0, v1, checks, sessionGapMs, logger, 1000);
    }

    public LegacyMigrator(V0Reader v0, SqliteBackend v1, CheckRegistry checks, long sessionGapMs,
                          Logger logger, int chunkSize) {
        this.v0 = v0;
        this.v1 = v1;
        this.checks = checks;
        this.sessionGapMs = sessionGapMs;
        this.logger = logger;
        this.chunkSize = chunkSize;
    }

    public Result run(LongConsumer progress) throws BackendException {
        long start = System.currentTimeMillis();
        if (!v0.isLegacyStorePresent()) {
            logger.info("[grim-datastore] no legacy v0 store found — nothing to migrate");
            return new Result(0, 0, 0, false);
        }

        long totalLegacyViolations;
        try {
            totalLegacyViolations = v0.maxViolationId();
        } catch (SQLException e) {
            throw new BackendException("legacy pre-scan failed", e);
        }
        if (totalLegacyViolations == 0) {
            logger.info("[grim-datastore] legacy v0 store is empty; nothing to migrate");
            return new Result(0, 0, System.currentTimeMillis() - start, false);
        }

        MigrationState state = readState();
        boolean resumed = state.lastMigratedViolationId > 0;
        if (resumed) {
            logger.info("[grim-datastore] resuming legacy migration from v0 id " + state.lastMigratedViolationId);
        } else {
            logger.info("[grim-datastore] starting legacy migration (v0 max id " + totalLegacyViolations + ")");
            writeState(0, "IN_PROGRESS", System.currentTimeMillis(), 0);
        }

        try {
            SessionReconstructor.LookupLens lens = loadLookups();
            SessionReconstructor.CheckIdResolver resolver = (legacyName) -> {
                String stableKey = StableKeyMapping.stableKeyFor(legacyName)
                        .orElseGet(() -> StableKeyMapping.legacyFallback(legacyName));
                return checks.intern(stableKey, legacyName);
            };

            AtomicLong sessionsEmitted = new AtomicLong();
            AtomicLong violationsEmitted = new AtomicLong();
            AtomicLong lastViolationLegacyId = new AtomicLong(state.lastMigratedViolationId);

            // PlayerIdentity backfill: track seen UUIDs and their min/max timestamps.
            Map<UUID, long[]> identityAcc = new LinkedHashMap<>();

            SessionReconstructor.Emit emit = (session, violations) -> {
                try {
                    // Write session first, then its violations.
                    v1.writeBatch(Categories.SESSION, List.of(session));
                    if (!violations.isEmpty()) {
                        List<ViolationRecord> rows = new ArrayList<>(violations.size());
                        for (SessionReconstructor.ReconstructedViolation v : violations) {
                            rows.add(new ViolationRecord(
                                    0,
                                    v.sessionId(),
                                    v.playerUuid(),
                                    v.checkId(),
                                    v.vl(),
                                    v.occurredEpochMs(),
                                    v.verbose(),
                                    VerboseFormat.TEXT));
                            long legacyId = v.legacyId();
                            if (legacyId > lastViolationLegacyId.get()) lastViolationLegacyId.set(legacyId);
                        }
                        v1.writeBatch(Categories.VIOLATION, rows);
                    }
                    sessionsEmitted.incrementAndGet();
                    violationsEmitted.addAndGet(violations.size());
                    // Checkpoint after each session commit.
                    writeState(lastViolationLegacyId.get(), "IN_PROGRESS",
                            state.startedAt > 0 ? state.startedAt : System.currentTimeMillis(), 0);
                    progress.accept(violationsEmitted.get());
                    // Track identity window
                    long firstSeen = session.startedEpochMs();
                    long lastSeen = session.lastActivityEpochMs();
                    identityAcc.compute(session.playerUuid(), (k, curr) -> {
                        if (curr == null) return new long[]{firstSeen, lastSeen};
                        return new long[]{Math.min(curr[0], firstSeen), Math.max(curr[1], lastSeen)};
                    });
                } catch (BackendException e) {
                    throw new RuntimeException("failed to write reconstructed session", e);
                }
            };

            SessionReconstructor reconstructor = new SessionReconstructor(sessionGapMs, lens, resolver, emit);

            long afterId = state.lastMigratedViolationId;
            while (true) {
                List<V0Reader.V0Violation> chunk = v0.readChunk(afterId, chunkSize);
                if (chunk.isEmpty()) break;
                for (V0Reader.V0Violation row : chunk) {
                    if (row.legacyId() > afterId) afterId = row.legacyId();
                    reconstructor.accept(row);
                }
            }
            reconstructor.flush();

            if (!identityAcc.isEmpty()) {
                List<PlayerIdentity> idRows = new ArrayList<>(identityAcc.size());
                for (Map.Entry<UUID, long[]> e : identityAcc.entrySet()) {
                    idRows.add(new PlayerIdentity(e.getKey(), null, e.getValue()[0], e.getValue()[1]));
                }
                v1.writeBatch(Categories.PLAYER_IDENTITY, idRows);
            }

            writeState(lastViolationLegacyId.get(), "COMPLETE",
                    state.startedAt > 0 ? state.startedAt : System.currentTimeMillis(), System.currentTimeMillis());
            long elapsed = System.currentTimeMillis() - start;
            logger.info("[grim-datastore] migration complete: " + sessionsEmitted.get() + " sessions, "
                    + violationsEmitted.get() + " violations in " + elapsed + "ms");
            return new Result((int) sessionsEmitted.get(), (int) violationsEmitted.get(), elapsed, resumed);
        } catch (SQLException e) {
            throw new BackendException("legacy migration read failed", e);
        }
    }

    private SessionReconstructor.LookupLens loadLookups() throws SQLException {
        Map<Integer, String> servers = v0.loadLookup("grim_history_servers", "server_name");
        Map<Integer, String> checkNames = v0.loadLookup("grim_history_check_names", "check_name_string");
        Map<Integer, String> grimVersions = v0.loadLookup("grim_history_versions", "grim_version_string");
        Map<Integer, String> clientBrands = v0.loadLookup("grim_history_client_brands", "client_brand_string");
        Map<Integer, String> clientVersions = v0.loadLookup("grim_history_client_versions", "client_version_string");
        Map<Integer, String> serverVersions = v0.loadLookup("grim_history_server_versions", "server_version_string");
        return new SessionReconstructor.LookupLens() {
            @Override public String server(int id) { return servers.getOrDefault(id, "Unknown"); }
            @Override public String checkName(int id) { return checkNames.getOrDefault(id, "Unknown"); }
            @Override public String grimVersion(int id) { return grimVersions.getOrDefault(id, "unknown"); }
            @Override public String clientBrand(int id) { return clientBrands.getOrDefault(id, "unknown"); }
            @Override public String clientVersion(int id) { return clientVersions.getOrDefault(id, "unknown"); }
            @Override public String serverVersion(int id) { return serverVersions.getOrDefault(id, "unknown"); }
        };
    }

    private MigrationState readState() throws BackendException {
        Connection c = v1.writeConnection();
        if (c == null) throw new BackendException("v1 backend not initialised");
        try {
            try (Statement s = c.createStatement()) {
                // Already created in SqliteSchema — but defensive in case an older db
                // predates that init.
                s.executeUpdate("CREATE TABLE IF NOT EXISTS grim_migration_state ("
                        + "id INTEGER PRIMARY KEY CHECK (id = 0), "
                        + "last_migrated_violation_id INTEGER NOT NULL DEFAULT 0, "
                        + "state TEXT NOT NULL DEFAULT 'PENDING', "
                        + "started_at INTEGER, "
                        + "completed_at INTEGER)");
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT last_migrated_violation_id, state, started_at, completed_at FROM grim_migration_state WHERE id=0");
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new MigrationState(rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getLong(4));
                }
            }
            return new MigrationState(0L, "PENDING", 0L, 0L);
        } catch (SQLException e) {
            throw new BackendException("failed to read migration state", e);
        }
    }

    private void writeState(long lastId, String state, long startedAt, long completedAt) {
        Connection c = v1.writeConnection();
        if (c == null) return;
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO grim_migration_state(id, last_migrated_violation_id, state, started_at, completed_at) "
                        + "VALUES (0, ?, ?, ?, ?) "
                        + "ON CONFLICT(id) DO UPDATE SET "
                        + "last_migrated_violation_id=excluded.last_migrated_violation_id, "
                        + "state=excluded.state, "
                        + "started_at=COALESCE(grim_migration_state.started_at, excluded.started_at), "
                        + "completed_at=CASE WHEN excluded.completed_at>0 THEN excluded.completed_at "
                        + "                   ELSE grim_migration_state.completed_at END")) {
            ps.setLong(1, lastId);
            ps.setString(2, state);
            ps.setLong(3, startedAt);
            ps.setLong(4, completedAt);
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.log(Level.WARNING, "[grim-datastore] failed to update migration state", e);
        }
    }

    public record MigrationState(long lastMigratedViolationId, String state, long startedAt, long completedAt) {}
}
