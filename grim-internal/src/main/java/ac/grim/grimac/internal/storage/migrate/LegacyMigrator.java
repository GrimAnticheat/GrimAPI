package ac.grim.grimac.internal.storage.migrate;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.SettingScope;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import ac.grim.grimac.internal.storage.checks.StableKeyMapping;
import org.jetbrains.annotations.ApiStatus;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
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
 * Migrates the legacy {@code grim_history_*} tables into the v1 schema. Reads
 * the old rows in ascending {@code (player_uuid, created_at, id)} order, feeds
 * them through {@link SessionReconstructor} to bucket by time-gap, and writes
 * v1 records synchronously via {@link Backend#bulkImport} — bypasses the ring
 * buffers because it runs at startup, before the server accepts players.
 * <p>
 * Target-agnostic: works with any {@link Backend} that implements
 * {@code bulkImport}. Migration state is stored as a setting under
 * (SERVER, grim-core, legacy_v0_migration_state), so resumability works on
 * backends that have no native migration-state table. Format is a
 * pipe-delimited UTF-8 string: {@code "<lastId>|<state>|<startedAt>|<completedAt>"}.
 */
@ApiStatus.Internal
public final class LegacyMigrator {

    private static final SettingScope STATE_SCOPE = SettingScope.SERVER;
    private static final String STATE_SCOPE_KEY = "grim-core";
    private static final String STATE_KEY = "legacy_v0_migration_state";

    public record Result(int sessionsWritten, int violationsWritten, long elapsedMs, boolean resumed) {}

    private final V0Reader v0;
    private final Backend v1;
    private final CheckRegistry checks;
    private final SessionReconstructor.ClientVersionResolver clientVersionResolver;
    private final long sessionGapMs;
    private final Logger logger;
    private final int chunkSize;

    /**
     * Convenience ctor for callers (tests) that don't care about mapping the
     * legacy client-version string; every session ends up with {@code -1}.
     */
    public LegacyMigrator(V0Reader v0, Backend v1, CheckRegistry checks, long sessionGapMs, Logger logger) {
        this(v0, v1, checks, s -> -1, sessionGapMs, logger, 1000);
    }

    public LegacyMigrator(V0Reader v0, Backend v1, CheckRegistry checks,
                          SessionReconstructor.ClientVersionResolver clientVersionResolver,
                          long sessionGapMs, Logger logger) {
        this(v0, v1, checks, clientVersionResolver, sessionGapMs, logger, 1000);
    }

    public LegacyMigrator(V0Reader v0, Backend v1, CheckRegistry checks,
                          SessionReconstructor.ClientVersionResolver clientVersionResolver,
                          long sessionGapMs, Logger logger, int chunkSize) {
        this.v0 = v0;
        this.v1 = v1;
        this.checks = checks;
        this.clientVersionResolver = clientVersionResolver;
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
        if ("COMPLETE".equals(state.state) && state.lastMigratedViolationId >= totalLegacyViolations) {
            logger.info("[grim-datastore] legacy migration already complete (v0 id "
                    + state.lastMigratedViolationId + "); skipping");
            return new Result(0, 0, System.currentTimeMillis() - start, true);
        }
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
                    v1.bulkImport(Categories.SESSION, List.of(session));
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
                        v1.bulkImport(Categories.VIOLATION, rows);
                    }
                    sessionsEmitted.incrementAndGet();
                    violationsEmitted.addAndGet(violations.size());
                    // Checkpoint after each session commit.
                    writeState(lastViolationLegacyId.get(), "IN_PROGRESS",
                            state.startedAt > 0 ? state.startedAt : System.currentTimeMillis(), 0);
                    progress.accept(violationsEmitted.get());
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

            SessionReconstructor reconstructor = new SessionReconstructor(
                    sessionGapMs, lens, resolver, clientVersionResolver, emit);

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
                v1.bulkImport(Categories.PLAYER_IDENTITY, idRows);
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
        Page<SettingRecord> page = v1.read(
                Categories.SETTING,
                Queries.getSetting(STATE_SCOPE, STATE_SCOPE_KEY, STATE_KEY));
        if (page.items().isEmpty()) return new MigrationState(0L, "PENDING", 0L, 0L);
        byte[] value = page.items().get(0).value();
        if (value == null || value.length == 0) return new MigrationState(0L, "PENDING", 0L, 0L);
        String raw = new String(value, StandardCharsets.UTF_8);
        String[] parts = raw.split("\\|", -1);
        if (parts.length < 4) return new MigrationState(0L, "PENDING", 0L, 0L);
        try {
            return new MigrationState(
                    Long.parseLong(parts[0]),
                    parts[1],
                    Long.parseLong(parts[2]),
                    Long.parseLong(parts[3]));
        } catch (NumberFormatException e) {
            return new MigrationState(0L, "PENDING", 0L, 0L);
        }
    }

    private void writeState(long lastId, String state, long startedAt, long completedAt) {
        String packed = lastId + "|" + state + "|" + startedAt + "|" + completedAt;
        SettingRecord rec = new SettingRecord(
                STATE_SCOPE, STATE_SCOPE_KEY, STATE_KEY,
                packed.getBytes(StandardCharsets.UTF_8),
                System.currentTimeMillis());
        try {
            v1.bulkImport(Categories.SETTING, List.of(rec));
        } catch (BackendException e) {
            logger.log(Level.WARNING,
                    "[grim-datastore] failed to update migration state", e);
        }
    }

    public record MigrationState(long lastMigratedViolationId, String state, long startedAt, long completedAt) {}
}
