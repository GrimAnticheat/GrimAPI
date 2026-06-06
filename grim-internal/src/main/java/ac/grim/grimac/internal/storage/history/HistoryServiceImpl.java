package ac.grim.grimac.internal.storage.history;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.category.EventStreamCategory;
import ac.grim.grimac.api.storage.event.ServerStartupEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.history.CheckBucket;
import ac.grim.grimac.api.storage.history.CheckCount;
import ac.grim.grimac.api.storage.history.HistoryService;
import ac.grim.grimac.api.storage.history.SessionDetail;
import ac.grim.grimac.api.storage.history.SessionSummary;
import ac.grim.grimac.api.storage.history.ViolationEntry;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.api.storage.verbose.VerboseBuf;
import ac.grim.grimac.api.storage.verbose.VerboseFormatter;
import ac.grim.grimac.api.storage.verbose.VerboseSchema;
import ac.grim.grimac.api.storage.verbose.VerboseSink;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import ac.grim.grimac.internal.storage.verbose.GenericVerboseReader;
import ac.grim.grimac.internal.storage.verbose.VerboseManifest;
import ac.grim.grimac.internal.storage.verbose.VerboseRegistry;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link HistoryService} implementation that walks the {@link DataStore} via
 * the public query surface and returns pure data records. Rendering to chat
 * components is a concern of the host plugin.
 * <p>
 * {@link #listSessions} returns page-local ordinals cheaply; {@link
 * #listSessionsPaged} recomputes the global chronological ordinal via a
 * {@code countSessions} query so Session 1 is always the player's very first
 * session across their whole history, regardless of which page it appears on.
 */
@ApiStatus.Internal
public final class HistoryServiceImpl implements HistoryService {

    private final DataStore store;
    private final CheckRegistry checks;
    private final int defaultPageSize;
    private final long defaultGroupIntervalMs;
    /**
     * Optional v2 {@link EventStreamCategory} for violations. When set
     * (post-Phase-1.4c wiring), the per-page violation-count batch uses
     * {@code countMany} — one round-trip instead of N. When null, falls
     * back to the legacy per-session loop.
     */
    private volatile @Nullable EventStreamCategory<ViolationEvent, ViolationRecord> v2Violations;
    private volatile @Nullable Category<ServerStartupEvent> v2Startups;
    private volatile @Nullable VerboseRegistry verboseRegistry;
    private final Map<UUID, ServerStartupRecord> startupCache = new ConcurrentHashMap<>();
    private final Map<UUID, VerboseManifest.Decoded> verboseManifestCache = new ConcurrentHashMap<>();

    public HistoryServiceImpl(@NotNull DataStore store, @NotNull CheckRegistry checks,
                              int defaultPageSize, long defaultGroupIntervalMs) {
        this.store = store;
        this.checks = checks;
        this.defaultPageSize = defaultPageSize;
        this.defaultGroupIntervalMs = defaultGroupIntervalMs;
    }

    public HistoryServiceImpl(@NotNull DataStore store, @NotNull CheckRegistry checks,
                              int defaultPageSize, long defaultGroupIntervalMs,
                              @Nullable VerboseRegistry verboseRegistry) {
        this(store, checks, defaultPageSize, defaultGroupIntervalMs);
        this.verboseRegistry = verboseRegistry;
    }

    /**
     * Install the v2 violations category. After installation the N+1 batch
     * fix in {@code toSummaryPage} kicks in.
     */
    public HistoryServiceImpl withV2Violations(@NotNull EventStreamCategory<ViolationEvent, ViolationRecord> v2) {
        this.v2Violations = v2;
        return this;
    }

    /** Install the v2 server-startup category for startup metadata resolution. */
    public HistoryServiceImpl withV2Startups(@NotNull Category<ServerStartupEvent> v2) {
        this.v2Startups = v2;
        return this;
    }

    /** Install the binary verbose registry used by history rendering. */
    public HistoryServiceImpl withVerboseRegistry(@NotNull VerboseRegistry registry) {
        this.verboseRegistry = registry;
        return this;
    }

    @Override
    public @NotNull CompletionStage<@NotNull Page<SessionSummary>> listSessions(
            @NotNull UUID player, @Nullable Cursor cursor, int pageSize) {
        int ps = pageSize > 0 ? pageSize : defaultPageSize;
        // Session ordinals are GLOBAL chronological — Session 1 = the player's
        // very first session, Session K = their most recent. Needs countSessions
        // to know where this page's slice sits in the global ordering.
        return store.countSessionsByPlayer(player).thenCompose(total ->
                store.query(Categories.SESSION, Queries.listSessionsByPlayer(player, ps, cursor))
                        .thenCompose(p -> toSummaryPage(p, total, 0 /* unknown sessions-before; see listSessionsPaged */)));
    }

    /**
     * Page-indexed variant that knows exactly where in the global list this page
     * sits, so ordinals on returned summaries are always correct.
     */
    public @NotNull CompletionStage<@NotNull Page<SessionSummary>> listSessionsPaged(
            @NotNull UUID player, int pageIndex1Based, int pageSize) {
        int ps = pageSize > 0 ? pageSize : defaultPageSize;
        int page = Math.max(1, pageIndex1Based);
        return store.countSessionsByPlayer(player).thenCompose(total -> {
            long sessionsBefore = (long) (page - 1) * ps;
            if (sessionsBefore >= total) {
                return CompletableFuture.completedStage(new Page<>(List.of(), null));
            }
            return advanceCursor(player, page, ps).thenCompose(cursor ->
                    store.query(Categories.SESSION, Queries.listSessionsByPlayer(player, ps, cursor))
                            .thenCompose(p -> toSummaryPage(p, total, sessionsBefore)));
        });
    }

    private CompletionStage<Cursor> advanceCursor(UUID player, int pageIndex1Based, int pageSize) {
        if (pageIndex1Based <= 1) return CompletableFuture.completedStage(null);
        CompletionStage<Cursor> cur = CompletableFuture.completedStage(null);
        for (int i = 1; i < pageIndex1Based; i++) {
            cur = cur.thenCompose(c -> store.query(Categories.SESSION,
                            Queries.listSessionsByPlayer(player, pageSize, c))
                    .thenApply(Page::nextCursor));
        }
        return cur;
    }

    private CompletionStage<Page<SessionSummary>> toSummaryPage(Page<SessionRecord> page, long total, long sessionsBefore) {
        List<SessionRecord> sessions = page.items();
        if (sessions.isEmpty()) {
            return CompletableFuture.completedFuture(new Page<>(List.of(), page.nextCursor()));
        }
        return resolveStartups(sessions).thenCompose(startups -> {
            EventStreamCategory<ViolationEvent, ViolationRecord> v2 = this.v2Violations;
            if (v2 != null) {
                return toSummaryPageV2(v2, sessions, startups, page.nextCursor(), total, sessionsBefore);
            }
            return toSummaryPageLegacy(sessions, startups, page.nextCursor(), total, sessionsBefore);
        });
    }

    /**
     * N+1-killing path: one {@code countMany} for all session-on-page
     * violation counts, plus per-session unique-check counts in parallel
     * (single fan-out, all sessions in flight at once). Once the design
     * adds {@code countDistinctMany}, the unique-checks call also collapses
     * to a single round-trip.
     */
    @SuppressWarnings("unchecked")
    private CompletionStage<Page<SessionSummary>> toSummaryPageV2(
            @NotNull EventStreamCategory<ViolationEvent, ViolationRecord> v2,
            @NotNull List<SessionRecord> sessions,
            @NotNull Map<UUID, ServerStartupRecord> startups,
            @Nullable Cursor nextCursor,
            long total,
            long sessionsBefore) {
        List<UUID> sessionIds = new ArrayList<>(sessions.size());
        for (SessionRecord s : sessions) sessionIds.add(s.sessionId());

        CompletionStage<Map<UUID, Long>> countsByIdStage =
            (CompletionStage<Map<UUID, Long>>) (CompletionStage<?>)
                store.execute(v2.countMany("session_id", sessionIds));

        return countsByIdStage.thenCompose(countsById -> {
            SessionSummary[] out = new SessionSummary[sessions.size()];
            @SuppressWarnings("unchecked")
            CompletionStage<Long>[] uniqueStages = new CompletionStage[sessions.size()];
            for (int i = 0; i < sessions.size(); i++) {
                UUID sid = sessions.get(i).sessionId();
                uniqueStages[i] = (CompletionStage<Long>) (CompletionStage<?>)
                    store.execute(v2.countDistinct("session_id", sid, "check_id"));
            }
            CompletionStage<Void> all = CompletableFuture.completedStage(null);
            for (int i = 0; i < sessions.size(); i++) {
                SessionRecord s = sessions.get(i);
                int ordinal = (int) (total - sessionsBefore - i);
                int slot = i;
                long violationCount = countsById.getOrDefault(s.sessionId(), 0L);
                CompletionStage<Long> uniqueStage = uniqueStages[i];
                all = all.thenCombine(uniqueStage, (v, unique) -> {
                    out[slot] = toSummary(s, startups.get(s.startupId()), ordinal, violationCount, unique.intValue());
                    return null;
                });
            }
            return all.thenApply(v -> new Page<>(List.of(out), nextCursor));
        });
    }

    /** Legacy per-session sequential count path; retained as the fallback. */
    @SuppressWarnings({"deprecation", "removal"})
    private CompletionStage<Page<SessionSummary>> toSummaryPageLegacy(
            @NotNull List<SessionRecord> sessions,
            @NotNull Map<UUID, ServerStartupRecord> startups,
            @Nullable Cursor nextCursor,
            long total,
            long sessionsBefore) {
        SessionSummary[] out = new SessionSummary[sessions.size()];
        CompletionStage<Void> chain = CompletableFuture.completedStage(null);
        for (int i = 0; i < sessions.size(); i++) {
            SessionRecord s = sessions.get(i);
            int ordinal = (int) (total - sessionsBefore - i);
            int slot = i;
            chain = chain.thenCompose(v -> store.countViolationsInSession(s.sessionId())
                    .thenCompose(count -> store.countUniqueChecksInSession(s.sessionId())
                            .thenAccept(unique -> out[slot] =
                                    toSummary(s, startups.get(s.startupId()), ordinal, count, unique.intValue()))));
        }
        return chain.thenApply(v -> new Page<>(List.of(out), nextCursor));
    }

    private static SessionSummary toSummary(
            SessionRecord s,
            @Nullable ServerStartupRecord startup,
            int ordinal,
            long violationCount,
            int uniqueCheckCount) {
        return new SessionSummary(
                s.sessionId(), s.playerUuid(), ordinal,
                s.startedEpochMs(), s.lastActivityEpochMs(), s.closedAtEpochMs(),
                grimVersion(s, startup), serverName(s, startup), s.clientVersion(), s.clientBrand(),
                violationCount, uniqueCheckCount);
    }

    @Override
    public @NotNull CompletionStage<@Nullable SessionDetail> getSessionDetail(
            @NotNull UUID player, @NotNull UUID sessionId) {
        return store.query(Categories.SESSION, Queries.getSessionById(sessionId))
                .thenCompose(sessionPage -> {
                    if (sessionPage.items().isEmpty()) return CompletableFuture.completedStage(null);
                    SessionRecord s = sessionPage.items().get(0);
                    if (!s.playerUuid().equals(player)) return CompletableFuture.completedStage(null);
                    return resolveStartup(s).thenCompose(startup ->
                            store.query(Categories.VIOLATION,
                                            Queries.listViolationsInSession(sessionId, 10_000, null))
                                    .thenApply(vPage -> toDetail(s, startup, vPage.items(), /*sessionOrdinal*/ 0)));
                });
    }

    /**
     * Detail-by-ordinal using GLOBAL chronological ordering — Session 1 is the
     * player's very first session, Session K is their most recent. Computes the
     * DESC-index from {@code total - ordinal} and fetches exactly the page
     * containing that session.
     */
    public @NotNull CompletionStage<@Nullable SessionDetail> getSessionDetailByOrdinal(
            @NotNull UUID player, int sessionOrdinal) {
        if (sessionOrdinal < 1) return CompletableFuture.completedStage(null);
        return store.countSessionsByPlayer(player).thenCompose(total -> {
            if (sessionOrdinal > total) return CompletableFuture.completedStage(null);
            // descIndex: 0 = newest, (total - 1) = oldest. ordinal 1 = oldest → descIndex = total - 1.
            long descIndex = total - sessionOrdinal;
            int pageSize = Math.max(1, defaultPageSize);
            int pageIndex = (int) (descIndex / pageSize) + 1;
            int inPage = (int) (descIndex % pageSize);
            return advanceCursor(player, pageIndex, pageSize).thenCompose(cursor ->
                    store.query(Categories.SESSION,
                                    Queries.listSessionsByPlayer(player, pageSize, cursor))
                            .thenCompose(p -> {
                                if (inPage >= p.items().size()) return CompletableFuture.completedStage(null);
                                SessionRecord s = p.items().get(inPage);
                                return resolveStartup(s).thenCompose(startup ->
                                        store.query(Categories.VIOLATION,
                                                        Queries.listViolationsInSession(s.sessionId(), 10_000, null))
                                                .thenApply(vPage -> toDetail(s, startup, vPage.items(), sessionOrdinal)));
                            }));
        });
    }

    @Override
    public @NotNull CompletionStage<@NotNull Long> countSessions(@NotNull UUID player) {
        return store.countSessionsByPlayer(player);
    }

    private SessionDetail toDetail(
            SessionRecord s,
            @Nullable ServerStartupRecord startup,
            List<ViolationRecord> violations,
            int sessionOrdinal) {
        long bucketSize = Math.max(1, defaultGroupIntervalMs);
        long start = s.startedEpochMs();

        List<ViolationEntry> entries = new ArrayList<>(violations.size());
        Map<Long, Map<Integer, Integer>> bucketCounts = new LinkedHashMap<>();
        java.util.Set<Integer> uniqueChecks = new java.util.LinkedHashSet<>();
        for (ViolationRecord v : violations) {
            long offset = v.occurredEpochMs() - start;
            String display = checks.displayFor(v.checkId())
                    .orElseGet(() -> checks.stableKeyFor(v.checkId()).orElse("check#" + v.checkId()));
            String stable = checks.stableKeyFor(v.checkId()).orElse("check#" + v.checkId());
            String description = checks.descriptionFor(v.checkId()).orElse("");
            entries.add(new ViolationEntry(
                    v.checkId(), stable, display, description,
                    offset, v.vl(), renderVerbose(v.verboseData(), startup, v.checkId()),
                    ac.grim.grimac.api.storage.model.VerboseFormat.TEXT));

            long bucket = offset / bucketSize;
            bucketCounts.computeIfAbsent(bucket, k -> new LinkedHashMap<>())
                    .merge(v.checkId(), 1, Integer::sum);
            uniqueChecks.add(v.checkId());
        }

        List<CheckBucket> buckets = new ArrayList<>(bucketCounts.size());
        for (Map.Entry<Long, Map<Integer, Integer>> e : bucketCounts.entrySet()) {
            long bucketStart = e.getKey() * bucketSize;
            List<CheckCount> counts = new ArrayList<>(e.getValue().size());
            for (Map.Entry<Integer, Integer> cc : e.getValue().entrySet()) {
                String display = checks.displayFor(cc.getKey())
                        .orElseGet(() -> checks.stableKeyFor(cc.getKey()).orElse("check#" + cc.getKey()));
                String stable = checks.stableKeyFor(cc.getKey()).orElse("check#" + cc.getKey());
                String description = checks.descriptionFor(cc.getKey()).orElse("");
                counts.add(new CheckCount(cc.getKey(), stable, display, description, cc.getValue()));
            }
            buckets.add(new CheckBucket(bucketStart, counts));
        }

        return new SessionDetail(
                s.sessionId(), s.playerUuid(), sessionOrdinal,
                s.startedEpochMs(), s.lastActivityEpochMs(),
                grimVersion(s, startup), serverName(s, startup), s.clientVersion(), s.clientBrand(),
                bucketSize, uniqueChecks.size(), buckets, entries);
    }

    @SuppressWarnings("unchecked")
    private CompletionStage<Map<UUID, ServerStartupRecord>> resolveStartups(@NotNull List<SessionRecord> sessions) {
        Category<ServerStartupEvent> cat = this.v2Startups;
        if (cat == null) return CompletableFuture.completedStage(Map.of());

        LinkedHashSet<UUID> missing = new LinkedHashSet<>();
        Map<UUID, ServerStartupRecord> resolved = new HashMap<>();
        for (SessionRecord s : sessions) {
            UUID id = s.startupId();
            if (id == null) continue;
            ServerStartupRecord cached = startupCache.get(id);
            if (cached != null) {
                resolved.put(id, cached);
            } else {
                missing.add(id);
            }
        }
        if (missing.isEmpty()) return CompletableFuture.completedStage(resolved);

        CompletionStage<List<ServerStartupRecord>> fetched =
                (CompletionStage<List<ServerStartupRecord>>) (CompletionStage<?>)
                        store.execute(new EntityOps.GetManyOp<>(cat, missing));
        return fetched.thenApply(rows -> {
            for (ServerStartupRecord row : rows) {
                startupCache.put(row.startupId(), row);
                resolved.put(row.startupId(), row);
            }
            return resolved;
        });
    }

    private CompletionStage<@Nullable ServerStartupRecord> resolveStartup(@NotNull SessionRecord session) {
        UUID startupId = session.startupId();
        if (startupId == null || v2Startups == null) return CompletableFuture.completedStage(null);
        ServerStartupRecord cached = startupCache.get(startupId);
        if (cached != null) return CompletableFuture.completedStage(cached);
        return resolveStartups(List.of(session)).thenApply(map -> map.get(startupId));
    }

    private static @Nullable String serverName(
            @NotNull SessionRecord session,
            @Nullable ServerStartupRecord startup) {
        return startup != null ? startup.serverName() : session.serverName();
    }

    private static @Nullable String grimVersion(
            @NotNull SessionRecord session,
            @Nullable ServerStartupRecord startup) {
        return startup != null ? startup.grimVersion() : session.grimVersion();
    }

    @Nullable String renderVerbose(
            @Nullable byte[] verboseData,
            @Nullable ServerStartupRecord startup,
            int checkId) {
        if (verboseData == null || verboseData.length == 0) return null;
        VerboseManifest.Decoded manifest = startup == null ? null : decodedManifest(startup);
        if (manifest == null) return renderText(verboseData);

        int version = manifest.codecVersionOrText(checkId);
        if (!manifest.supported() || version <= 0) return renderText(verboseData);

        int flavor = manifest.flavor();
        VerboseRegistry registry = this.verboseRegistry;
        if (registry != null) {
            VerboseFormatter formatter = safeFormatter(registry, flavor, checkId, version);
            if (formatter != null) {
                try {
                    StringBuilder rendered = new StringBuilder();
                    formatter.render(VerboseBuf.wrap(verboseData), VerboseSink.into(rendered));
                    return rendered.toString();
                } catch (Throwable ignored) {
                    return placeholder(verboseData, version, "decode failed");
                }
            }

            VerboseSchema.Layout layout = safeLayout(registry, flavor, checkId, version);
            if (layout != null) {
                try {
                    StringBuilder rendered = new StringBuilder();
                    GenericVerboseReader.render(layout, VerboseBuf.wrap(verboseData), VerboseSink.into(rendered));
                    return rendered.toString();
                } catch (GenericVerboseReader.UnderflowException | RuntimeException ignored) {
                    return placeholder(verboseData, version, "decode failed");
                }
            }
        }
        return placeholder(verboseData, version, "schema unavailable");
    }

    private @Nullable VerboseManifest.Decoded decodedManifest(@NotNull ServerStartupRecord startup) {
        byte[] manifest = startup.verboseManifest();
        if (manifest == null || manifest.length == 0) return null;
        return verboseManifestCache.computeIfAbsent(startup.startupId(), ignored -> {
            try {
                return VerboseManifest.decode(manifest);
            } catch (RuntimeException e) {
                return new VerboseManifest.Decoded(
                        0, VerboseManifest.FLAVOR_UNKNOWN, Map.of(), false);
            }
        });
    }

    private static @Nullable VerboseFormatter safeFormatter(
            @NotNull VerboseRegistry registry,
            int flavor,
            int checkId,
            int version) {
        try {
            return registry.codeFormatter(flavor, checkId, version);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static @Nullable VerboseSchema.Layout safeLayout(
            @NotNull VerboseRegistry registry,
            int flavor,
            int checkId,
            int version) {
        try {
            return registry.layout(flavor, checkId, version);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static @NotNull String renderText(byte @NotNull [] verboseData) {
        return new String(verboseData, StandardCharsets.UTF_8);
    }

    private static @NotNull String placeholder(byte @NotNull [] verboseData, int version, @NotNull String reason) {
        return "[binary verbose v" + version + ", " + reason + "] " + hex(verboseData);
    }

    private static @NotNull String hex(byte @NotNull [] bytes) {
        char[] out = new char[2 + bytes.length * 2];
        out[0] = '0';
        out[1] = 'x';
        char[] digits = "0123456789abcdef".toCharArray();
        int j = 2;
        for (byte b : bytes) {
            out[j++] = digits[(b >>> 4) & 0x0F];
            out[j++] = digits[b & 0x0F];
        }
        return new String(out);
    }
}
