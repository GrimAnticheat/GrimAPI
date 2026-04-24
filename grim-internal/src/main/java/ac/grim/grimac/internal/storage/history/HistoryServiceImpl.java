package ac.grim.grimac.internal.storage.history;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.history.CheckBucket;
import ac.grim.grimac.api.storage.history.CheckCount;
import ac.grim.grimac.api.storage.history.HistoryService;
import ac.grim.grimac.api.storage.history.SessionDetail;
import ac.grim.grimac.api.storage.history.SessionSummary;
import ac.grim.grimac.api.storage.history.ViolationEntry;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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

    public HistoryServiceImpl(@NotNull DataStore store, @NotNull CheckRegistry checks,
                              int defaultPageSize, long defaultGroupIntervalMs) {
        this.store = store;
        this.checks = checks;
        this.defaultPageSize = defaultPageSize;
        this.defaultGroupIntervalMs = defaultGroupIntervalMs;
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
        SessionSummary[] out = new SessionSummary[sessions.size()];
        CompletionStage<Void> chain = CompletableFuture.completedStage(null);
        for (int i = 0; i < sessions.size(); i++) {
            SessionRecord s = sessions.get(i);
            // Global ordinal: newest in DESC list = total; session at DESC index i
            // on a page starting at sessionsBefore has ordinal
            // total - sessionsBefore - i. Session 1 = oldest.
            int ordinal = (int) (total - sessionsBefore - i);
            int slot = i;
            chain = chain.thenCompose(v -> store.countViolationsInSession(s.sessionId())
                    .thenCompose(count -> store.countUniqueChecksInSession(s.sessionId())
                            .thenAccept(unique -> out[slot] = toSummary(s, ordinal, count, unique.intValue()))));
        }
        return chain.thenApply(v -> new Page<>(List.of(out), page.nextCursor()));
    }

    private static SessionSummary toSummary(SessionRecord s, int ordinal, long violationCount, int uniqueCheckCount) {
        return new SessionSummary(
                s.sessionId(), s.playerUuid(), ordinal,
                s.startedEpochMs(), s.lastActivityEpochMs(),
                s.grimVersion(), s.serverName(), s.clientVersion(), s.clientBrand(),
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
                    return store.query(Categories.VIOLATION,
                                    Queries.listViolationsInSession(sessionId, 10_000, null))
                            .thenApply(vPage -> toDetail(s, vPage.items(), /*sessionOrdinal*/ 0));
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
                                return store.query(Categories.VIOLATION,
                                                Queries.listViolationsInSession(s.sessionId(), 10_000, null))
                                        .thenApply(vPage -> toDetail(s, vPage.items(), sessionOrdinal));
                            }));
        });
    }

    @Override
    public @NotNull CompletionStage<@NotNull Long> countSessions(@NotNull UUID player) {
        return store.countSessionsByPlayer(player);
    }

    private SessionDetail toDetail(SessionRecord s, List<ViolationRecord> violations, int sessionOrdinal) {
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
                    offset, v.vl(), v.verbose(), v.verboseFormat()));

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
                s.grimVersion(), s.serverName(), s.clientVersion(), s.clientBrand(),
                bucketSize, uniqueChecks.size(), buckets, entries);
    }
}
