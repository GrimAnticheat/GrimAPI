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
 * Phase-1 {@link HistoryService} impl. Pure data — all rendering lives in Layer 3
 * (e.g. Grim-public's {@code HistoryComponentRenderer}).
 * <p>
 * Session labels on list pages are page-local ordinals (newest-on-page = N, oldest-on-page
 * = 1): §12 calls for global ordinals but cheaply computing those needs a count query we
 * haven't added. See DESIGN_NOTES.md.
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
        return store.query(Categories.SESSION, Queries.listSessionsByPlayer(player, ps, cursor))
                .thenCompose(this::toSummaryPage);
    }

    private CompletionStage<Page<SessionSummary>> toSummaryPage(Page<SessionRecord> page) {
        List<SessionRecord> sessions = page.items();
        if (sessions.isEmpty()) {
            return CompletableFuture.completedFuture(new Page<>(List.of(), page.nextCursor()));
        }
        SessionSummary[] out = new SessionSummary[sessions.size()];
        CompletionStage<Void> chain = CompletableFuture.completedStage(null);
        for (int i = 0; i < sessions.size(); i++) {
            SessionRecord s = sessions.get(i);
            int ordinal = sessions.size() - i;
            int slot = i;
            chain = chain.thenCompose(v -> store.countViolationsInSession(s.sessionId())
                    .thenAccept(count -> out[slot] = toSummary(s, ordinal, count)));
        }
        return chain.thenApply(v -> new Page<>(List.of(out), page.nextCursor()));
    }

    private static SessionSummary toSummary(SessionRecord s, int ordinal, long violationCount) {
        return new SessionSummary(
                s.sessionId(), s.playerUuid(), ordinal,
                s.startedEpochMs(), s.lastActivityEpochMs(),
                s.grimVersion(), s.serverName(), s.clientVersionString(), s.clientBrand(),
                violationCount);
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
                            .thenApply(vPage -> toDetail(s, vPage.items()));
                });
    }

    private SessionDetail toDetail(SessionRecord s, List<ViolationRecord> violations) {
        long bucketSize = Math.max(1, defaultGroupIntervalMs);
        long start = s.startedEpochMs();

        List<ViolationEntry> entries = new ArrayList<>(violations.size());
        Map<Long, Map<Integer, Integer>> bucketCounts = new LinkedHashMap<>();
        for (ViolationRecord v : violations) {
            long offset = v.occurredEpochMs() - start;
            String display = checks.displayFor(v.checkId())
                    .orElseGet(() -> checks.stableKeyFor(v.checkId()).orElse("check#" + v.checkId()));
            String stable = checks.stableKeyFor(v.checkId()).orElse("check#" + v.checkId());
            entries.add(new ViolationEntry(
                    v.checkId(), stable, display,
                    offset, v.vl(), v.verbose(), v.verboseFormat()));

            long bucket = offset / bucketSize;
            bucketCounts.computeIfAbsent(bucket, k -> new LinkedHashMap<>())
                    .merge(v.checkId(), 1, Integer::sum);
        }

        List<CheckBucket> buckets = new ArrayList<>(bucketCounts.size());
        for (Map.Entry<Long, Map<Integer, Integer>> e : bucketCounts.entrySet()) {
            long bucketStart = e.getKey() * bucketSize;
            List<CheckCount> counts = new ArrayList<>(e.getValue().size());
            for (Map.Entry<Integer, Integer> cc : e.getValue().entrySet()) {
                String display = checks.displayFor(cc.getKey())
                        .orElseGet(() -> checks.stableKeyFor(cc.getKey()).orElse("check#" + cc.getKey()));
                String stable = checks.stableKeyFor(cc.getKey()).orElse("check#" + cc.getKey());
                counts.add(new CheckCount(cc.getKey(), stable, display, cc.getValue()));
            }
            buckets.add(new CheckBucket(bucketStart, counts));
        }

        return new SessionDetail(
                s.sessionId(), s.playerUuid(),
                s.startedEpochMs(), s.lastActivityEpochMs(),
                s.grimVersion(), s.serverName(), s.clientVersionString(), s.clientBrand(),
                bucketSize, buckets, entries);
    }
}
