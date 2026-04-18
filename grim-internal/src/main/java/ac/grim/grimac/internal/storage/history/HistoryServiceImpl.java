package ac.grim.grimac.internal.storage.history;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.history.HistoryService;
import ac.grim.grimac.api.storage.history.RenderOptions;
import ac.grim.grimac.api.storage.history.RenderedHistoryLine;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Phase-1 {@link HistoryService} impl. Produces platform-neutral
 * {@link RenderedHistoryLine} output — each platform's command layer converts the
 * sealed {@code Segment} types to its native chat component model.
 * <p>
 * Session labels are page-local ordinals (newest-on-page = 1, oldest-on-page = N): §12
 * calls for global ordinals but computing those cheaply needs a count query we haven't
 * added. See DESIGN_NOTES.md.
 */
@ApiStatus.Internal
public final class HistoryServiceImpl implements HistoryService {

    private final DataStore store;
    private final CheckRegistry checks;
    private final int defaultPageSize;
    private final long defaultGroupIntervalMs;

    public HistoryServiceImpl(DataStore store, CheckRegistry checks,
                              int defaultPageSize, long defaultGroupIntervalMs) {
        this.store = store;
        this.checks = checks;
        this.defaultPageSize = defaultPageSize;
        this.defaultGroupIntervalMs = defaultGroupIntervalMs;
    }

    @Override
    public CompletionStage<SessionListResult> renderSessionList(UUID player, @Nullable Cursor cursor, int pageSize) {
        int ps = pageSize > 0 ? pageSize : defaultPageSize;
        return store.query(Categories.SESSION, Queries.listSessionsByPlayer(player, ps, cursor))
                .thenCompose(page -> renderPageLines(page, player));
    }

    private CompletionStage<SessionListResult> renderPageLines(Page<SessionRecord> page, UUID player) {
        List<RenderedHistoryLine> out = new ArrayList<>();
        if (page.items().isEmpty()) {
            out.add(line(
                    literal("No session history for "),
                    new RenderedHistoryLine.Segment.PlayerRef(player, null),
                    literal(".")));
            return java.util.concurrent.CompletableFuture.completedFuture(
                    new SessionListResult(out, page.nextCursor(), 0));
        }

        // Header line — pagination info goes here.
        out.add(line(styled("Showing session history for ", RenderedHistoryLine.Color.AQUA),
                new RenderedHistoryLine.Segment.PlayerRef(player, null)));

        // For each session, compute violation count in parallel? Phase 1: sequential;
        // typical page size is ~15 which keeps total latency low on local SQLite.
        CompletionStage<Void> chain = java.util.concurrent.CompletableFuture.completedStage(null);
        int[] ordinal = new int[]{page.items().size()};
        for (SessionRecord session : page.items()) {
            final int ord = ordinal[0]--;
            chain = chain.thenCompose(v -> store.countViolationsInSession(session.sessionId())
                    .thenAccept(count -> out.add(renderSessionSummary(session, ord, count))));
        }
        return chain.thenApply(v -> new SessionListResult(out, page.nextCursor(), page.items().size()));
    }

    private RenderedHistoryLine renderSessionSummary(SessionRecord session, int ordinal, long violationCount) {
        long duration = session.lastActivityEpochMs() - session.startedEpochMs();
        long ageMs = System.currentTimeMillis() - session.startedEpochMs();
        return line(
                styled("[" + nullToUnknown(session.grimVersion()) + "]", RenderedHistoryLine.Color.DARK_GRAY),
                literal(" "),
                styled("[" + nullToUnknown(session.serverName()) + "]", RenderedHistoryLine.Color.DARK_GRAY),
                literal(" "),
                styled("[" + nullToUnknown(session.clientVersionString()) + "]", RenderedHistoryLine.Color.DARK_GRAY),
                literal(" "),
                styled("Session " + ordinal, RenderedHistoryLine.Color.AQUA),
                literal(" duration "),
                new RenderedHistoryLine.Segment.Duration(duration),
                literal(" with "),
                styled(Long.toString(violationCount), RenderedHistoryLine.Color.RED),
                literal(" violations "),
                literal("("),
                new RenderedHistoryLine.Segment.Timestamp(session.startedEpochMs(),
                        RenderedHistoryLine.RelativeFormat.AGO_COMPACT),
                literal(")"));
    }

    @Override
    public CompletionStage<List<RenderedHistoryLine>> renderSessionDetail(UUID player, UUID sessionId, RenderOptions opts) {
        RenderOptions resolved = opts == null ? new RenderOptions(false, defaultGroupIntervalMs) : opts;
        return store.query(Categories.SESSION, Queries.getSessionById(sessionId))
                .thenCompose(sessionPage -> {
                    if (sessionPage.items().isEmpty()) {
                        return java.util.concurrent.CompletableFuture.completedStage(
                                List.of(line(literal("Session not found."))));
                    }
                    SessionRecord s = sessionPage.items().get(0);
                    if (!s.playerUuid().equals(player)) {
                        return java.util.concurrent.CompletableFuture.completedStage(
                                List.of(line(literal("Session does not belong to that player."))));
                    }
                    return store.query(Categories.VIOLATION,
                                    Queries.listViolationsInSession(sessionId, 10_000, null))
                            .thenApply(vPage -> renderDetail(s, vPage.items(), resolved));
                });
    }

    private List<RenderedHistoryLine> renderDetail(SessionRecord s, List<ViolationRecord> violations, RenderOptions opts) {
        long dur = s.lastActivityEpochMs() - s.startedEpochMs();
        List<RenderedHistoryLine> out = new ArrayList<>();
        out.add(line(literal("Showing session details:")));
        out.add(line(literal("Grim: "), styled(nullToUnknown(s.grimVersion()), RenderedHistoryLine.Color.AQUA),
                literal(", Server: "), styled(nullToUnknown(s.serverName()), RenderedHistoryLine.Color.AQUA),
                literal(", Duration: "), new RenderedHistoryLine.Segment.Duration(dur),
                literal(", Date: "),
                new RenderedHistoryLine.Segment.Timestamp(s.startedEpochMs(), RenderedHistoryLine.RelativeFormat.AGO_COMPACT)));
        out.add(line(literal("Client: "), styled(nullToUnknown(s.clientVersionString()), RenderedHistoryLine.Color.AQUA),
                literal(", Brand: "), styled(nullToUnknown(s.clientBrand()), RenderedHistoryLine.Color.AQUA)));
        out.add(line(literal("Violations:")));

        if (violations.isEmpty()) {
            out.add(line(literal("  (none)")));
            return out;
        }

        // Group into buckets by groupIntervalMs, relative to session start.
        long bucketSize = Math.max(1, opts.groupIntervalMs());
        long start = s.startedEpochMs();
        Map<Long, List<ViolationRecord>> buckets = new LinkedHashMap<>();
        for (ViolationRecord v : violations) {
            long bucket = (v.occurredEpochMs() - start) / bucketSize;
            buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(v);
        }
        for (Map.Entry<Long, List<ViolationRecord>> entry : buckets.entrySet()) {
            long bucketStartRel = entry.getKey() * bucketSize;
            Map<Integer, Integer> perCheck = new LinkedHashMap<>();
            for (ViolationRecord v : entry.getValue()) {
                perCheck.merge(v.checkId(), 1, Integer::sum);
            }
            List<RenderedHistoryLine.Segment> segs = new ArrayList<>();
            segs.add(literal("- "));
            int i = 0;
            for (Map.Entry<Integer, Integer> cc : perCheck.entrySet()) {
                if (i++ > 0) segs.add(literal(", "));
                String name = checks.displayFor(cc.getKey()).orElseGet(() ->
                        checks.stableKeyFor(cc.getKey()).orElse("check#" + cc.getKey()));
                segs.add(new RenderedHistoryLine.Segment.CheckRef(cc.getKey(), name));
                segs.add(literal(" x"));
                segs.add(styled(Integer.toString(cc.getValue()), RenderedHistoryLine.Color.RED));
            }
            segs.add(literal(" ("));
            segs.add(new RenderedHistoryLine.Segment.Duration(bucketStartRel));
            segs.add(literal(")"));
            out.add(new RenderedHistoryLine(segs));
        }

        if (opts.detailed()) {
            for (ViolationRecord v : violations) {
                String name = checks.displayFor(v.checkId())
                        .orElseGet(() -> checks.stableKeyFor(v.checkId()).orElse("check#" + v.checkId()));
                List<RenderedHistoryLine.Segment> segs = new ArrayList<>();
                segs.add(literal("    "));
                segs.add(new RenderedHistoryLine.Segment.CheckRef(v.checkId(), name));
                segs.add(literal(" @ "));
                segs.add(new RenderedHistoryLine.Segment.Duration(v.occurredEpochMs() - start));
                if (v.verbose() != null && !v.verbose().isBlank()) {
                    segs.add(literal(" — "));
                    segs.add(styled(v.verbose(), RenderedHistoryLine.Color.GRAY));
                }
                out.add(new RenderedHistoryLine(segs));
            }
        }

        // Unique checks hinted-at on the summary — not used directly in the per-session
        // summary line yet but surfaced here for callers who render the [x] count.
        Set<Integer> distinctChecks = new LinkedHashSet<>();
        for (ViolationRecord v : violations) distinctChecks.add(v.checkId());
        return out;
    }

    private static RenderedHistoryLine line(RenderedHistoryLine.Segment... segments) {
        return new RenderedHistoryLine(List.of(segments));
    }

    private static RenderedHistoryLine.Segment literal(String s) {
        return new RenderedHistoryLine.Segment.Literal(s);
    }

    private static RenderedHistoryLine.Segment styled(String s, RenderedHistoryLine.Color c) {
        return new RenderedHistoryLine.Segment.Styled(s, RenderedHistoryLine.Style.of(c));
    }

    private static String nullToUnknown(String s) {
        return s == null ? "unknown" : s;
    }
}
