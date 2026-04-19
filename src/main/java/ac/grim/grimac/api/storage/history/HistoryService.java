package ac.grim.grimac.api.storage.history;

import ac.grim.grimac.api.storage.query.Cursor;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * High-level history facade. Returns {@link RenderedHistoryLine} — platform-neutral
 * structured output that each platform's command layer converts to its chat format.
 * <p>
 * Phase-1 surface covers the two {@code /grim history} use cases (list sessions for a
 * player + show detail of one session).
 */
@ApiStatus.Experimental
public interface HistoryService {

    @NotNull CompletionStage<SessionListResult> renderSessionList(@NotNull UUID player, @Nullable Cursor cursor, int pageSize);

    @NotNull CompletionStage<List<RenderedHistoryLine>> renderSessionDetail(@NotNull UUID player, @NotNull UUID sessionId, @NotNull RenderOptions opts);

    record SessionListResult(@NotNull List<RenderedHistoryLine> lines, @Nullable Cursor nextCursor, int totalPagesHint) {

        public SessionListResult {
            lines = lines == null ? List.of() : List.copyOf(lines);
        }
    }
}
