package ac.grim.grimac.api.storage.history;

import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * High-level history facade. Returns pure data records — each platform's command
 * layer formats these into its own chat component model.
 * <p>
 * Phase-1 surface covers the two {@code /grim history} use cases (list sessions for a
 * player + show detail of one session). Rendering lives in Layer 3 (e.g. the 2.0
 * command module's {@code HistoryComponentRenderer}); this interface carries zero
 * chat-library types on purpose.
 */
@ApiStatus.Experimental
public interface HistoryService {

    /**
     * Paged listing of a player's sessions, newest first. Each summary's
     * {@code sessionOrdinal} is the global chronological 1-based position of the
     * session across the player's whole history — Session 1 is their very first
     * ever, Session K is their most recent — see {@link SessionSummary}.
     */
    @NotNull CompletionStage<@NotNull Page<SessionSummary>> listSessions(
            @NotNull UUID player,
            @Nullable Cursor cursor,
            int pageSize);

    /**
     * Detail view for one session. Returns {@code null} when the session does not
     * exist <em>or</em> belongs to a different player — collapsing those two cases
     * keeps the surface minimal; commands surface a single "not found" message.
     */
    @NotNull CompletionStage<@Nullable SessionDetail> getSessionDetail(
            @NotNull UUID player,
            @NotNull UUID sessionId);

    /**
     * Total number of sessions ever recorded for {@code player}. Callers use it
     * to compute the {@code [page / maxPages]} label in the session-list view.
     */
    @NotNull CompletionStage<@NotNull Long> countSessions(@NotNull UUID player);
}
