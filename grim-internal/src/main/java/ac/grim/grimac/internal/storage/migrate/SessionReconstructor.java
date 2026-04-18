package ac.grim.grimac.internal.storage.migrate;

import ac.grim.grimac.api.storage.model.SessionRecord;
import org.jetbrains.annotations.ApiStatus;

import java.util.List;
import java.util.UUID;

/**
 * Groups a time-ordered stream of v0 violations into reconstructed sessions. The
 * caller feeds rows in ascending {@code (player_uuid, created_at)} order and receives
 * "open session" updates as the gap heuristic produces them.
 * <p>
 * Usage:
 * <pre>
 *   SessionReconstructor r = new SessionReconstructor(gapMs, lookup, listener);
 *   for (V0Violation v : stream) r.accept(v);
 *   r.flush();
 * </pre>
 */
@ApiStatus.Internal
public final class SessionReconstructor {

    public interface Emit {
        /** Called whenever the reconstructor finalises / updates a session. */
        void onSessionBoundary(SessionRecord session, List<ReconstructedViolation> violations);
    }

    public interface LookupLens {
        String server(int id);
        String checkName(int id);
        String grimVersion(int id);
        String clientBrand(int id);
        String clientVersion(int id);
        String serverVersion(int id);
    }

    public record ReconstructedViolation(
            long legacyId,
            UUID sessionId,
            UUID playerUuid,
            int checkId,
            double vl,
            long occurredEpochMs,
            String verbose) {}

    private final long gapMs;
    private final LookupLens lookup;
    private final Emit emit;
    private final CheckIdResolver checkIdResolver;

    private UUID currentPlayer;
    private SessionRecord currentSession;
    private java.util.List<ReconstructedViolation> pendingInSession = new java.util.ArrayList<>();

    public SessionReconstructor(long gapMs, LookupLens lookup, CheckIdResolver checkIdResolver, Emit emit) {
        this.gapMs = gapMs;
        this.lookup = lookup;
        this.checkIdResolver = checkIdResolver;
        this.emit = emit;
    }

    public void accept(V0Reader.V0Violation v) {
        if (currentPlayer == null || !currentPlayer.equals(v.playerUuid())) {
            flush();
            currentPlayer = v.playerUuid();
        }

        if (currentSession == null
                || v.createdAtEpochMs() - currentSession.lastActivityEpochMs() > gapMs
                || !sameServerContext(currentSession, v)) {
            if (currentSession != null) flushSession();
            currentSession = newSession(v);
        } else {
            currentSession = new SessionRecord(
                    currentSession.sessionId(),
                    currentSession.playerUuid(),
                    currentSession.serverName(),
                    currentSession.startedEpochMs(),
                    Math.max(currentSession.lastActivityEpochMs(), v.createdAtEpochMs()),
                    currentSession.grimVersion(),
                    currentSession.clientBrand(),
                    currentSession.clientVersionString(),
                    currentSession.serverVersionString(),
                    currentSession.replayClips());
        }

        String legacyName = lookup.checkName(v.checkNameId());
        int checkId = checkIdResolver.resolve(legacyName);
        pendingInSession.add(new ReconstructedViolation(
                v.legacyId(),
                currentSession.sessionId(),
                v.playerUuid(),
                checkId,
                v.vl(),
                v.createdAtEpochMs(),
                sanitizeVerbose(v.verbose())));
    }

    public void flush() {
        if (currentSession != null) flushSession();
        currentSession = null;
        currentPlayer = null;
    }

    private void flushSession() {
        emit.onSessionBoundary(currentSession, List.copyOf(pendingInSession));
        pendingInSession.clear();
    }

    private SessionRecord newSession(V0Reader.V0Violation v) {
        return new SessionRecord(
                UUID.randomUUID(),
                v.playerUuid(),
                lookup.server(v.serverId()),
                v.createdAtEpochMs(),
                v.createdAtEpochMs(),
                lookup.grimVersion(v.grimVersionId()),
                lookup.clientBrand(v.clientBrandId()),
                lookup.clientVersion(v.clientVersionId()),
                lookup.serverVersion(v.serverVersionId()),
                List.of());
    }

    private boolean sameServerContext(SessionRecord s, V0Reader.V0Violation v) {
        // Per §14 open question #1: sessions are scoped per (uuid, server_name). A
        // server change within the gap window still starts a new session.
        return s.serverName() == null
                || s.serverName().equals(lookup.server(v.serverId()));
    }

    private static String sanitizeVerbose(String verbose) {
        if (verbose == null) return null;
        int marker = verbose.indexOf(" /gl ");
        return marker >= 0 ? verbose.substring(0, marker) : verbose;
    }

    @FunctionalInterface
    public interface CheckIdResolver {
        int resolve(String legacyDisplayName);
    }
}
