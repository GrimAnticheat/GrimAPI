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
 * Client-version mapping is delegated to a caller-supplied
 * {@link ClientVersionResolver}. The v0 store kept client versions as free-form
 * strings (e.g. {@code "1.21.1"}); v1 stores PacketEvents protocol-version
 * numbers. The resolver lives in the host plugin module (where PacketEvents
 * is on the classpath); this code stays PE-free and accepts {@code -1} for
 * strings the resolver couldn't map.
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
    private final ClientVersionResolver clientVersionResolver;

    private UUID currentPlayer;
    private SessionRecord currentSession;
    private java.util.List<ReconstructedViolation> pendingInSession = new java.util.ArrayList<>();

    public SessionReconstructor(long gapMs, LookupLens lookup,
                                CheckIdResolver checkIdResolver,
                                ClientVersionResolver clientVersionResolver,
                                Emit emit) {
        this.gapMs = gapMs;
        this.lookup = lookup;
        this.checkIdResolver = checkIdResolver;
        this.clientVersionResolver = clientVersionResolver;
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
                    currentSession.clientVersion(),
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
        String clientVersionStr = lookup.clientVersion(v.clientVersionId());
        int clientVersionPvn = clientVersionResolver.resolve(clientVersionStr);
        return new SessionRecord(
                UUID.randomUUID(),
                v.playerUuid(),
                lookup.server(v.serverId()),
                v.createdAtEpochMs(),
                v.createdAtEpochMs(),
                lookup.grimVersion(v.grimVersionId()),
                lookup.clientBrand(v.clientBrandId()),
                clientVersionPvn,
                lookup.serverVersion(v.serverVersionId()),
                List.of());
    }

    private boolean sameServerContext(SessionRecord s, V0Reader.V0Violation v) {
        // Sessions are scoped per (uuid, server_name). A server change
        // within the gap window still starts a new session.
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

    /**
     * Maps a legacy v0 client-version string (e.g. {@code "1.21.1"}) to a
     * PacketEvents protocol-version number. Return {@code -1} when the string
     * doesn't resolve to any known {@code ClientVersion} enum value.
     */
    @FunctionalInterface
    public interface ClientVersionResolver {
        int resolve(String legacyClientVersionString);
    }
}
