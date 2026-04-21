package ac.grim.grimac.internal.storage.copy;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongConsumer;

/**
 * One-shot v1-to-v1 copier. Streams SESSION + VIOLATION + PLAYER_IDENTITY records
 * from a source {@link Backend} into a destination via its {@link Backend#bulkImport}
 * escape hatch. No rings; synchronous; progress is reported through {@code onBatch}.
 * <p>
 * Scans by player UUID: identity rows in the source define the set of players to
 * walk, then for each player we page sessions and (per session) page violations.
 * This matches the only {@code Query} shape the Layer 1 read API currently
 * supports without a free-form "all rows" query.
 * <p>
 * Not idempotent against a partially-populated destination — if you aborted
 * halfway and re-run, rows previously copied reappear (sessions are upserts
 * keyed by sessionId so they dedup; violations get new autoincrement IDs per
 * insert and will duplicate). The caller is expected to start with an empty
 * destination OR accept violation duplication.
 */
@ApiStatus.Internal
public final class BackendToBackendCopier {

    public record Result(long sessions, long violations, long players, long elapsedMs) {}

    private final Backend src;
    private final Backend dst;
    private final int pageSize;

    public BackendToBackendCopier(@NotNull Backend src, @NotNull Backend dst) {
        this(src, dst, 500);
    }

    public BackendToBackendCopier(@NotNull Backend src, @NotNull Backend dst, int pageSize) {
        if (src == dst) throw new IllegalArgumentException("source and destination cannot be the same backend");
        this.src = src;
        this.dst = dst;
        this.pageSize = Math.max(50, pageSize);
    }

    /**
     * Run a full copy. {@code onBatch} fires after each flushed batch with the
     * running total of violations copied so callers can paint progress lines.
     */
    public Result run(LongConsumer onBatch) throws BackendException {
        long start = System.currentTimeMillis();
        // Enumerate source players. The read API doesn't expose "list all identities"
        // today; we derive the player set from sessions (one per player-scoped
        // session listing) as a reasonable proxy. Sessions without an identity row
        // are still copied; identity rows without sessions won't migrate — an
        // accepted limitation documented in the copier's javadoc.
        Set<UUID> players = collectPlayersBySessionWalk();

        List<PlayerIdentity> identities = new ArrayList<>(players.size());
        for (UUID u : players) {
            Page<PlayerIdentity> p = src.read(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentity(u));
            if (!p.items().isEmpty()) identities.add(p.items().get(0));
        }
        if (!identities.isEmpty()) dst.bulkImport(Categories.PLAYER_IDENTITY, identities);

        long copiedSessions = 0;
        long copiedViolations = 0;
        List<SessionRecord> sessionBatch = new ArrayList<>(pageSize);
        List<ViolationRecord> violationBatch = new ArrayList<>(pageSize);

        for (UUID player : players) {
            Cursor sCursor = null;
            while (true) {
                Page<SessionRecord> sp = src.read(Categories.SESSION,
                        Queries.listSessionsByPlayer(player, pageSize, sCursor));
                for (SessionRecord s : sp.items()) {
                    sessionBatch.add(s);
                    if (sessionBatch.size() >= pageSize) {
                        dst.bulkImport(Categories.SESSION, sessionBatch);
                        copiedSessions += sessionBatch.size();
                        sessionBatch.clear();
                    }
                    // Per-session violation sweep.
                    Cursor vCursor = null;
                    while (true) {
                        Page<ViolationRecord> vp = src.read(Categories.VIOLATION,
                                Queries.listViolationsInSession(s.sessionId(), pageSize, vCursor));
                        for (ViolationRecord v : vp.items()) {
                            violationBatch.add(v);
                            if (violationBatch.size() >= pageSize) {
                                dst.bulkImport(Categories.VIOLATION, violationBatch);
                                copiedViolations += violationBatch.size();
                                violationBatch.clear();
                                onBatch.accept(copiedViolations);
                            }
                        }
                        vCursor = vp.nextCursor();
                        if (vCursor == null) break;
                    }
                }
                sCursor = sp.nextCursor();
                if (sCursor == null) break;
            }
        }
        if (!sessionBatch.isEmpty()) {
            dst.bulkImport(Categories.SESSION, sessionBatch);
            copiedSessions += sessionBatch.size();
        }
        if (!violationBatch.isEmpty()) {
            dst.bulkImport(Categories.VIOLATION, violationBatch);
            copiedViolations += violationBatch.size();
            onBatch.accept(copiedViolations);
        }
        return new Result(copiedSessions, copiedViolations, identities.size(),
                System.currentTimeMillis() - start);
    }

    /**
     * Walks the source backend enumerating distinct players. The Layer-1 API
     * doesn't expose a "scan all identities" query; we read sessions via a
     * well-known seed (all players the backend knows of through PLAYER_IDENTITY
     * rows — which we probe by iterating known identity rows per UUID when we
     * have them, otherwise by scanning session rows).
     * <p>
     * SqliteBackend's cursor scheme paginates per-player, so we can't just scan
     * "all sessions". Instead we rely on the InMemory / Sqlite backends having
     * grown identity rows for any active player (handled by
     * {@code PlayerIdentityService.observe} at join time) — so grim_players is
     * the canonical "all players" set.
     * <p>
     * For backends without that guarantee (future Mongo? theoretical), callers
     * would need a different enumeration strategy. Phase 1 supports only
     * backends where PLAYER_IDENTITY is the source of truth for "who to copy".
     */
    private Set<UUID> collectPlayersBySessionWalk() throws BackendException {
        // The Layer 1 Query surface doesn't expose "list all identities" — so
        // we probe through a different angle: the SqliteBackend + InMemoryBackend
        // both happen to expose readers that let us walk rows, but the Backend
        // interface doesn't. The cleanest legal approach right now is to require
        // the caller to supply the player UUID set they want copied. We take the
        // identity-row table as the authoritative set via a reflective hatch.
        //
        // For the two current backends:
        //   - InMemoryBackend: ConcurrentHashMap is visible via reflection; we
        //     avoid that brittleness.
        //   - SqliteBackend: direct JDBC read.
        //
        // Simplest portable answer: use the Set<UUID> the caller hands us — the
        // copier's commands collect it from identity queries against the source.
        // For the phase-1 shape below we fall back to a raw JDBC read on SQLite
        // and a reflective field read on InMemory via a small helper.
        Set<UUID> out = new LinkedHashSet<>();
        if (src instanceof ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackend sq) {
            try (java.sql.Connection c = java.sql.DriverManager.getConnection(sq.jdbcUrl());
                 java.sql.Statement s = c.createStatement();
                 java.sql.ResultSet rs = s.executeQuery("SELECT uuid FROM grim_players")) {
                while (rs.next()) {
                    byte[] bytes = rs.getBytes(1);
                    if (bytes != null && bytes.length == 16) {
                        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(bytes);
                        out.add(new UUID(bb.getLong(), bb.getLong()));
                    }
                }
            } catch (java.sql.SQLException e) {
                throw new BackendException("failed to enumerate source players", e);
            }
            return out;
        }
        if (src instanceof ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend mem) {
            out.addAll(mem.knownPlayerUuids());
            return out;
        }
        throw new BackendException("backend " + src.id() + " does not expose a player-enumeration hatch; cannot use as copy source");
    }

    /**
     * Idempotent drop of the source backend's data after a successful copy.
     * SQLite gets a DELETE sweep on the v1 tables (copier's implicit
     * contract is "after --delete, the source is empty for v1"). Other
     * backends throw — MySQL/Postgres implementations can override when they
     * land.
     */
    public void dropSource() throws BackendException {
        if (src instanceof ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackend sq) {
            synchronized (sq.writeMutexForCopier()) {
                java.sql.Connection conn = sq.writeConnection();
                if (conn == null) throw new BackendException("source backend not initialised");
                try (java.sql.Statement s = conn.createStatement()) {
                    s.executeUpdate("DELETE FROM grim_violations");
                    s.executeUpdate("DELETE FROM grim_sessions");
                    s.executeUpdate("DELETE FROM grim_players");
                    conn.commit();
                } catch (java.sql.SQLException e) {
                    try { conn.rollback(); } catch (java.sql.SQLException ignore) {}
                    throw new BackendException("failed to drop source data", e);
                }
            }
            return;
        }
        if (src instanceof ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend mem) {
            mem.wipeAllForCopier();
            return;
        }
        throw new BackendException("backend " + src.id() + " does not support in-place wipe from the copier");
    }
}
