package ac.grim.grimac.internal.storage.backend.memory;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.event.PlayerIdentityEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.event.SettingEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Deletes;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory reference backend. Used by tests and as a sanity-bar for other
 * backend implementations; also a useful target for operators who want to run
 * a category without persistence (e.g. ephemeral setting state). Not durable
 * — contents are lost at process exit.
 * <p>
 * {@link #eventHandlerFor(Category)} returns a thin handler per category that
 * materialises events into records and appends to the internal maps under a
 * shared write lock. Cross-category handlers on the same backend serialise
 * on {@code this}.
 */
@ApiStatus.Internal
public final class InMemoryBackend implements Backend {

    public static final String ID = "memory";

    public record Config() implements BackendConfig {}

    private final AtomicLong violationIdSeq = new AtomicLong(1);
    private final Object writeMutex = new Object();
    private final HashMap<UUID, List<ViolationRecord>> violationsBySession = new HashMap<>();
    private final HashMap<UUID, List<ViolationRecord>> violationsByPlayer = new HashMap<>();
    private final HashMap<UUID, SessionRecord> sessions = new HashMap<>();
    private final HashMap<UUID, List<SessionRecord>> sessionsByPlayer = new HashMap<>();
    private final HashMap<UUID, PlayerIdentity> identities = new HashMap<>();
    private final HashMap<String, PlayerIdentity> identityByName = new HashMap<>();
    private final HashMap<String, SettingRecord> settings = new HashMap<>();

    @Override public @NotNull String id() { return ID; }

    @Override public @NotNull ApiVersion getApiVersion() { return ApiVersion.CURRENT; }

    @Override
    public @NotNull java.util.EnumSet<Capability> capabilities() {
        return java.util.EnumSet.of(
                Capability.INDEXED_KV,
                Capability.TIMESERIES_APPEND,
                Capability.TTL,
                Capability.TRANSACTIONS,
                Capability.HISTORY,
                Capability.SETTINGS,
                Capability.PLAYER_IDENTITY);
    }

    @Override
    public @NotNull Set<Category<?>> supportedCategories() {
        return Set.of(Categories.VIOLATION, Categories.SESSION, Categories.PLAYER_IDENTITY, Categories.SETTING);
    }

    @Override public void init(@NotNull BackendContext ctx) {}

    @Override public void flush() {}

    @Override
    public void close() {
        synchronized (writeMutex) {
            violationsBySession.clear();
            violationsByPlayer.clear();
            sessions.clear();
            sessionsByPlayer.clear();
            identities.clear();
            identityByName.clear();
            settings.clear();
        }
    }

    /**
     * Copier-only hatch: snapshot of player UUIDs this backend knows about
     * (from identity rows). Used by {@code BackendToBackendCopier} to enumerate
     * players for the session/violation walk.
     */
    @ApiStatus.Internal
    public java.util.Set<UUID> knownPlayerUuids() {
        synchronized (writeMutex) {
            return new java.util.LinkedHashSet<>(identities.keySet());
        }
    }

    /**
     * Copier-only hatch: drop all v1 data while keeping the backend live.
     * Used by {@code --delete} after a successful copy to empty the source.
     */
    @ApiStatus.Internal
    public void wipeAllForCopier() {
        synchronized (writeMutex) {
            violationsBySession.clear();
            violationsByPlayer.clear();
            sessions.clear();
            sessionsByPlayer.clear();
            identities.clear();
            identityByName.clear();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <E> StorageEventHandler<E> eventHandlerFor(@NotNull Category<E> cat) {
        if (cat == Categories.VIOLATION) return (StorageEventHandler<E>) (StorageEventHandler<ViolationEvent>) (event, seq, endOfBatch) -> applyViolation(event);
        if (cat == Categories.SESSION) return (StorageEventHandler<E>) (StorageEventHandler<SessionEvent>) (event, seq, endOfBatch) -> applySession(event);
        if (cat == Categories.PLAYER_IDENTITY) return (StorageEventHandler<E>) (StorageEventHandler<PlayerIdentityEvent>) (event, seq, endOfBatch) -> applyIdentity(event);
        if (cat == Categories.SETTING) return (StorageEventHandler<E>) (StorageEventHandler<SettingEvent>) (event, seq, endOfBatch) -> applySetting(event);
        throw new IllegalArgumentException("unsupported category: " + cat.id());
    }

    private void applyViolation(ViolationEvent v) {
        long id = violationIdSeq.getAndIncrement();
        ViolationRecord stored = new ViolationRecord(
                id, v.sessionId(), v.playerUuid(), v.checkId(), v.vl(),
                v.occurredEpochMs(), v.verbose(), v.verboseFormat());
        synchronized (writeMutex) {
            violationsBySession.computeIfAbsent(stored.sessionId(), k -> new ArrayList<>()).add(stored);
            violationsByPlayer.computeIfAbsent(stored.playerUuid(), k -> new ArrayList<>()).add(stored);
        }
    }

    private void applySession(SessionEvent e) {
        synchronized (writeMutex) {
            SessionRecord prev = sessions.get(e.sessionId());
            // Preserve already-set closed_at across heartbeat upserts —
            // matches the SQL/Mongo/Redis NULL → set transition semantics.
            Long closedAt = e.closedAtEpochMs();
            if (prev != null && prev.closedAtEpochMs() != null) closedAt = prev.closedAtEpochMs();
            SessionRecord s = new SessionRecord(
                    e.sessionId(), e.playerUuid(), e.serverName(),
                    e.startedEpochMs(), e.lastActivityEpochMs(),
                    closedAt,
                    e.grimVersion(), e.clientBrand(), e.clientVersion(),
                    e.serverVersionString(), List.copyOf(e.replayClips()));
            sessions.put(s.sessionId(), s);
            if (prev != null) {
                List<SessionRecord> existing = sessionsByPlayer.get(prev.playerUuid());
                if (existing != null) existing.removeIf(r -> r.sessionId().equals(s.sessionId()));
            }
            sessionsByPlayer.computeIfAbsent(s.playerUuid(), k -> new ArrayList<>()).add(s);
        }
    }

    private void applyIdentity(PlayerIdentityEvent e) {
        synchronized (writeMutex) {
            PlayerIdentity prev = identities.get(e.uuid());
            long firstSeen = prev == null ? e.firstSeenEpochMs() : Math.min(prev.firstSeenEpochMs(), e.firstSeenEpochMs());
            long lastSeen = prev == null ? e.lastSeenEpochMs() : Math.max(prev.lastSeenEpochMs(), e.lastSeenEpochMs());
            PlayerIdentity merged = new PlayerIdentity(e.uuid(), e.currentName(), firstSeen, lastSeen);
            identities.put(e.uuid(), merged);
            if (e.currentName() != null) {
                identityByName.put(e.currentName().toLowerCase(java.util.Locale.ROOT), merged);
            }
        }
    }

    private void applySetting(SettingEvent e) {
        byte[] valueCopy = e.value().clone();
        SettingRecord s = new SettingRecord(e.scope(), e.scopeKey(), e.key(), valueCopy, e.updatedEpochMs());
        synchronized (writeMutex) {
            settings.put(settingKey(s.scope().name(), s.scopeKey(), s.key()), s);
        }
    }

    // --- direct write paths used by LegacyMigrator / tests only -------------

    @Override
    public <R> void bulkImport(@NotNull Category<?> cat, @NotNull List<R> records) throws BackendException {
        writeRecordsDirect(cat, records);
    }

    /** Package-private: bypass-ring record write for migration. */
    public synchronized void writeRecordsDirect(Category<?> cat, List<?> records) throws BackendException {
        synchronized (writeMutex) {
            for (Object r : records) {
                if (cat == Categories.VIOLATION) applyViolationRecord((ViolationRecord) r);
                else if (cat == Categories.SESSION) applySessionRecord((SessionRecord) r);
                else if (cat == Categories.PLAYER_IDENTITY) applyIdentityRecord((PlayerIdentity) r);
                else if (cat == Categories.SETTING) applySettingRecord((SettingRecord) r);
                else throw new BackendException("unsupported category: " + cat.id());
            }
        }
    }

    private void applyViolationRecord(ViolationRecord v) {
        long id = v.id() == 0 ? violationIdSeq.getAndIncrement() : v.id();
        ViolationRecord stored = new ViolationRecord(
                id, v.sessionId(), v.playerUuid(), v.checkId(), v.vl(),
                v.occurredEpochMs(), v.verbose(), v.verboseFormat());
        violationsBySession.computeIfAbsent(stored.sessionId(), k -> new ArrayList<>()).add(stored);
        violationsByPlayer.computeIfAbsent(stored.playerUuid(), k -> new ArrayList<>()).add(stored);
    }

    private void applySessionRecord(SessionRecord s) {
        SessionRecord prev = sessions.put(s.sessionId(), s);
        if (prev != null) {
            List<SessionRecord> existing = sessionsByPlayer.get(prev.playerUuid());
            if (existing != null) existing.removeIf(r -> r.sessionId().equals(s.sessionId()));
        }
        sessionsByPlayer.computeIfAbsent(s.playerUuid(), k -> new ArrayList<>()).add(s);
    }

    private void applyIdentityRecord(PlayerIdentity id) {
        PlayerIdentity prev = identities.get(id.uuid());
        long firstSeen = prev == null ? id.firstSeenEpochMs() : Math.min(prev.firstSeenEpochMs(), id.firstSeenEpochMs());
        long lastSeen = prev == null ? id.lastSeenEpochMs() : Math.max(prev.lastSeenEpochMs(), id.lastSeenEpochMs());
        PlayerIdentity merged = new PlayerIdentity(id.uuid(), id.currentName(), firstSeen, lastSeen);
        identities.put(id.uuid(), merged);
        if (id.currentName() != null) identityByName.put(id.currentName().toLowerCase(java.util.Locale.ROOT), merged);
    }

    private void applySettingRecord(SettingRecord s) {
        settings.put(settingKey(s.scope().name(), s.scopeKey(), s.key()), s);
    }

    private String settingKey(String scope, String scopeKey, String key) {
        return scope + "::" + scopeKey + "::" + key;
    }

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <R> Page<R> read(@NotNull Category<?> cat, @NotNull Query<R> query) throws BackendException {
        synchronized (writeMutex) {
            if (query instanceof Queries.ListSessionsByPlayer q) {
                List<SessionRecord> all = new ArrayList<>(sessionsByPlayer.getOrDefault(q.player(), List.of()));
                all.sort(Comparator.comparingLong(SessionRecord::startedEpochMs).reversed());
                int skip = decodeSkipCursor(q.cursor());
                int pageSize = q.pageSize();
                int end = Math.min(skip + pageSize, all.size());
                List<SessionRecord> slice = all.subList(skip, end);
                Cursor next = end < all.size() ? encodeSkipCursor(end) : null;
                return (Page<R>) new Page<>(slice, next);
            }
            if (query instanceof Queries.GetSessionById q) {
                SessionRecord s = sessions.get(q.sessionId());
                return (Page<R>) new Page<>(s == null ? List.of() : List.of(s), null);
            }
            if (query instanceof Queries.ListViolationsInSession q) {
                List<ViolationRecord> all = new ArrayList<>(violationsBySession.getOrDefault(q.sessionId(), List.of()));
                all.sort(Comparator.comparingLong(ViolationRecord::occurredEpochMs));
                int skip = decodeSkipCursor(q.cursor());
                int end = Math.min(skip + q.pageSize(), all.size());
                List<ViolationRecord> slice = all.subList(skip, end);
                Cursor next = end < all.size() ? encodeSkipCursor(end) : null;
                return (Page<R>) new Page<>(slice, next);
            }
            if (query instanceof Queries.GetPlayerIdentity q) {
                PlayerIdentity p = identities.get(q.uuid());
                return (Page<R>) new Page<>(p == null ? List.of() : List.of(p), null);
            }
            if (query instanceof Queries.GetPlayerIdentityByName q) {
                PlayerIdentity p = identityByName.get(q.name().toLowerCase(java.util.Locale.ROOT));
                return (Page<R>) new Page<>(p == null ? List.of() : List.of(p), null);
            }
            if (query instanceof Queries.ListPlayersByNamePrefix q) {
                String prefix = q.lowerPrefix();
                if (prefix == null || prefix.isEmpty() || q.limit() <= 0) {
                    return (Page<R>) Page.empty();
                }
                List<PlayerIdentity> out = new ArrayList<>();
                for (Map.Entry<String, PlayerIdentity> e : identityByName.entrySet()) {
                    if (e.getKey().startsWith(prefix)) out.add(e.getValue());
                }
                out.sort(Comparator.comparingLong(PlayerIdentity::lastSeenEpochMs).reversed());
                if (out.size() > q.limit()) out = out.subList(0, q.limit());
                return (Page<R>) new Page<>(out, null);
            }
            if (query instanceof Queries.GetSetting q) {
                SettingRecord s = settings.get(settingKey(q.scope().name(), q.scopeKey(), q.key()));
                return (Page<R>) new Page<>(s == null ? List.of() : List.of(s), null);
            }
            throw new BackendException("unsupported query: " + query.getClass().getSimpleName());
        }
    }

    private static int decodeSkipCursor(Cursor c) {
        if (c == null) return 0;
        try {
            return Integer.parseInt(c.token());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static Cursor encodeSkipCursor(int skip) {
        return new Cursor(Integer.toString(skip));
    }

    @Override
    public <E> void delete(@NotNull Category<E> cat, @NotNull DeleteCriteria criteria) {
        synchronized (writeMutex) {
            if (criteria instanceof Deletes.ByPlayer d) {
                UUID uuid = d.uuid();
                if (cat == Categories.VIOLATION) {
                    List<ViolationRecord> v = violationsByPlayer.remove(uuid);
                    if (v != null) {
                        for (ViolationRecord r : v) {
                            List<ViolationRecord> perSession = violationsBySession.get(r.sessionId());
                            if (perSession != null) perSession.removeIf(x -> x.id() == r.id());
                        }
                    }
                } else if (cat == Categories.SESSION) {
                    List<SessionRecord> s = sessionsByPlayer.remove(uuid);
                    if (s != null) for (SessionRecord r : s) sessions.remove(r.sessionId());
                } else if (cat == Categories.PLAYER_IDENTITY) {
                    PlayerIdentity p = identities.remove(uuid);
                    if (p != null && p.currentName() != null) identityByName.remove(p.currentName().toLowerCase(java.util.Locale.ROOT));
                } else if (cat == Categories.SETTING) {
                    settings.keySet().removeIf(k -> k.startsWith("PLAYER::" + uuid + "::"));
                }
            } else if (criteria instanceof Deletes.OlderThan d) {
                long cutoff = System.currentTimeMillis() - d.maxAgeMs();
                if (cat == Categories.SESSION) {
                    List<SessionRecord> toRemove = new ArrayList<>();
                    for (SessionRecord s : sessions.values()) if (s.startedEpochMs() < cutoff) toRemove.add(s);
                    for (SessionRecord s : toRemove) {
                        sessions.remove(s.sessionId());
                        sessionsByPlayer.getOrDefault(s.playerUuid(), List.of()).removeIf(x -> x.sessionId().equals(s.sessionId()));
                        violationsBySession.remove(s.sessionId());
                    }
                } else if (cat == Categories.VIOLATION) {
                    for (List<ViolationRecord> list : violationsBySession.values()) {
                        list.removeIf(r -> r.occurredEpochMs() < cutoff);
                    }
                    for (List<ViolationRecord> list : violationsByPlayer.values()) {
                        list.removeIf(r -> r.occurredEpochMs() < cutoff);
                    }
                }
            }
        }
    }

    @Override
    public long countViolationsInSession(@NotNull UUID sessionId) {
        synchronized (writeMutex) {
            return violationsBySession.getOrDefault(sessionId, List.of()).size();
        }
    }

    @Override
    public long countUniqueChecksInSession(@NotNull UUID sessionId) {
        synchronized (writeMutex) {
            java.util.List<ViolationRecord> list = violationsBySession.get(sessionId);
            if (list == null || list.isEmpty()) return 0L;
            java.util.HashSet<Integer> seen = new java.util.HashSet<>();
            for (ViolationRecord v : list) seen.add(v.checkId());
            return seen.size();
        }
    }

    @Override
    public long countSessionsByPlayer(@NotNull UUID player) {
        synchronized (writeMutex) {
            java.util.List<SessionRecord> list = sessionsByPlayer.get(player);
            return list == null ? 0L : list.size();
        }
    }

    @Override
    public long markCrashedSessions() {
        long affected = 0;
        synchronized (writeMutex) {
            for (java.util.Map.Entry<UUID, SessionRecord> e : sessions.entrySet()) {
                SessionRecord s = e.getValue();
                if (s.closedAtEpochMs() != null) continue;
                SessionRecord swept = new SessionRecord(
                        s.sessionId(), s.playerUuid(), s.serverName(),
                        s.startedEpochMs(), s.lastActivityEpochMs(),
                        s.lastActivityEpochMs(),
                        s.grimVersion(), s.clientBrand(), s.clientVersion(),
                        s.serverVersionString(), s.replayClips());
                e.setValue(swept);
                java.util.List<SessionRecord> byPlayer = sessionsByPlayer.get(s.playerUuid());
                if (byPlayer != null) {
                    for (int i = 0; i < byPlayer.size(); i++) {
                        if (byPlayer.get(i).sessionId().equals(s.sessionId())) {
                            byPlayer.set(i, swept);
                            break;
                        }
                    }
                }
                affected++;
            }
            return affected;
        }
    }
}
