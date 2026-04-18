package ac.grim.grimac.internal.storage.backend.memory;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory reference backend. Ships day 1 per §10 / brief. Used by tests and as a
 * sanity-bar for other backend impls. Not persistent.
 */
@ApiStatus.Internal
public final class InMemoryBackend implements Backend {

    public static final String ID = "memory";

    public record Config() implements BackendConfig {}

    private final AtomicLong violationIdSeq = new AtomicLong(1);
    private final Map<UUID, List<ViolationRecord>> violationsBySession = new HashMap<>();
    private final Map<UUID, List<ViolationRecord>> violationsByPlayer = new HashMap<>();
    private final Map<UUID, SessionRecord> sessions = new HashMap<>();
    private final Map<UUID, List<SessionRecord>> sessionsByPlayer = new HashMap<>();
    private final Map<UUID, PlayerIdentity> identities = new HashMap<>();
    private final Map<String, PlayerIdentity> identityByName = new HashMap<>();
    private final Map<String, SettingRecord> settings = new HashMap<>();

    @Override
    public String id() {
        return ID;
    }

    @Override
    public ApiVersion getApiVersion() {
        return ApiVersion.CURRENT;
    }

    @Override
    public java.util.EnumSet<Capability> capabilities() {
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
    public Set<Category<?>> supportedCategories() {
        return Set.of(Categories.VIOLATION, Categories.SESSION, Categories.PLAYER_IDENTITY, Categories.SETTING);
    }

    @Override
    public synchronized void init(BackendContext ctx) {
        // nothing — in-memory
    }

    @Override
    public synchronized void flush() {
        // nothing — in-memory
    }

    @Override
    public synchronized void close() {
        violationsBySession.clear();
        violationsByPlayer.clear();
        sessions.clear();
        sessionsByPlayer.clear();
        identities.clear();
        identityByName.clear();
        settings.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <R> void writeBatch(Category<R> cat, List<R> records) {
        for (R r : records) {
            if (cat == Categories.VIOLATION) writeViolation((ViolationRecord) r);
            else if (cat == Categories.SESSION) writeSession((SessionRecord) r);
            else if (cat == Categories.PLAYER_IDENTITY) writeIdentity((PlayerIdentity) r);
            else if (cat == Categories.SETTING) writeSetting((SettingRecord) r);
            else throw new IllegalArgumentException("unsupported category: " + cat.id());
        }
    }

    private void writeViolation(ViolationRecord v) {
        long id = v.id() == 0 ? violationIdSeq.getAndIncrement() : v.id();
        ViolationRecord stored = new ViolationRecord(
                id, v.sessionId(), v.playerUuid(), v.checkId(), v.vl(),
                v.occurredEpochMs(), v.verbose(), v.verboseFormat());
        violationsBySession.computeIfAbsent(stored.sessionId(), k -> new ArrayList<>()).add(stored);
        violationsByPlayer.computeIfAbsent(stored.playerUuid(), k -> new ArrayList<>()).add(stored);
    }

    private void writeSession(SessionRecord s) {
        SessionRecord prev = sessions.put(s.sessionId(), s);
        if (prev != null) {
            sessionsByPlayer.get(prev.playerUuid()).removeIf(r -> r.sessionId().equals(s.sessionId()));
        }
        sessionsByPlayer.computeIfAbsent(s.playerUuid(), k -> new ArrayList<>()).add(s);
    }

    private void writeIdentity(PlayerIdentity id) {
        PlayerIdentity prev = identities.get(id.uuid());
        long firstSeen = prev == null ? id.firstSeenEpochMs() : Math.min(prev.firstSeenEpochMs(), id.firstSeenEpochMs());
        long lastSeen = prev == null ? id.lastSeenEpochMs() : Math.max(prev.lastSeenEpochMs(), id.lastSeenEpochMs());
        PlayerIdentity merged = new PlayerIdentity(id.uuid(), id.currentName(), firstSeen, lastSeen);
        identities.put(id.uuid(), merged);
        if (id.currentName() != null) identityByName.put(id.currentName().toLowerCase(java.util.Locale.ROOT), merged);
    }

    private void writeSetting(SettingRecord s) {
        settings.put(settingKey(s.scope().name(), s.scopeKey(), s.key()), s);
    }

    private String settingKey(String scope, String scopeKey, String key) {
        return scope + "::" + scopeKey + "::" + key;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <R> Page<R> read(Category<R> cat, Query<R> query) throws BackendException {
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
        if (query instanceof Queries.GetSetting q) {
            SettingRecord s = settings.get(settingKey(q.scope().name(), q.scopeKey(), q.key()));
            return (Page<R>) new Page<>(s == null ? List.of() : List.of(s), null);
        }
        throw new BackendException("unsupported query: " + query.getClass().getSimpleName());
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
    public synchronized <R> void delete(Category<R> cat, DeleteCriteria criteria) {
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

    @Override
    public synchronized long countViolationsInSession(UUID sessionId) {
        return violationsBySession.getOrDefault(sessionId, List.of()).size();
    }
}
