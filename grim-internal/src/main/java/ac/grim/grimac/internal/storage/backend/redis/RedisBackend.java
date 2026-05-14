package ac.grim.grimac.internal.storage.backend.redis;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.api.storage.event.PlayerIdentityEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.event.SettingEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.SettingScope;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Deletes;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.api.storage.query.Query;
import ac.grim.grimac.internal.storage.util.UuidCodec;
import ac.grim.grimac.internal.storage.util.UuidV7;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.args.ListDirection;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.Tuple;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Redis 6+ backend. Operational caveats first:
 * <ul>
 *   <li>Redis is <em>primarily an in-memory store</em>. Persistence relies on
 *       AOF/RDB — a misconfigured server will lose data. This backend is
 *       offered for operators who only have Redis to work with and need a
 *       working history surface in a pinch, not as a primary production
 *       choice. The other backends (SQLite/MySQL/Postgres/Mongo) are a
 *       better default for history.</li>
 *   <li>When {@link RedisBackendConfig#warnOnHistory()} is true (default),
 *       {@link #init(BackendContext)} logs a warning if the user is shipping
 *       this as the violation/session backend. Toggle it off once you have
 *       confirmed your operator is aware.</li>
 * </ul>
 * Storage layout (all keys prefixed with {@code config.keyPrefix()}):
 * <ul>
 *   <li>{@code <prefix><meta>}: hash of schema_version + core_version + counters.</li>
 *   <li>{@code <prefix><players>:<uuid-hex>}: per-player hash.</li>
 *   <li>{@code <prefix><players>:by-name-lower:<lc-name>}: string → current uuid-hex.</li>
 *   <li>{@code <prefix><sessions>:<session-hex>}: per-session hash.</li>
 *   <li>{@code <prefix><sessions>:by-player:<uuid-hex>}: ZSET, score=startedAt, member=session-hex.</li>
 *   <li>{@code <prefix><violations>:<uuid-hex>}: per-violation hash.</li>
 *   <li>{@code <prefix><violations>:by-session:<session-hex>}: ZSET, score=occurredAt, member=uuid-hex.</li>
 *   <li>{@code <prefix><violations>:by-player:<uuid-hex>}: ZSET, score=occurredAt, member=uuid-hex.</li>
 *   <li>{@code <prefix><settings>:<scope>:<scope_key>:<key>}: hash holding value+updated_at.</li>
 * </ul>
 * Violation ids are UUIDv7 minted producer-side by {@code ViolationSinkImpl};
 * the timestamp prefix means they're k-sortable and the lookup keys stay
 * append-friendly to Redis's hash index.
 */
@ApiStatus.Internal
public final class RedisBackend implements Backend {

    public static final String ID = "redis";

    /** Bumped on every breaking shape change. {@code migrateSchema} walks forward steps. */
    static final int CURRENT_SCHEMA_VERSION = 6;

    private final RedisBackendConfig config;
    private JedisPool pool;
    private Logger logger;

    public RedisBackend(RedisBackendConfig config) {
        this.config = config;
    }

    @Override public @NotNull String id() { return ID; }
    @Override public @NotNull ApiVersion getApiVersion() { return ApiVersion.CURRENT; }

    @Override
    public @NotNull EnumSet<Capability> capabilities() {
        return EnumSet.of(
                Capability.INDEXED_KV,
                Capability.SETTINGS,
                Capability.PLAYER_IDENTITY,
                Capability.HISTORY,
                Capability.TIMESERIES_APPEND);
    }

    @Override
    public @NotNull Set<Category<?>> supportedCategories() {
        return Set.of(
                Categories.VIOLATION,
                Categories.SESSION,
                Categories.PLAYER_IDENTITY,
                Categories.SETTING);
    }

    @Override
    public void init(@NotNull BackendContext ctx) throws BackendException {
        this.logger = ctx.logger();
        try {
            Class.forName("redis.clients.jedis.Jedis");
        } catch (ClassNotFoundException cnf) {
            throw new BackendException("jedis not on the classpath — shade it into the plugin jar or drop it into server/plugins", cnf);
        }
        DefaultJedisClientConfig.Builder b = DefaultJedisClientConfig.builder()
                .database(config.database())
                .timeoutMillis(config.timeoutMs());
        if (config.user() != null && !config.user().isEmpty()) b.user(config.user());
        if (config.password() != null && !config.password().isEmpty()) b.password(config.password());
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(16);
            this.pool = new JedisPool(poolConfig, new HostAndPort(config.host(), config.port()), b.build());
            try (Jedis j = pool.getResource()) {
                String existingVer = j.hget(metaKey(), "schema_version");
                if (existingVer == null) {
                    // Fresh DB — stamp the current version and we're done.
                    j.hset(metaKey(), Map.of(
                            "schema_version", Integer.toString(CURRENT_SCHEMA_VERSION),
                            "grim_core_version", "phase1"));
                } else {
                    int existing;
                    try { existing = Integer.parseInt(existingVer); }
                    catch (NumberFormatException nfe) { existing = 0; }
                    if (existing < CURRENT_SCHEMA_VERSION) migrateSchema(j, existing);
                }
            }
            if (config.warnOnHistory()) {
                // Pre-format the String so java.util.logging's MessageFormat
                // doesn't swallow the single-quoted interpolations.
                logger.warning("[grim-datastore] Redis backend '"
                        + config.keyPrefix() + config.tableNames().violations()
                        + "' is serving violation/session history. Redis is primarily an in-memory "
                        + "store — ensure AOF persistence is enabled, or this history will "
                        + "disappear on restart. Set " + ID + ".warn-on-history=false to silence.");
            }
        } catch (RuntimeException e) {
            throw new BackendException("failed to initialise Redis backend", e);
        }
    }

    /**
     * Forward-only Redis schema migration. {@code v5} ids were monotonic
     * longs minted via {@code HINCRBY meta violation_id_seq 1}; {@code v6}
     * makes them UUIDv7s minted producer-side. Each legacy violation row
     * gets a synthesised UUIDv7 derived from its {@code occurred_at}, and
     * the per-session / per-player ZSETs are rewritten with the new
     * member ids.
     */
    private void migrateSchema(Jedis j, int existing) {
        if (existing < 6) migrateV5ToV6(j);
        j.hset(metaKey(), "schema_version", Integer.toString(CURRENT_SCHEMA_VERSION));
        j.hdel(metaKey(), "violation_id_seq");
    }

    private void migrateV5ToV6(Jedis j) {
        logger.info("[grim-datastore] migrating Redis violations id long→UUIDv7 …");
        String prefix = tableKey(config.tableNames().violations()) + ":";
        String pattern = prefix + "*";
        ScanParams sp = new ScanParams().match(pattern).count(500);
        String cursor = ScanParams.SCAN_POINTER_START;
        long migrated = 0;
        do {
            redis.clients.jedis.resps.ScanResult<String> res = j.scan(cursor, sp);
            cursor = res.getCursor();
            for (String oldKey : res.getResult()) {
                // Skip the secondary indexes — only direct violation hashes
                // need rewriting.
                if (oldKey.contains(":by-session:") || oldKey.contains(":by-player:")) continue;
                Map<String, String> h = j.hgetAll(oldKey);
                if (h.isEmpty()) continue;
                String oldId = h.get("id");
                if (oldId == null) continue;
                // Already-migrated rows have a UUID-shaped id (32 hex chars).
                if (oldId.length() == 32 && oldId.matches("[0-9a-fA-F]+")) continue;
                String occurredStr = h.get("occurred_at");
                long occurred = occurredStr == null ? 0L : Long.parseLong(occurredStr);
                UUID newId = UuidV7.fromTimestampMs(occurred);
                String newIdHex = hex(newId);
                String newKey = prefix + newIdHex;
                String sessionHex = h.get("session_id");
                String playerHex = h.get("player_uuid");
                h.put("id", newIdHex);
                Pipeline p = j.pipelined();
                p.hset(newKey, h);
                p.del(oldKey);
                if (sessionHex != null) {
                    String zSession = tableKey(config.tableNames().violations()) + ":by-session:" + sessionHex;
                    p.zrem(zSession, oldId);
                    p.zadd(zSession, occurred, newIdHex);
                }
                if (playerHex != null) {
                    String zPlayer = tableKey(config.tableNames().violations()) + ":by-player:" + playerHex;
                    p.zrem(zPlayer, oldId);
                    p.zadd(zPlayer, occurred, newIdHex);
                }
                p.sync();
                migrated++;
            }
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        logger.info("[grim-datastore] migrated " + migrated + " violation rows to UUIDv7 id");
    }

    @Override public void flush() {}

    @Override
    public void close() throws BackendException {
        if (pool != null) {
            pool.close();
            pool = null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <E> StorageEventHandler<E> eventHandlerFor(@NotNull Category<E> cat) throws BackendException {
        StorageEventHandler<?> h;
        if (cat == Categories.VIOLATION) h = (StorageEventHandler<ViolationEvent>) this::writeViolation;
        else if (cat == Categories.SESSION) h = (StorageEventHandler<SessionEvent>) this::writeSession;
        else if (cat == Categories.PLAYER_IDENTITY) h = (StorageEventHandler<PlayerIdentityEvent>) this::writeIdentity;
        else if (cat == Categories.SETTING) h = (StorageEventHandler<SettingEvent>) this::writeSetting;
        else throw new IllegalArgumentException("unsupported category: " + cat.id());
        return (StorageEventHandler<E>) h;
    }

    // --- write handlers ------------------------------------------------------

    private void writeViolation(ViolationEvent v, long sequence, boolean endOfBatch) throws BackendException {
        try (Jedis j = pool.getResource()) {
            UUID id = v.id();
            String idHex = hex(id);
            String sessionHex = hex(v.sessionId());
            String playerHex = hex(v.playerUuid());
            String recordKey = tableKey(config.tableNames().violations()) + ":" + idHex;
            Map<String, String> h = new HashMap<>();
            h.put("id", idHex);
            h.put("session_id", sessionHex);
            h.put("player_uuid", playerHex);
            h.put("check_id", Integer.toString(v.checkId()));
            h.put("vl", Double.toString(v.vl()));
            h.put("occurred_at", Long.toString(v.occurredEpochMs()));
            if (v.verbose() != null) h.put("verbose", v.verbose());
            h.put("verbose_format", Integer.toString(v.verboseFormat().code()));
            Pipeline p = j.pipelined();
            p.hset(recordKey, h);
            p.zadd(tableKey(config.tableNames().violations()) + ":by-session:" + sessionHex, v.occurredEpochMs(), idHex);
            p.zadd(tableKey(config.tableNames().violations()) + ":by-player:" + playerHex, v.occurredEpochMs(), idHex);
            p.sync();
        } catch (RuntimeException e) {
            throw new BackendException("violation write failed", e);
        }
    }

    private void writeSession(SessionEvent s, long sequence, boolean endOfBatch) throws BackendException {
        if (!s.sessionBlobs().isEmpty()) {
            throw new BackendException("session-blob serialisation isn't implemented by this backend");
        }
        try (Jedis j = pool.getResource()) {
            String sessionHex = hex(s.sessionId());
            String playerHex = hex(s.playerUuid());
            String key = tableKey(config.tableNames().sessions()) + ":" + sessionHex;
            Map<String, String> h = new HashMap<>();
            h.put("session_id", sessionHex);
            h.put("player_uuid", playerHex);
            if (s.serverName() != null) h.put("server_name", s.serverName());
            h.put("started_at", Long.toString(s.startedEpochMs()));
            h.put("last_activity", Long.toString(s.lastActivityEpochMs()));
            // closed_at: only write when the event has it (close path).
            // Heartbeats with null closed_at don't HSET the field, so any
            // previously-written value survives. Same NULL → set transition
            // semantics as the SQL backends.
            if (s.closedAtEpochMs() != null) h.put("closed_at", Long.toString(s.closedAtEpochMs()));
            if (s.grimVersion() != null) h.put("grim_version", s.grimVersion());
            if (s.clientBrand() != null) h.put("client_brand", s.clientBrand());
            h.put("client_version_pvn", Integer.toString(s.clientVersion()));
            if (s.serverVersionString() != null) h.put("server_version", s.serverVersionString());
            Pipeline p = j.pipelined();
            p.hset(key, h);
            p.zadd(tableKey(config.tableNames().sessions()) + ":by-player:" + playerHex,
                    s.startedEpochMs(), sessionHex);
            p.sync();
        } catch (RuntimeException e) {
            throw new BackendException("session write failed", e);
        }
    }

    private void writeIdentity(PlayerIdentityEvent e, long sequence, boolean endOfBatch) throws BackendException {
        try (Jedis j = pool.getResource()) {
            String hex = hex(e.uuid());
            String key = tableKey(config.tableNames().players()) + ":" + hex;
            Map<String, String> existing = j.hgetAll(key);
            long firstSeen = existing.get("first_seen") == null ? e.firstSeenEpochMs()
                    : Math.min(e.firstSeenEpochMs(), Long.parseLong(existing.get("first_seen")));
            long lastSeen = existing.get("last_seen") == null ? e.lastSeenEpochMs()
                    : Math.max(e.lastSeenEpochMs(), Long.parseLong(existing.get("last_seen")));
            Map<String, String> h = new HashMap<>();
            h.put("uuid", hex);
            if (e.currentName() != null) h.put("current_name", e.currentName());
            h.put("first_seen", Long.toString(firstSeen));
            h.put("last_seen", Long.toString(lastSeen));
            Pipeline p = j.pipelined();
            p.hset(key, h);
            if (e.currentName() != null) {
                p.set(tableKey(config.tableNames().players()) + ":by-name-lower:"
                        + e.currentName().toLowerCase(Locale.ROOT), hex);
            }
            p.sync();
        } catch (RuntimeException e2) {
            throw new BackendException("identity write failed", e2);
        }
    }

    private void writeSetting(SettingEvent s, long sequence, boolean endOfBatch) throws BackendException {
        try (Jedis j = pool.getResource()) {
            String key = tableKey(config.tableNames().settings()) + ":"
                    + s.scope().name() + ":" + s.scopeKey() + ":" + s.key();
            Map<String, String> h = new HashMap<>();
            h.put("scope", s.scope().name());
            h.put("scope_key", s.scopeKey());
            h.put("key", s.key());
            h.put("value_b64", Base64.getEncoder().encodeToString(s.value()));
            h.put("updated_at", Long.toString(s.updatedEpochMs()));
            j.hset(key, h);
        } catch (RuntimeException e) {
            throw new BackendException("setting write failed", e);
        }
    }

    // --- bulk-load path ------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public <R> void bulkImport(@NotNull Category<?> cat, @NotNull List<R> records) throws BackendException {
        if (records.isEmpty()) return;
        try {
            if (cat == Categories.VIOLATION) for (ViolationRecord v : (List<ViolationRecord>) records) bulkViolation(v);
            else if (cat == Categories.SESSION) for (SessionRecord s : (List<SessionRecord>) records) bulkSession(s);
            else if (cat == Categories.PLAYER_IDENTITY) for (PlayerIdentity p : (List<PlayerIdentity>) records) bulkIdentity(p);
            else if (cat == Categories.SETTING) for (SettingRecord s : (List<SettingRecord>) records) bulkSetting(s);
            else throw new BackendException("unsupported category: " + cat.id());
        } catch (RuntimeException e) {
            throw new BackendException("bulkImport failed for " + cat.id(), e);
        }
    }

    private void bulkViolation(ViolationRecord v) {
        try (Jedis j = pool.getResource()) {
            // ViolationRecord.id is non-null by record contract — cross-backend
            // bulkImport always carries an id through verbatim.
            String idHex = hex(v.id());
            String sessionHex = hex(v.sessionId());
            String playerHex = hex(v.playerUuid());
            String key = tableKey(config.tableNames().violations()) + ":" + idHex;
            Map<String, String> h = new HashMap<>();
            h.put("id", idHex);
            h.put("session_id", sessionHex);
            h.put("player_uuid", playerHex);
            h.put("check_id", Integer.toString(v.checkId()));
            h.put("vl", Double.toString(v.vl()));
            h.put("occurred_at", Long.toString(v.occurredEpochMs()));
            if (v.verbose() != null) h.put("verbose", v.verbose());
            h.put("verbose_format", Integer.toString(v.verboseFormat().code()));
            Pipeline p = j.pipelined();
            p.hset(key, h);
            p.zadd(tableKey(config.tableNames().violations()) + ":by-session:" + sessionHex, v.occurredEpochMs(), idHex);
            p.zadd(tableKey(config.tableNames().violations()) + ":by-player:" + playerHex, v.occurredEpochMs(), idHex);
            p.sync();
        }
    }

    private void bulkSession(SessionRecord s) {
        try (Jedis j = pool.getResource()) {
            String sessionHex = hex(s.sessionId());
            String playerHex = hex(s.playerUuid());
            String key = tableKey(config.tableNames().sessions()) + ":" + sessionHex;
            Map<String, String> h = new HashMap<>();
            h.put("session_id", sessionHex);
            h.put("player_uuid", playerHex);
            if (s.serverName() != null) h.put("server_name", s.serverName());
            h.put("started_at", Long.toString(s.startedEpochMs()));
            h.put("last_activity", Long.toString(s.lastActivityEpochMs()));
            if (s.closedAtEpochMs() != null) h.put("closed_at", Long.toString(s.closedAtEpochMs()));
            if (s.grimVersion() != null) h.put("grim_version", s.grimVersion());
            if (s.clientBrand() != null) h.put("client_brand", s.clientBrand());
            h.put("client_version_pvn", Integer.toString(s.clientVersion()));
            if (s.serverVersionString() != null) h.put("server_version", s.serverVersionString());
            Pipeline p = j.pipelined();
            p.hset(key, h);
            p.zadd(tableKey(config.tableNames().sessions()) + ":by-player:" + playerHex,
                    s.startedEpochMs(), sessionHex);
            p.sync();
        }
    }

    private void bulkIdentity(PlayerIdentity id) {
        try (Jedis j = pool.getResource()) {
            String hex = hex(id.uuid());
            String key = tableKey(config.tableNames().players()) + ":" + hex;
            Map<String, String> h = new HashMap<>();
            h.put("uuid", hex);
            if (id.currentName() != null) h.put("current_name", id.currentName());
            h.put("first_seen", Long.toString(id.firstSeenEpochMs()));
            h.put("last_seen", Long.toString(id.lastSeenEpochMs()));
            Pipeline p = j.pipelined();
            p.hset(key, h);
            if (id.currentName() != null) {
                p.set(tableKey(config.tableNames().players()) + ":by-name-lower:"
                        + id.currentName().toLowerCase(Locale.ROOT), hex);
            }
            p.sync();
        }
    }

    private void bulkSetting(SettingRecord s) {
        try (Jedis j = pool.getResource()) {
            String key = tableKey(config.tableNames().settings()) + ":"
                    + s.scope().name() + ":" + s.scopeKey() + ":" + s.key();
            Map<String, String> h = new HashMap<>();
            h.put("scope", s.scope().name());
            h.put("scope_key", s.scopeKey());
            h.put("key", s.key());
            h.put("value_b64", Base64.getEncoder().encodeToString(s.value()));
            h.put("updated_at", Long.toString(s.updatedEpochMs()));
            j.hset(key, h);
        }
    }

    // --- read path -----------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <R> Page<R> read(@NotNull Category<?> cat, @NotNull Query<R> query) throws BackendException {
        try (Jedis j = pool.getResource()) {
            if (query instanceof Queries.ListSessionsByPlayer q) return (Page<R>) listSessionsByPlayer(j, q);
            if (query instanceof Queries.GetSessionById q) return (Page<R>) getSessionById(j, q);
            if (query instanceof Queries.ListViolationsInSession q) return (Page<R>) listViolationsInSession(j, q);
            if (query instanceof Queries.GetPlayerIdentity q) return (Page<R>) getPlayerIdentity(j, q);
            if (query instanceof Queries.GetPlayerIdentityByName q) return (Page<R>) getPlayerIdentityByName(j, q);
            if (query instanceof Queries.ListPlayersByNamePrefix q) return (Page<R>) listPlayersByNamePrefix(j, q);
            if (query instanceof Queries.GetSetting q) return (Page<R>) getSetting(j, q);
            throw new BackendException("unsupported query: " + query.getClass().getSimpleName());
        } catch (RuntimeException e) {
            throw new BackendException("read failed", e);
        }
    }

    private Page<SessionRecord> listSessionsByPlayer(Jedis j, Queries.ListSessionsByPlayer q) {
        long cursorStarted = decodeStartedCursor(q.cursor(), Long.MAX_VALUE);
        String zsetKey = tableKey(config.tableNames().sessions()) + ":by-player:" + hex(q.player());
        // ZREVRANGEBYSCORE [max, min) ordered highest-score first. For page 1,
        // max=+inf; for subsequent pages, max = cursorStarted (exclusive).
        String max = q.cursor() == null ? "+inf" : "(" + cursorStarted;
        List<Tuple> members = j.zrevrangeByScoreWithScores(zsetKey, max, "-inf", 0, q.pageSize() + 1);
        List<SessionRecord> out = new ArrayList<>();
        boolean hasMore = false;
        for (Tuple t : members) {
            if (out.size() >= q.pageSize()) { hasMore = true; break; }
            Map<String, String> h = j.hgetAll(tableKey(config.tableNames().sessions()) + ":" + t.getElement());
            if (h.isEmpty()) continue;
            out.add(mapSession(h));
        }
        Cursor next = null;
        if (hasMore && !out.isEmpty()) {
            SessionRecord last = out.get(out.size() - 1);
            next = encodeStartedCursor(last.startedEpochMs(), last.sessionId());
        }
        return new Page<>(out, next);
    }

    private Page<SessionRecord> getSessionById(Jedis j, Queries.GetSessionById q) {
        Map<String, String> h = j.hgetAll(tableKey(config.tableNames().sessions()) + ":" + hex(q.sessionId()));
        return h.isEmpty() ? Page.empty() : new Page<>(List.of(mapSession(h)), null);
    }

    private Page<ViolationRecord> listViolationsInSession(Jedis j, Queries.ListViolationsInSession q) {
        // ZSET score is occurred_at, member is the violation's UUIDv7 hex.
        // Cursor carries both (score, id) so we can resume mid-burst: exclusive
        // lower bound on score alone would skip every other row sharing the
        // boundary score. Inclusive lower bound + in-client filter on (score,
        // member) gets us back to (occurred_at, id) lexicographic ordering.
        long cursorScore = decodeViolationOccurredCursor(q.cursor(), Long.MIN_VALUE);
        UUID cursorId = decodeViolationIdCursor(q.cursor());
        String zsetKey = tableKey(config.tableNames().violations()) + ":by-session:" + hex(q.sessionId());
        String min = cursorId == null ? "-inf" : Long.toString(cursorScore);
        String cursorMemberHex = cursorId == null ? null : hex(cursorId);
        // Fetch pageSize+1 *past* the cursor — but at the boundary score the
        // ZSET may return rows we already shipped on the previous page, so we
        // skip them in client until we move past (score, member). Worst case
        // (huge same-score burst) is a couple extra ZRANGE calls; in practice
        // bursts at a single ms are bounded by ring throughput.
        List<ViolationRecord> out = new ArrayList<>();
        boolean hasMore = false;
        boolean skipping = cursorId != null;
        long offset = 0;
        int wanted = q.pageSize() + 1;
        while (out.size() < wanted) {
            List<Tuple> members = j.zrangeByScoreWithScores(zsetKey, min, "+inf", (int) offset, wanted - out.size());
            if (members.isEmpty()) break;
            offset += members.size();
            for (Tuple t : members) {
                if (skipping) {
                    if ((long) t.getScore() == cursorScore && t.getElement().compareTo(cursorMemberHex) <= 0) continue;
                    skipping = false;
                }
                if (out.size() >= q.pageSize()) { hasMore = true; break; }
                Map<String, String> h = j.hgetAll(tableKey(config.tableNames().violations()) + ":" + t.getElement());
                if (h.isEmpty()) continue;
                out.add(mapViolation(h));
            }
            if (hasMore || members.size() < wanted - out.size()) break;
        }
        Cursor next = null;
        if (hasMore && !out.isEmpty()) {
            ViolationRecord last = out.get(out.size() - 1);
            next = encodeViolationCursor(last.occurredEpochMs(), last.id());
        }
        return new Page<>(out, next);
    }

    private Page<PlayerIdentity> getPlayerIdentity(Jedis j, Queries.GetPlayerIdentity q) {
        Map<String, String> h = j.hgetAll(tableKey(config.tableNames().players()) + ":" + hex(q.uuid()));
        return h.isEmpty() ? Page.empty() : new Page<>(List.of(mapIdentity(h)), null);
    }

    private Page<PlayerIdentity> getPlayerIdentityByName(Jedis j, Queries.GetPlayerIdentityByName q) {
        String hexId = j.get(tableKey(config.tableNames().players()) + ":by-name-lower:"
                + q.name().toLowerCase(Locale.ROOT));
        if (hexId == null) return Page.empty();
        Map<String, String> h = j.hgetAll(tableKey(config.tableNames().players()) + ":" + hexId);
        return h.isEmpty() ? Page.empty() : new Page<>(List.of(mapIdentity(h)), null);
    }

    private Page<PlayerIdentity> listPlayersByNamePrefix(Jedis j, Queries.ListPlayersByNamePrefix q) {
        String prefix = q.lowerPrefix();
        if (prefix == null || prefix.isEmpty() || q.limit() <= 0) return Page.empty();
        // SCAN keyspace for <players>:by-name-lower:<prefix>* — Redis glob-escape
        // the user-supplied chars so '*'/'?'/'[' in a name don't expand the match.
        String pattern = tableKey(config.tableNames().players()) + ":by-name-lower:" + globEscape(prefix) + "*";
        redis.clients.jedis.params.ScanParams sp = new redis.clients.jedis.params.ScanParams()
                .match(pattern)
                .count(Math.min(Math.max(q.limit() * 4, 50), 500));
        // Worst case: SCAN visits every key in the players index. Cap iteration
        // count so an enormous keyspace doesn't make tab-completion hang. Sort
        // the candidates we DID see by last_seen DESC and trim to limit.
        java.util.List<PlayerIdentity> out = new java.util.ArrayList<>();
        int maxCandidates = Math.max(q.limit() * 8, 200);
        String cursor = redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;
        do {
            redis.clients.jedis.resps.ScanResult<String> r = j.scan(cursor, sp);
            cursor = r.getCursor();
            for (String key : r.getResult()) {
                String hexId = j.get(key);
                if (hexId == null) continue;
                Map<String, String> h = j.hgetAll(tableKey(config.tableNames().players()) + ":" + hexId);
                if (h.isEmpty()) continue;
                out.add(mapIdentity(h));
                if (out.size() >= maxCandidates) break;
            }
            if (out.size() >= maxCandidates) break;
        } while (!cursor.equals(redis.clients.jedis.params.ScanParams.SCAN_POINTER_START));
        out.sort(java.util.Comparator.comparingLong(PlayerIdentity::lastSeenEpochMs).reversed());
        if (out.size() > q.limit()) out = out.subList(0, q.limit());
        return new Page<>(out, null);
    }

    private static String globEscape(String s) {
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '*' || c == '?' || c == '[' || c == ']' || c == '\\') out.append('\\');
            out.append(c);
        }
        return out.toString();
    }

    private Page<SettingRecord> getSetting(Jedis j, Queries.GetSetting q) {
        Map<String, String> h = j.hgetAll(tableKey(config.tableNames().settings()) + ":"
                + q.scope().name() + ":" + q.scopeKey() + ":" + q.key());
        return h.isEmpty() ? Page.empty() : new Page<>(List.of(mapSetting(h)), null);
    }

    private static SessionRecord mapSession(Map<String, String> h) {
        String closedAt = h.get("closed_at");
        return new SessionRecord(
                fromHex(h.get("session_id")),
                fromHex(h.get("player_uuid")),
                h.get("server_name"),
                Long.parseLong(h.get("started_at")),
                Long.parseLong(h.get("last_activity")),
                closedAt == null || closedAt.isEmpty() ? null : Long.parseLong(closedAt),
                h.get("grim_version"),
                h.get("client_brand"),
                Integer.parseInt(h.getOrDefault("client_version_pvn", "-1")),
                h.get("server_version"),
                List.of());
    }

    private static ViolationRecord mapViolation(Map<String, String> h) {
        return new ViolationRecord(
                fromHex(h.get("id")),
                fromHex(h.get("session_id")),
                fromHex(h.get("player_uuid")),
                Integer.parseInt(h.get("check_id")),
                Double.parseDouble(h.get("vl")),
                Long.parseLong(h.get("occurred_at")),
                h.get("verbose"),
                VerboseFormat.fromCode(Integer.parseInt(h.getOrDefault("verbose_format", "0"))));
    }

    private static PlayerIdentity mapIdentity(Map<String, String> h) {
        return new PlayerIdentity(
                fromHex(h.get("uuid")),
                h.get("current_name"),
                Long.parseLong(h.get("first_seen")),
                Long.parseLong(h.get("last_seen")));
    }

    private static SettingRecord mapSetting(Map<String, String> h) {
        return new SettingRecord(
                SettingScope.valueOf(h.get("scope")),
                h.get("scope_key"),
                h.get("key"),
                Base64.getDecoder().decode(h.get("value_b64")),
                Long.parseLong(h.get("updated_at")));
    }

    @Override
    public <E> void delete(@NotNull Category<E> cat, @NotNull DeleteCriteria criteria) throws BackendException {
        try (Jedis j = pool.getResource()) {
            if (criteria instanceof Deletes.ByPlayer d) {
                String phex = hex(d.uuid());
                if (cat == Categories.VIOLATION) deletePlayerViolations(j, phex);
                else if (cat == Categories.SESSION) {
                    deletePlayerViolations(j, phex);
                    deletePlayerSessions(j, phex);
                } else if (cat == Categories.PLAYER_IDENTITY) {
                    deletePlayerIdentity(j, d.uuid(), phex);
                } else if (cat == Categories.SETTING) {
                    scanDel(j, tableKey(config.tableNames().settings()) + ":PLAYER:" + d.uuid() + ":*");
                } else {
                    throw new BackendException("unsupported category for delete: " + cat.id());
                }
            } else if (criteria instanceof Deletes.OlderThan d) {
                long cutoff = System.currentTimeMillis() - d.maxAgeMs();
                if (cat == Categories.SESSION) deleteSessionsOlderThan(j, cutoff);
                else if (cat == Categories.VIOLATION) deleteViolationsOlderThan(j, cutoff);
                else throw new BackendException("unsupported category for retention: " + cat.id());
            } else {
                throw new BackendException("unknown DeleteCriteria: " + criteria.getClass().getSimpleName());
            }
        } catch (RuntimeException e) {
            throw new BackendException("delete failed", e);
        }
    }

    private void deletePlayerViolations(Jedis j, String phex) {
        String zsetKey = tableKey(config.tableNames().violations()) + ":by-player:" + phex;
        for (String id : j.zrange(zsetKey, 0, -1)) {
            String recordKey = tableKey(config.tableNames().violations()) + ":" + id;
            Map<String, String> h = j.hgetAll(recordKey);
            if (!h.isEmpty()) {
                j.zrem(tableKey(config.tableNames().violations()) + ":by-session:" + h.get("session_id"), id);
            }
            j.del(recordKey);
        }
        j.del(zsetKey);
    }

    private void deletePlayerSessions(Jedis j, String phex) {
        String zsetKey = tableKey(config.tableNames().sessions()) + ":by-player:" + phex;
        for (String sessionHex : j.zrange(zsetKey, 0, -1)) {
            j.del(tableKey(config.tableNames().sessions()) + ":" + sessionHex);
        }
        j.del(zsetKey);
    }

    private void deletePlayerIdentity(Jedis j, UUID uuid, String phex) {
        Map<String, String> h = j.hgetAll(tableKey(config.tableNames().players()) + ":" + phex);
        if (!h.isEmpty() && h.get("current_name") != null) {
            j.del(tableKey(config.tableNames().players()) + ":by-name-lower:"
                    + h.get("current_name").toLowerCase(Locale.ROOT));
        }
        j.del(tableKey(config.tableNames().players()) + ":" + phex);
    }

    private void deleteSessionsOlderThan(Jedis j, long cutoff) {
        // Iterate every by-player zset, find sessions with started_at < cutoff,
        // and scrub them plus their violations. Could be optimised with a global
        // by-time zset, but operators using the Redis backend are unlikely to
        // have enough volume for that to matter and the extra index doubles
        // every session write.
        String pattern = tableKey(config.tableNames().sessions()) + ":by-player:*";
        ScanParams sp = new ScanParams().match(pattern).count(100);
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            var res = j.scan(cursor, sp);
            cursor = res.getCursor();
            for (String zsetKey : res.getResult()) {
                List<Tuple> doomed = j.zrangeByScoreWithScores(zsetKey, "-inf", "(" + cutoff);
                for (Tuple t : doomed) {
                    String sessionHex = t.getElement();
                    String recordKey = tableKey(config.tableNames().sessions()) + ":" + sessionHex;
                    j.del(recordKey);
                    j.zrem(zsetKey, sessionHex);
                    // Drop associated violations.
                    String vZset = tableKey(config.tableNames().violations()) + ":by-session:" + sessionHex;
                    for (String vid : j.zrange(vZset, 0, -1)) {
                        String vkey = tableKey(config.tableNames().violations()) + ":" + vid;
                        Map<String, String> h = j.hgetAll(vkey);
                        if (!h.isEmpty()) {
                            j.zrem(tableKey(config.tableNames().violations()) + ":by-player:" + h.get("player_uuid"), vid);
                        }
                        j.del(vkey);
                    }
                    j.del(vZset);
                }
            }
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
    }

    private void deleteViolationsOlderThan(Jedis j, long cutoff) {
        String pattern = tableKey(config.tableNames().violations()) + ":by-session:*";
        ScanParams sp = new ScanParams().match(pattern).count(100);
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            var res = j.scan(cursor, sp);
            cursor = res.getCursor();
            for (String zsetKey : res.getResult()) {
                List<Tuple> doomed = j.zrangeByScoreWithScores(zsetKey, "-inf", "(" + cutoff);
                for (Tuple t : doomed) {
                    String id = t.getElement();
                    String vkey = tableKey(config.tableNames().violations()) + ":" + id;
                    Map<String, String> h = j.hgetAll(vkey);
                    if (!h.isEmpty()) {
                        j.zrem(tableKey(config.tableNames().violations()) + ":by-player:" + h.get("player_uuid"), id);
                    }
                    j.del(vkey);
                    j.zrem(zsetKey, id);
                }
            }
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
    }

    private void scanDel(Jedis j, String pattern) {
        ScanParams sp = new ScanParams().match(pattern).count(100);
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            var res = j.scan(cursor, sp);
            cursor = res.getCursor();
            for (String k : res.getResult()) j.del(k);
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
    }

    @Override
    public long countViolationsInSession(@NotNull UUID sessionId) throws BackendException {
        try (Jedis j = pool.getResource()) {
            return j.zcard(tableKey(config.tableNames().violations()) + ":by-session:" + hex(sessionId));
        } catch (RuntimeException e) {
            throw new BackendException("countViolationsInSession failed", e);
        }
    }

    @Override
    public long countUniqueChecksInSession(@NotNull UUID sessionId) throws BackendException {
        try (Jedis j = pool.getResource()) {
            String zsetKey = tableKey(config.tableNames().violations()) + ":by-session:" + hex(sessionId);
            java.util.Set<String> seenChecks = new java.util.HashSet<>();
            for (String id : j.zrange(zsetKey, 0, -1)) {
                Map<String, String> h = j.hgetAll(tableKey(config.tableNames().violations()) + ":" + id);
                if (!h.isEmpty()) seenChecks.add(h.get("check_id"));
            }
            return seenChecks.size();
        } catch (RuntimeException e) {
            throw new BackendException("countUniqueChecksInSession failed", e);
        }
    }

    @Override
    public long countSessionsByPlayer(@NotNull UUID player) throws BackendException {
        try (Jedis j = pool.getResource()) {
            return j.zcard(tableKey(config.tableNames().sessions()) + ":by-player:" + hex(player));
        } catch (RuntimeException e) {
            throw new BackendException("countSessionsByPlayer failed", e);
        }
    }

    @Override
    public long markCrashedSessions() throws BackendException {
        // Redis has no native UPDATE; SCAN every session hash and copy
        // last_activity into closed_at when closed_at is missing. Runs
        // once at startup, sweep cost is O(open sessions).
        long affected = 0;
        try (Jedis j = pool.getResource()) {
            String prefix = tableKey(config.tableNames().sessions()) + ":";
            String matchPattern = prefix + "*";
            ScanParams sp = new ScanParams().match(matchPattern).count(500);
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                redis.clients.jedis.resps.ScanResult<String> res = j.scan(cursor, sp);
                for (String key : res.getResult()) {
                    // Skip the by-player ZSETs and meta keys — only direct
                    // session hashes have a numeric trailing segment.
                    if (key.contains(":by-player:") || key.contains(":seq:")) continue;
                    Map<String, String> h = j.hgetAll(key);
                    if (h.isEmpty()) continue;
                    if (h.get("closed_at") != null) continue;
                    String lastActivity = h.get("last_activity");
                    if (lastActivity == null) continue;
                    j.hset(key, "closed_at", lastActivity);
                    affected++;
                }
                cursor = res.getCursor();
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
            return affected;
        } catch (RuntimeException e) {
            throw new BackendException("markCrashedSessions failed", e);
        }
    }

    // --- key / value encoding ------------------------------------------------

    private String metaKey() {
        return tableKey(config.tableNames().meta());
    }

    private String tableKey(String logicalName) {
        return config.keyPrefix() + logicalName;
    }

    private static String hex(UUID u) {
        return u.toString().replace("-", "");
    }

    private static UUID fromHex(String hex) {
        if (hex == null || hex.length() != 32) {
            throw new IllegalArgumentException("uuid hex must be 32 chars, got " + (hex == null ? "null" : hex.length()));
        }
        byte[] bytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return UuidCodec.fromBytes(bytes);
    }

    // --- cursor helpers ------------------------------------------------------

    private static Cursor encodeStartedCursor(long started, UUID sessionId) {
        return new Cursor(started + ":" + hex(sessionId));
    }

    private static long decodeStartedCursor(Cursor c, long defaultVal) {
        if (c == null) return defaultVal;
        String t = c.token();
        int colon = t.indexOf(':');
        if (colon <= 0) return defaultVal;
        try { return Long.parseLong(t.substring(0, colon)); }
        catch (NumberFormatException e) { return defaultVal; }
    }

    private static Cursor encodeViolationCursor(long occurredAt, UUID id) {
        return new Cursor("v:" + occurredAt + ":" + id);
    }

    private static long decodeViolationOccurredCursor(Cursor c, long defaultVal) {
        if (c == null) return defaultVal;
        String[] parts = c.token().split(":", 3);
        if (parts.length < 3 || !"v".equals(parts[0])) return defaultVal;
        try { return Long.parseLong(parts[1]); }
        catch (NumberFormatException e) { return defaultVal; }
    }

    private static UUID decodeViolationIdCursor(Cursor c) {
        if (c == null) return null;
        String[] parts = c.token().split(":", 3);
        if (parts.length < 3 || !"v".equals(parts[0])) return null;
        try { return UUID.fromString(parts[2]); }
        catch (IllegalArgumentException e) { return null; }
    }

    @ApiStatus.Internal
    public TableNames tableNames() {
        return config.tableNames();
    }
}
