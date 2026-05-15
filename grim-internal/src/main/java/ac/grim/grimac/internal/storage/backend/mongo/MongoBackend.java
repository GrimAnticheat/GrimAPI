package ac.grim.grimac.internal.storage.backend.mongo;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.check.CheckCatalogPersistence;
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
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonBinary;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * MongoDB 6.x+ backend. Each logical "table" from {@link TableNames} maps to a
 * collection in the configured database; records are BSON documents whose
 * fields match the SQL column names one-for-one so cross-backend copy works
 * without transform code.
 * <p>
 * Binary fields (UUIDs, setting values) are stored as
 * {@link org.bson.BsonBinary} with subtype 4 (UUID) for id-like fields and
 * subtype 0 (generic) for opaque byte-arrays. Violations key off a UUIDv7
 * {@code id} (also UUID-binary) minted producer-side so multiple JVMs can
 * share one MongoDB cluster without coordinating an allocator. The
 * timestamp prefix on UUIDv7 keeps the unique-index B-tree append-friendly;
 * violation paging orders by {@code (occurred_at, id)} so event time remains
 * authoritative and the UUID is a stable tie-breaker.
 */
@ApiStatus.Internal
public final class MongoBackend implements Backend {

    public static final String ID = "mongo";

    /**
     * Bumped on every breaking shape change; {@link #migrateSchema(int)}
     * runs forward migrations for any older value found in the meta doc.
     */
    static final int CURRENT_SCHEMA_VERSION = 6;

    private final MongoBackendConfig config;
    private final List<BatchingHandler<?>> handlers = new ArrayList<>();
    private MongoClient client;
    private MongoDatabase db;
    private MongoCollection<Document> meta, checks, players, sessions, violations, settings;
    private Logger logger;

    public MongoBackend(MongoBackendConfig config) {
        this.config = config;
    }

    @Override public @NotNull String id() { return ID; }
    @Override public @NotNull ApiVersion getApiVersion() { return ApiVersion.CURRENT; }

    @Override
    public @NotNull EnumSet<Capability> capabilities() {
        return EnumSet.of(
                Capability.INDEXED_KV,
                Capability.TIMESERIES_APPEND,
                Capability.HISTORY,
                Capability.SETTINGS,
                Capability.PLAYER_IDENTITY);
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
            Class.forName("com.mongodb.client.MongoClients");
        } catch (ClassNotFoundException cnf) {
            throw new BackendException("mongodb-driver-sync not on the classpath — shade it into the plugin jar or drop it into server/plugins", cnf);
        }
        try {
            this.client = MongoClients.create(config.connectionString());
            this.db = client.getDatabase(config.database());
            TableNames t = config.tableNames();
            this.meta = db.getCollection(t.meta());
            this.checks = db.getCollection(t.checks());
            this.players = db.getCollection(t.players());
            this.sessions = db.getCollection(t.sessions());
            this.violations = db.getCollection(t.violations());
            this.settings = db.getCollection(t.settings());
            int existingVersion = readSchemaVersion();
            ensureMetaDoc();
            if (existingVersion >= 0 && existingVersion < CURRENT_SCHEMA_VERSION) {
                migrateSchema(existingVersion);
            }
            ensureIndexes();
            MongoCheckCatalogPersistence.alignCounterWithExistingRows(meta, checks);
        } catch (RuntimeException e) {
            throw new BackendException("failed to initialise Mongo backend", e);
        }
    }

    @Override
    public @NotNull CheckCatalogPersistence checkCatalog() {
        return new MongoCheckCatalogPersistence(meta, checks);
    }

    private int readSchemaVersion() {
        Document metaDoc = meta.find(Filters.eq("_id", 0)).first();
        if (metaDoc == null) return -1;
        return metaDoc.getInteger("schema_version", 0);
    }

    private void ensureIndexes() {
        checks.createIndex(Indexes.ascending("stable_key"), new IndexOptions().unique(true));
        checks.createIndex(Indexes.ascending("check_id"), new IndexOptions().unique(true));
        players.createIndex(Indexes.ascending("current_name_lower"));
        sessions.createIndex(Indexes.descending("started_at"));
        sessions.createIndex(Indexes.compoundIndex(Indexes.ascending("player_uuid"), Indexes.descending("started_at")));
        violations.createIndex(Indexes.compoundIndex(
                Indexes.ascending("session_id"), Indexes.ascending("occurred_at"), Indexes.ascending("id")));
        violations.createIndex(Indexes.ascending("player_uuid"));
        violations.createIndex(Indexes.ascending("id"), new IndexOptions().unique(true));
        settings.createIndex(Indexes.compoundIndex(
                Indexes.ascending("scope"), Indexes.ascending("scope_key"), Indexes.ascending("key")),
                new IndexOptions().unique(true));
    }

    private void ensureMetaDoc() {
        Document metaDoc = meta.find(Filters.eq("_id", 0)).first();
        if (metaDoc == null) {
            long now = System.currentTimeMillis();
            meta.insertOne(new Document("_id", 0)
                    .append("schema_version", CURRENT_SCHEMA_VERSION)
                    .append("grim_core_version", "phase1")
                    .append("initialized_at", now)
                    .append("last_migration_at", now));
        }
    }

    /**
     * Forward-only schema migrations. Each step is idempotent (gated on
     * {@code existing < N}) and updates {@code schema_version} on success.
     */
    private void migrateSchema(int existing) {
        if (existing < 6) migrateV5ToV6();
        meta.updateOne(Filters.eq("_id", 0),
                Updates.combine(
                        Updates.set("schema_version", CURRENT_SCHEMA_VERSION),
                        Updates.set("last_migration_at", System.currentTimeMillis()),
                        Updates.unset("violation_id_seq")));
    }

    /**
     * v5 → v6: violation {@code id} type changes from {@code long} to UUID
     * (UUIDv7, stored as {@link BsonBinary} subtype 4). Each legacy row's
     * replacement id is synthesised from {@code occurred_at} and the old
     * numeric id so same-millisecond ordering is preserved. The unique index
     * on {@code id} is rebuilt afterwards because index entries reference the
     * now-replaced byte values.
     */
    private void migrateV5ToV6() {
        logger.info("[grim-datastore] migrating Mongo violations id long→UUIDv7 …");
        try {
            violations.dropIndex(Indexes.ascending("id"));
        } catch (RuntimeException ignore) {
            // Index may not exist (fresh-ish DB); ensureIndexes() recreates it.
        }
        long count = 0;
        List<WriteModel<Document>> ops = new ArrayList<>();
        for (Document d : violations.find().projection(new Document("_id", 1).append("id", 1).append("occurred_at", 1))) {
            Object idObj = d.get("id");
            // Already migrated rows have BsonBinary / Binary ids — skip.
            if (idObj instanceof org.bson.types.Binary || idObj instanceof BsonBinary) continue;
            long occurred = d.getLong("occurred_at") == null ? 0L : d.getLong("occurred_at");
            long legacyId = idObj instanceof Number n ? n.longValue() : 0L;
            UUID newId = UuidV7.fromTimestampMs(occurred, legacyId);
            ops.add(new UpdateOneModel<>(Filters.eq("_id", d.get("_id")),
                    Updates.set("id", binUuid(newId))));
            if (ops.size() >= 1024) {
                violations.bulkWrite(ops);
                count += ops.size();
                ops.clear();
            }
        }
        if (!ops.isEmpty()) {
            violations.bulkWrite(ops);
            count += ops.size();
        }
        logger.info("[grim-datastore] migrated " + count + " violation rows to UUIDv7 id");
    }

    @Override public void flush() {}

    @Override
    public void close() throws BackendException {
        synchronized (handlers) {
            for (BatchingHandler<?> h : handlers) h.shutDown();
            handlers.clear();
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <E> StorageEventHandler<E> eventHandlerFor(@NotNull Category<E> cat) throws BackendException {
        BatchingHandler<?> h;
        if (cat == Categories.VIOLATION) h = new ViolationHandler();
        else if (cat == Categories.SESSION) h = new SessionHandler();
        else if (cat == Categories.PLAYER_IDENTITY) h = new IdentityHandler();
        else if (cat == Categories.SETTING) h = new SettingHandler();
        else throw new IllegalArgumentException("unsupported category: " + cat.id());
        synchronized (handlers) { handlers.add(h); }
        return (StorageEventHandler<E>) h;
    }

    /**
     * Batches writes into {@link MongoCollection#bulkWrite} calls on endOfBatch
     * or at the configured cap. No dedicated connection per handler — the Mongo
     * driver pools internally — but we still batch to amortise round trips.
     */
    private abstract class BatchingHandler<E> implements StorageEventHandler<E> {
        final List<WriteModel<Document>> pending = new ArrayList<>();

        @Override
        public synchronized void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            try {
                append(event);
                if (endOfBatch || pending.size() >= config.batchFlushCap()) flushLocked();
            } catch (RuntimeException e) {
                pending.clear();
                throw new BackendException(categoryId() + " write failed", e);
            }
        }

        private void flushLocked() {
            if (pending.isEmpty()) return;
            collection().bulkWrite(pending);
            pending.clear();
        }

        synchronized void shutDown() {
            try { flushLocked(); } catch (RuntimeException ignore) {}
        }

        protected abstract MongoCollection<Document> collection();
        protected abstract String categoryId();
        protected abstract void append(E event);
    }

    private final class ViolationHandler extends BatchingHandler<ViolationEvent> {
        @Override protected MongoCollection<Document> collection() { return violations; }
        @Override protected String categoryId() { return "violation"; }
        @Override
        protected void append(ViolationEvent v) {
            UUID id = v.id();
            pending.add(new InsertOneModel<>(violationDoc(id, v.sessionId(), v.playerUuid(),
                    v.checkId(), v.vl(), v.occurredEpochMs(), v.verbose(), v.verboseFormat())));
        }
    }

    private final class SessionHandler extends BatchingHandler<SessionEvent> {
        @Override protected MongoCollection<Document> collection() { return sessions; }
        @Override protected String categoryId() { return "session"; }
        @Override
        protected void append(SessionEvent s) {
            // Aggregation-pipeline update so closed_at preserves any
            // already-set value across heartbeats. $ifNull keeps the
            // existing closed_at if non-null, falls back to the event's
            // value (null on heartbeats, the close timestamp on close()).
            // Same NULL → set transition semantics as the SQL backends.
            Document setStage = new Document("$set", sessionUpdateFields(s));
            pending.add(new UpdateOneModel<>(
                    Filters.eq("_id", binUuid(s.sessionId())),
                    List.of(setStage),
                    new UpdateOptions().upsert(true)));
        }
    }

    private Document sessionUpdateFields(SessionEvent s) {
        if (!s.sessionBlobs().isEmpty()) {
            throw new UnsupportedOperationException(
                    "session-blob serialisation isn't implemented by this backend; "
                            + "sessions with non-empty sessionBlobs cannot be stored");
        }
        Document fields = new Document()
                .append("player_uuid", binUuid(s.playerUuid()))
                .append("server_name", s.serverName())
                .append("started_at", s.startedEpochMs())
                .append("last_activity", s.lastActivityEpochMs())
                .append("grim_version", s.grimVersion())
                .append("client_brand", s.clientBrand())
                .append("client_version_pvn", s.clientVersion())
                .append("server_version", s.serverVersionString())
                .append("replay_clips", List.of());
        // closed_at: existing wins when non-null; event value when existing is null.
        fields.append("closed_at", new Document("$ifNull",
                Arrays.asList("$closed_at", s.closedAtEpochMs())));
        return fields;
    }

    private final class IdentityHandler extends BatchingHandler<PlayerIdentityEvent> {
        @Override protected MongoCollection<Document> collection() { return players; }
        @Override protected String categoryId() { return "player-identity"; }
        @Override
        protected void append(PlayerIdentityEvent e) {
            // Upsert with LEAST/GREATEST semantics on first_seen/last_seen —
            // replaceOne wipes the whole doc, so do a find-then-merge.
            // Acceptable on the write path: driver pools connections, and the
            // identity stream is low-volume compared to violations.
            Document existing = players.find(Filters.eq("_id", binUuid(e.uuid()))).first();
            long first = existing == null ? e.firstSeenEpochMs()
                    : Math.min(e.firstSeenEpochMs(), existing.getLong("first_seen"));
            long last = existing == null ? e.lastSeenEpochMs()
                    : Math.max(e.lastSeenEpochMs(), existing.getLong("last_seen"));
            Document doc = identityDoc(e.uuid(), e.currentName(), first, last);
            pending.add(new ReplaceOneModel<>(Filters.eq("_id", binUuid(e.uuid())),
                    doc, new ReplaceOptions().upsert(true)));
        }
    }

    private final class SettingHandler extends BatchingHandler<SettingEvent> {
        @Override protected MongoCollection<Document> collection() { return settings; }
        @Override protected String categoryId() { return "setting"; }
        @Override
        protected void append(SettingEvent s) {
            Document doc = settingDoc(s.scope().name(), s.scopeKey(), s.key(), s.value(), s.updatedEpochMs());
            Bson filter = Filters.and(
                    Filters.eq("scope", s.scope().name()),
                    Filters.eq("scope_key", s.scopeKey()),
                    Filters.eq("key", s.key()));
            pending.add(new ReplaceOneModel<>(filter, doc, new ReplaceOptions().upsert(true)));
        }
    }

    // --- doc builders --------------------------------------------------------

    private static BsonBinary binUuid(UUID u) {
        return new BsonBinary(UuidCodec.toBytes(u));
    }

    private static Document violationDoc(UUID id, UUID session, UUID player, int checkId,
                                         double vl, long occurred, String verbose,
                                         VerboseFormat fmt) {
        return new Document()
                .append("id", binUuid(id))
                .append("session_id", binUuid(session))
                .append("player_uuid", binUuid(player))
                .append("check_id", checkId)
                .append("vl", vl)
                .append("occurred_at", occurred)
                .append("verbose", verbose)
                .append("verbose_format", fmt.code());
    }

    private static Document sessionDoc(UUID session, UUID player, String serverName,
                                       long startedAt, long lastActivity, Long closedAt,
                                       String grimVersion, String clientBrand, int clientPvn,
                                       String serverVersion, boolean emptyReplay) {
        if (!emptyReplay) {
            throw new UnsupportedOperationException(
                    "session-blob serialisation isn't implemented by this backend; "
                            + "sessions with non-empty sessionBlobs cannot be stored");
        }
        Document d = new Document()
                .append("_id", binUuid(session))
                .append("player_uuid", binUuid(player))
                .append("server_name", serverName)
                .append("started_at", startedAt)
                .append("last_activity", lastActivity)
                .append("grim_version", grimVersion)
                .append("client_brand", clientBrand)
                .append("client_version_pvn", clientPvn)
                .append("server_version", serverVersion)
                .append("replay_clips", List.of());
        if (closedAt != null) d.append("closed_at", closedAt);
        return d;
    }

    private static Document identityDoc(UUID uuid, String currentName, long firstSeen, long lastSeen) {
        return new Document()
                .append("_id", binUuid(uuid))
                .append("current_name", currentName)
                .append("current_name_lower", currentName == null ? null : currentName.toLowerCase(Locale.ROOT))
                .append("first_seen", firstSeen)
                .append("last_seen", lastSeen);
    }

    private static Document settingDoc(String scope, String scopeKey, String key, byte[] value, long updatedAt) {
        return new Document()
                .append("scope", scope)
                .append("scope_key", scopeKey)
                .append("key", key)
                .append("value", new BsonBinary(value))
                .append("updated_at", updatedAt);
    }

    // --- bulk-load path ------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public <R> void bulkImport(@NotNull Category<?> cat, @NotNull List<R> records) throws BackendException {
        if (records.isEmpty()) return;
        try {
            if (cat == Categories.VIOLATION) bulkViolations((List<ViolationRecord>) records);
            else if (cat == Categories.SESSION) bulkSessions((List<SessionRecord>) records);
            else if (cat == Categories.PLAYER_IDENTITY) bulkIdentities((List<PlayerIdentity>) records);
            else if (cat == Categories.SETTING) bulkSettings((List<SettingRecord>) records);
            else throw new BackendException("unsupported category: " + cat.id());
        } catch (RuntimeException e) {
            throw new BackendException("bulkImport failed for " + cat.id(), e);
        }
    }

    private void bulkViolations(List<ViolationRecord> rows) {
        // ViolationRecord.id is non-null by record contract — bulkImport from
        // any backend preserves the upstream id verbatim, no mint needed.
        List<Document> docs = new ArrayList<>(rows.size());
        for (ViolationRecord v : rows) {
            docs.add(violationDoc(v.id(), v.sessionId(), v.playerUuid(), v.checkId(), v.vl(),
                    v.occurredEpochMs(), v.verbose(), v.verboseFormat()));
        }
        if (!docs.isEmpty()) {
            violations.insertMany(docs, new InsertManyOptions().ordered(false));
        }
    }

    private void bulkSessions(List<SessionRecord> rows) {
        // Used by bulkImport (cross-backend copy / V0 migration) — operates
        // on SessionRecord (immutable) rather than SessionEvent. closed_at
        // is taken straight from the record; this path doesn't need the
        // $ifNull preserve-on-heartbeat trick because each record is a
        // one-shot write.
        List<WriteModel<Document>> ops = new ArrayList<>(rows.size());
        for (SessionRecord s : rows) {
            Document doc = sessionDoc(s.sessionId(), s.playerUuid(), s.serverName(),
                    s.startedEpochMs(), s.lastActivityEpochMs(), s.closedAtEpochMs(),
                    s.grimVersion(), s.clientBrand(),
                    s.clientVersion(), s.serverVersionString(), s.sessionBlobs().isEmpty());
            ops.add(new ReplaceOneModel<>(Filters.eq("_id", binUuid(s.sessionId())),
                    doc, new ReplaceOptions().upsert(true)));
        }
        sessions.bulkWrite(ops);
    }

    private void bulkIdentities(List<PlayerIdentity> rows) {
        List<WriteModel<Document>> ops = new ArrayList<>(rows.size());
        for (PlayerIdentity id : rows) {
            ops.add(new ReplaceOneModel<>(Filters.eq("_id", binUuid(id.uuid())),
                    identityDoc(id.uuid(), id.currentName(), id.firstSeenEpochMs(), id.lastSeenEpochMs()),
                    new ReplaceOptions().upsert(true)));
        }
        players.bulkWrite(ops);
    }

    private void bulkSettings(List<SettingRecord> rows) {
        List<WriteModel<Document>> ops = new ArrayList<>(rows.size());
        for (SettingRecord s : rows) {
            Bson filter = Filters.and(
                    Filters.eq("scope", s.scope().name()),
                    Filters.eq("scope_key", s.scopeKey()),
                    Filters.eq("key", s.key()));
            ops.add(new ReplaceOneModel<>(filter,
                    settingDoc(s.scope().name(), s.scopeKey(), s.key(), s.value(), s.updatedEpochMs()),
                    new ReplaceOptions().upsert(true)));
        }
        settings.bulkWrite(ops);
    }

    // --- read path -----------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <R> Page<R> read(@NotNull Category<?> cat, @NotNull Query<R> query) throws BackendException {
        try {
            if (query instanceof Queries.ListSessionsByPlayer q) return (Page<R>) listSessionsByPlayer(q);
            if (query instanceof Queries.GetSessionById q) return (Page<R>) getSessionById(q);
            if (query instanceof Queries.ListViolationsInSession q) return (Page<R>) listViolationsInSession(q);
            if (query instanceof Queries.GetPlayerIdentity q) return (Page<R>) getPlayerIdentity(q);
            if (query instanceof Queries.GetPlayerIdentityByName q) return (Page<R>) getPlayerIdentityByName(q);
            if (query instanceof Queries.ListPlayersByNamePrefix q) return (Page<R>) listPlayersByNamePrefix(q);
            if (query instanceof Queries.GetSetting q) return (Page<R>) getSetting(q);
            throw new BackendException("unsupported query: " + query.getClass().getSimpleName());
        } catch (RuntimeException e) {
            throw new BackendException("read failed", e);
        }
    }

    private Page<SessionRecord> listSessionsByPlayer(Queries.ListSessionsByPlayer q) {
        long cursorStarted = decodeStartedCursor(q.cursor(), Long.MAX_VALUE);
        byte[] cursorSessionBytes = decodeSessionIdCursor(q.cursor());
        Bson playerEq = Filters.eq("player_uuid", binUuid(q.player()));
        Bson pageFilter = Filters.or(
                Filters.lt("started_at", cursorStarted),
                Filters.and(Filters.eq("started_at", cursorStarted),
                        Filters.lt("_id", new BsonBinary(cursorSessionBytes))));
        List<Document> docs = new ArrayList<>();
        for (Document d : sessions
                .find(Filters.and(playerEq, pageFilter))
                .sort(new Document("started_at", -1).append("_id", -1))
                .limit(q.pageSize() + 1)) {
            docs.add(d);
        }
        List<SessionRecord> out = new ArrayList<>();
        boolean hasMore = false;
        for (Document d : docs) {
            if (out.size() >= q.pageSize()) { hasMore = true; break; }
            out.add(mapSession(d));
        }
        Cursor next = null;
        if (hasMore && !out.isEmpty()) {
            SessionRecord last = out.get(out.size() - 1);
            next = encodeStartedCursor(last.startedEpochMs(), last.sessionId());
        }
        return new Page<>(out, next);
    }

    private Page<SessionRecord> getSessionById(Queries.GetSessionById q) {
        Document d = sessions.find(Filters.eq("_id", binUuid(q.sessionId()))).first();
        return d == null ? Page.empty() : new Page<>(List.of(mapSession(d)), null);
    }

    private Page<ViolationRecord> listViolationsInSession(Queries.ListViolationsInSession q) {
        // Order by (occurred_at, id) — id is wall-clock at mint time and can
        // drift from event time when callers pre-set event.id(), when ring
        // slots sit before the sink runs, or when bulkImport carries through
        // original ids. Id stays as the tiebreaker for same-ms bursts.
        long lastOccurred = decodeViolationOccurredCursor(q.cursor(), Long.MIN_VALUE);
        UUID lastId = decodeViolationIdCursor(q.cursor());
        Bson sessionEq = Filters.eq("session_id", binUuid(q.sessionId()));
        Bson finalFilter = lastId == null
                ? sessionEq
                : Filters.and(sessionEq, Filters.or(
                        Filters.gt("occurred_at", lastOccurred),
                        Filters.and(Filters.eq("occurred_at", lastOccurred),
                                Filters.gt("id", binUuid(lastId)))));
        List<ViolationRecord> out = new ArrayList<>();
        boolean hasMore = false;
        List<Document> docs = new ArrayList<>();
        for (Document d : violations
                .find(finalFilter)
                .sort(new Document("occurred_at", 1).append("id", 1))
                .limit(q.pageSize() + 1)) {
            docs.add(d);
        }
        for (Document d : docs) {
            if (out.size() >= q.pageSize()) { hasMore = true; break; }
            out.add(mapViolation(d));
        }
        Cursor next = null;
        if (hasMore && !out.isEmpty()) {
            ViolationRecord last = out.get(out.size() - 1);
            next = encodeViolationCursor(last.occurredEpochMs(), last.id());
        }
        return new Page<>(out, next);
    }

    private Page<PlayerIdentity> getPlayerIdentity(Queries.GetPlayerIdentity q) {
        Document d = players.find(Filters.eq("_id", binUuid(q.uuid()))).first();
        return d == null ? Page.empty() : new Page<>(List.of(mapIdentity(d)), null);
    }

    private Page<PlayerIdentity> getPlayerIdentityByName(Queries.GetPlayerIdentityByName q) {
        Document d = players.find(Filters.eq("current_name_lower", q.name().toLowerCase(Locale.ROOT)))
                .sort(new Document("last_seen", -1))
                .limit(1)
                .first();
        return d == null ? Page.empty() : new Page<>(List.of(mapIdentity(d)), null);
    }

    private Page<PlayerIdentity> listPlayersByNamePrefix(Queries.ListPlayersByNamePrefix q) {
        String prefix = q.lowerPrefix();
        if (prefix == null || prefix.isEmpty() || q.limit() <= 0) return Page.empty();
        // ^ + Pattern.quote anchors the regex and escapes any user-supplied
        // metachars. The current_name_lower index is ascending so anchored-
        // prefix regex hits the index range scan path.
        String anchored = "^" + java.util.regex.Pattern.quote(prefix);
        java.util.List<PlayerIdentity> out = new java.util.ArrayList<>();
        for (Document d : players.find(Filters.regex("current_name_lower", anchored))
                .sort(new Document("last_seen", -1))
                .limit(q.limit())) {
            out.add(mapIdentity(d));
        }
        return new Page<>(out, null);
    }

    private Page<SettingRecord> getSetting(Queries.GetSetting q) {
        Bson filter = Filters.and(
                Filters.eq("scope", q.scope().name()),
                Filters.eq("scope_key", q.scopeKey()),
                Filters.eq("key", q.key()));
        Document d = settings.find(filter).first();
        return d == null ? Page.empty() : new Page<>(List.of(mapSetting(d)), null);
    }

    /**
     * The Mongo sync driver decodes BSON binary into {@link org.bson.types.Binary}
     * on read (not {@link BsonBinary}), but tests and bulk-import paths that
     * build docs by hand may have either type on the way in. Accept both.
     */
    private static byte[] binBytes(Object raw) {
        if (raw instanceof org.bson.types.Binary b) return b.getData();
        if (raw instanceof BsonBinary b) return b.getData();
        if (raw instanceof byte[] b) return b;
        throw new IllegalStateException("expected binary field, got " + (raw == null ? "null" : raw.getClass().getName()));
    }

    private static SessionRecord mapSession(Document d) {
        return new SessionRecord(
                UuidCodec.fromBytes(binBytes(d.get("_id"))),
                UuidCodec.fromBytes(binBytes(d.get("player_uuid"))),
                d.getString("server_name"),
                d.getLong("started_at"),
                d.getLong("last_activity"),
                d.getLong("closed_at"), // Document.getLong returns null when absent
                d.getString("grim_version"),
                d.getString("client_brand"),
                d.getInteger("client_version_pvn", -1),
                d.getString("server_version"),
                List.of());
    }

    private static ViolationRecord mapViolation(Document d) {
        return new ViolationRecord(
                UuidCodec.fromBytes(binBytes(d.get("id"))),
                UuidCodec.fromBytes(binBytes(d.get("session_id"))),
                UuidCodec.fromBytes(binBytes(d.get("player_uuid"))),
                d.getInteger("check_id"),
                d.getDouble("vl"),
                d.getLong("occurred_at"),
                d.getString("verbose"),
                VerboseFormat.fromCode(d.getInteger("verbose_format", 0)));
    }

    private static PlayerIdentity mapIdentity(Document d) {
        return new PlayerIdentity(
                UuidCodec.fromBytes(binBytes(d.get("_id"))),
                d.getString("current_name"),
                d.getLong("first_seen"),
                d.getLong("last_seen"));
    }

    private static SettingRecord mapSetting(Document d) {
        return new SettingRecord(
                SettingScope.valueOf(d.getString("scope")),
                d.getString("scope_key"),
                d.getString("key"),
                binBytes(d.get("value")),
                d.getLong("updated_at"));
    }

    @Override
    public <E> void delete(@NotNull Category<E> cat, @NotNull DeleteCriteria criteria) throws BackendException {
        try {
            if (criteria instanceof Deletes.ByPlayer d) {
                BsonBinary uuid = binUuid(d.uuid());
                if (cat == Categories.VIOLATION) violations.deleteMany(Filters.eq("player_uuid", uuid));
                else if (cat == Categories.SESSION) {
                    violations.deleteMany(Filters.eq("player_uuid", uuid));
                    sessions.deleteMany(Filters.eq("player_uuid", uuid));
                } else if (cat == Categories.PLAYER_IDENTITY) {
                    players.deleteOne(Filters.eq("_id", uuid));
                } else if (cat == Categories.SETTING) {
                    settings.deleteMany(Filters.and(
                            Filters.eq("scope", "PLAYER"),
                            Filters.eq("scope_key", d.uuid().toString())));
                } else {
                    throw new BackendException("unsupported category for delete: " + cat.id());
                }
            } else if (criteria instanceof Deletes.OlderThan d) {
                long cutoff = System.currentTimeMillis() - d.maxAgeMs();
                if (cat == Categories.SESSION) {
                    // Find doomed session ids, then delete their violations + the sessions themselves.
                    List<BsonBinary> doomed = new ArrayList<>();
                    for (Document s : sessions.find(Filters.lt("started_at", cutoff)).projection(new Document("_id", 1))) {
                        doomed.add((BsonBinary) s.get("_id"));
                    }
                    if (!doomed.isEmpty()) {
                        violations.deleteMany(Filters.in("session_id", doomed));
                        sessions.deleteMany(Filters.in("_id", doomed));
                    }
                } else if (cat == Categories.VIOLATION) {
                    violations.deleteMany(Filters.lt("occurred_at", cutoff));
                } else {
                    throw new BackendException("unsupported category for retention: " + cat.id());
                }
            } else {
                throw new BackendException("unknown DeleteCriteria: " + criteria.getClass().getSimpleName());
            }
        } catch (RuntimeException e) {
            throw new BackendException("delete failed", e);
        }
    }

    @Override
    public long countViolationsInSession(@NotNull UUID sessionId) throws BackendException {
        try { return violations.countDocuments(Filters.eq("session_id", binUuid(sessionId))); }
        catch (RuntimeException e) { throw new BackendException("countViolationsInSession failed", e); }
    }

    @Override
    public long countUniqueChecksInSession(@NotNull UUID sessionId) throws BackendException {
        try {
            List<Document> pipeline = Arrays.asList(
                    new Document("$match", new Document("session_id", binUuid(sessionId))),
                    new Document("$group", new Document("_id", "$check_id")),
                    new Document("$count", "n"));
            for (Document d : violations.aggregate(pipeline)) {
                return d.getInteger("n", 0);
            }
            return 0L;
        } catch (RuntimeException e) {
            throw new BackendException("countUniqueChecksInSession failed", e);
        }
    }

    @Override
    public long countSessionsByPlayer(@NotNull UUID player) throws BackendException {
        try { return sessions.countDocuments(Filters.eq("player_uuid", binUuid(player))); }
        catch (RuntimeException e) { throw new BackendException("countSessionsByPlayer failed", e); }
    }

    @Override
    public long markCrashedSessions() throws BackendException {
        try {
            // Aggregation pipeline: set closed_at = last_activity for any
            // session where closed_at is missing OR null.
            Document setStage = new Document("$set", new Document("closed_at", "$last_activity"));
            return sessions.updateMany(
                    new Document("$or", List.of(
                            Filters.exists("closed_at", false),
                            Filters.eq("closed_at", null))),
                    List.of(setStage)).getModifiedCount();
        } catch (RuntimeException e) {
            throw new BackendException("markCrashedSessions failed", e);
        }
    }

    private static Cursor encodeStartedCursor(long started, UUID sessionId) {
        return new Cursor(started + ":" + sessionId.toString().replace("-", ""));
    }

    private static long decodeStartedCursor(Cursor c, long defaultVal) {
        if (c == null) return defaultVal;
        String t = c.token();
        int colon = t.indexOf(':');
        if (colon <= 0) return defaultVal;
        try { return Long.parseLong(t.substring(0, colon)); }
        catch (NumberFormatException e) { return defaultVal; }
    }

    private static byte[] decodeSessionIdCursor(Cursor c) {
        if (c == null) return new byte[16];
        String t = c.token();
        int colon = t.indexOf(':');
        if (colon <= 0 || colon == t.length() - 1) return new byte[16];
        String hex = t.substring(colon + 1);
        if (hex.length() != 32) return new byte[16];
        byte[] out = new byte[16];
        for (int i = 0; i < 16; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
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
