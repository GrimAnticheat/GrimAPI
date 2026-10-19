package ac.grim.grimac.internal.storage.backend.redis.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.IndexSpec;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodec;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodecs;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.logging.Logger;

/**
 * Redis adapter for {@link Entity} stores. Each entity is a Redis HASH:
 * {@code <prefix><storeId>:<encodedId>} with one hash field per
 * EncodeShape field. Secondary indexes use sorted sets.
 */
@ApiStatus.Internal
public final class RedisEntityAdapter implements KindAdapter<Entity<?, ?, ?>> {

    private final @NotNull JedisPool pool;
    private final @NotNull String keyPrefix;
    private final @NotNull Logger logger;

    public RedisEntityAdapter(@NotNull JedisPool pool, @NotNull String keyPrefix,
                              @NotNull Logger logger) {
        this.pool = pool;
        this.keyPrefix = keyPrefix;
        this.logger = logger;
    }

    @SuppressWarnings("unchecked")
    @Override public @NotNull Class<Entity<?, ?, ?>> kindType() {
        return (Class<Entity<?, ?, ?>>) (Class<?>) Entity.class;
    }

    @Override public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_ENTITY, Capability.ATOMIC_UPSERT);
    }

    @Override
    public void ensureStore(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind) throws BackendException {
        if (kind.secondaryIndexes().isEmpty()) return;
        String storePrefix = keyPrefix + id.name() + ":";
        try (var j = pool.getResource()) {
            deleteIndexKeys(j, storePrefix);

            BsonCodec codec = BsonCodecs.regular(kind.recordType());
            EncodeShape shape = kind.codec().shape();
            String cursor = redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;
            redis.clients.jedis.params.ScanParams params = new redis.clients.jedis.params.ScanParams()
                .match(storePrefix + "*")
                .count(1000);
            do {
                redis.clients.jedis.resps.ScanResult<String> result = j.scan(cursor, params);
                for (String key : result.getResult()) {
                    if (key.startsWith(storePrefix + "__idx:")) continue;
                    Map<byte[], byte[]> hash = j.hgetAll(bytes(key));
                    if (hash == null || hash.isEmpty()) continue;
                    Object record = decodeHash(kind, hash);
                    Object idVal = codec.readField(record, idFieldIndex(shape));
                    addIndexEntries(j, storePrefix, kind, shape, codec, record, idVal);
                }
                cursor = result.getCursor();
            } while (!redis.clients.jedis.params.ScanParams.SCAN_POINTER_START.equals(cursor));
        } catch (RuntimeException e) {
            throw new BackendException("redis entity ensureStore failed for " + id.name(), e);
        }
    }
    @Override public void dropStore(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind) {}

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id, @NotNull Entity<?, ?, ?> kind, @NotNull Category<E> category) {
        String storePrefix = keyPrefix + id.name() + ":";
        return (event, sequence, endOfBatch) -> {
            Object record = ((Entity) kind).eventToRecord().apply(event);
            upsertRecord(storePrefix, kind, record);
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind,
                         @NotNull Operation<R> op) throws BackendException {
        String storePrefix = keyPrefix + id.name() + ":";
        try {
            if (op instanceof EntityOps.UpsertOp u)
                { upsertRecord(storePrefix, kind, u.record()); return null; }
            if (op instanceof EntityOps.GetByIdOp g)
                return (R) getById(storePrefix, kind, g);
            if (op instanceof EntityOps.GetManyOp g)
                return (R) getMany(storePrefix, kind, g);
            if (op instanceof EntityOps.FindByIndexOp f)
                return (R) findByIndex(storePrefix, kind, f);
            if (op instanceof EntityOps.PrefixIndexOp p)
                return (R) prefixIndex(storePrefix, kind, p);
            if (op instanceof EntityOps.DeleteByIdOp d)
                { deleteById(storePrefix, kind, d); return null; }
            if (op instanceof EntityOps.DeleteByIndexOp d)
                { deleteByIndex(storePrefix, kind, d); return null; }
            if (op instanceof EntityOps.CountByIndexOp c)
                return (R) Long.valueOf(countByIndex(storePrefix, kind, c));
            if (op instanceof EntityOps.ReassignSessionOp r)
                return (R) Long.valueOf(reassignSession(storePrefix, kind, r));
            if (op instanceof EntityOps.SetIfSentinelOp s)
                return (R) Long.valueOf(setIfSentinel(storePrefix, kind, s));
            throw new UnsupportedOperationException("RedisEntityAdapter: " + op.getClass().getName());
        } catch (RuntimeException e) {
            throw new BackendException("redis entity failed", e);
        }
    }

    @Override public @NotNull List<Migration<Entity<?, ?, ?>>> migrations(@NotNull Entity<?, ?, ?> kind) { return List.of(); }

    private void upsertRecord(
            @NotNull String storePrefix,
            @NotNull Entity<?, ?, ?> kind,
            @NotNull Object record) {
        if (!kind.recordType().isInstance(record)) {
            throw new IllegalArgumentException("record type " + record.getClass().getName()
                    + " does not match entity " + kind.recordType().getName());
        }
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        EncodeShape shape = kind.codec().shape();
        Object idVal = codec.readField(record, idFieldIndex(shape));
        String redisKey = storePrefix + RedisKeys.encode(idVal);
        mutateAtomically(storePrefix, kind, RedisKeys.encode(idVal), existing -> {
            Map<byte[], byte[]> merged = new LinkedHashMap<>(existing);
            for (int i = 0; i < shape.fields().size(); i++) {
                EncodeShape.FieldDef field = shape.fields().get(i);
                Object incoming = codec.readField(record, i);
                byte[] old = hashGet(existing, field.name());
                Object value = mergeField(field, old, incoming);
                putField(merged, field.name(), value == null ? null : fieldValueBytes(field, value));
            }
            return merged;
        });
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object mergeField(EncodeShape.FieldDef field, byte[] old, Object incoming) {
        if (old == null) return incoming;
        Object existing = parseField(field, old);
        return switch (field.mergeMode()) {
            case OVERWRITE -> incoming;
            case INSERT_ONLY, PRESERVE_ON_NON_NULL -> existing;
            case PRESERVE_ON_NON_SENTINEL -> ((Number) existing).longValue() == field.sentinelValue() ? incoming : existing;
            case MAX -> incoming == null || ((Comparable) existing).compareTo(incoming) >= 0 ? existing : incoming;
            case MIN -> incoming == null || ((Comparable) existing).compareTo(incoming) <= 0 ? existing : incoming;
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <ID, R> Optional<R> getById(String prefix, Entity<?, ?, ?> kind, EntityOps.GetByIdOp<ID, R> op) {
        String redisKey = prefix + RedisKeys.encode(op.id());
        try (var j = pool.getResource()) {
            Map<byte[], byte[]> hash = j.hgetAll(bytes(redisKey));
            if (hash == null || hash.isEmpty()) return Optional.empty();
            BsonCodec codec = BsonCodecs.regular(kind.recordType());
            EncodeShape shape = kind.codec().shape();
            Object[] args = new Object[shape.fields().size()];
            for (int i = 0; i < shape.fields().size(); i++) {
                EncodeShape.FieldDef f = shape.fields().get(i);
                byte[] raw = hashGet(hash, f.name());
                args[i] = raw == null ? null : parseField(f, raw);
            }
            return Optional.of((R) codec.decodeFromValues(args));
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <ID, R> List<R> getMany(String prefix, Entity<?, ?, ?> kind, EntityOps.GetManyOp<ID, R> op) {
        List<R> out = new ArrayList<>(op.ids().size());
        for (ID id : op.ids()) {
            Optional<R> one = getById(prefix, kind, new EntityOps.GetByIdOp(op.category(), id));
            one.ifPresent(out::add);
        }
        return out;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <R> Page<R> findByIndex(String prefix, Entity<?, ?, ?> kind, EntityOps.FindByIndexOp<R> op) {
        IndexSpec spec = requireIndex(kind, op.indexName());
        int offset = cursorOffset(op.cursor());
        int pageSize = Math.max(1, op.pageSize());
        String indexKey = exactIndexKey(prefix, spec, normalizedIndexValue(spec, op.key()));
        try (var j = pool.getResource()) {
            List<String> ids = j.zrange(indexKey, offset, offset + pageSize);
            return pageFromIds(prefix, kind, ids, pageSize, offset);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <R> Page<R> prefixIndex(String prefix, Entity<?, ?, ?> kind, EntityOps.PrefixIndexOp<R> op) {
        IndexSpec spec = requireIndex(kind, op.indexName());
        if (!spec.caseInsensitivePrefix()) {
            throw new UnsupportedOperationException(
                "RedisEntityAdapter: PrefixIndexOp requires caseInsensitivePrefix index, got " + spec.name());
        }
        int offset = cursorOffset(op.cursor());
        int pageSize = Math.max(1, op.pageSize());
        String normalizedPrefix = op.prefix().toLowerCase(Locale.ROOT);
        String min = "[" + normalizedPrefix;
        String max = "[" + normalizedPrefix + "\uffff";
        try (var j = pool.getResource()) {
            List<String> members = j.zrangeByLex(lexIndexKey(prefix, spec), min, max, offset, pageSize + 1);
            List<String> ids = new ArrayList<>(members.size());
            for (String member : members) ids.add(idFromLexMember(member));
            return pageFromIds(prefix, kind, ids, pageSize, offset);
        }
    }

    private long countByIndex(String prefix, Entity<?, ?, ?> kind, EntityOps.CountByIndexOp op) {
        IndexSpec spec = requireIndex(kind, op.indexName());
        String indexKey = exactIndexKey(prefix, spec, normalizedIndexValue(spec, op.key()));
        try (var j = pool.getResource()) {
            return j.zcard(indexKey);
        }
    }

    private long reassignSession(String prefix, Entity<?, ?, ?> kind, EntityOps.ReassignSessionOp op) {
        if (kind.recordType() != ac.grim.grimac.api.storage.model.SessionRecord.class) {
            throw new IllegalArgumentException("session reassignment requires the session entity");
        }
        return mutateAtomically(prefix, kind, RedisKeys.encode(op.sessionId()), existing -> {
            byte[] rawOwner = hashGet(existing, "startup_id");
            if (rawOwner == null) return null;
            UUID owner = UUID.fromString(new String(rawOwner, java.nio.charset.StandardCharsets.UTF_8));
            if (owner.equals(op.startupId()) || !op.previousStartups().contains(owner)) return null;
            Map<byte[], byte[]> updated = new LinkedHashMap<>(existing);
            putField(updated, "startup_id", bytes(op.startupId().toString()));
            putField(updated, "closed_at", bytes("0"));
            return updated;
        });
    }

    private long setIfSentinel(String prefix, Entity<?, ?, ?> kind, EntityOps.SetIfSentinelOp op) {
        EncodeShape shape = kind.codec().shape();
        EncodeShape.FieldDef targetDef = shape.fields().get(fieldIndex(shape, op.field()));
        EncodeShape.FieldDef fromDef = op.value() == null ? shape.fields().get(fieldIndex(shape, op.fromField())) : null;
        byte[] sentinel = fieldValueBytes(targetDef, op.sentinel());
        IndexSpec spec = op.indexName() == null ? null : requireIndex(kind, op.indexName());
        List<String> ids;
        try (var j = pool.getResource()) {
            ids = spec == null ? List.of(RedisKeys.encode(op.key()))
                    : j.zrange(exactIndexKey(prefix, spec, normalizedIndexValue(spec, op.key())), 0, -1);
        }
        long changed = 0L;
        for (String id : ids) {
            changed += mutateAtomically(prefix, kind, id, existing -> {
                if (existing.isEmpty()) return null;
                // An index scan can be stale after ownership transfer. Recheck
                // its selector while WATCH protects the actual row.
                if (spec != null) {
                    String leading = stripDir(spec.fields().get(0));
                    byte[] expected = fieldValueBytes(shape.fields().get(fieldIndex(shape, leading)), op.key());
                    if (!Arrays.equals(hashGet(existing, leading), expected)) return null;
                }
                byte[] current = hashGet(existing, op.field());
                if (!Arrays.equals(current, sentinel)) return null;
                byte[] source = fromDef == null ? null : hashGet(existing, fromDef.name());
                if (fromDef != null && source == null) return null;
                byte[] encoded = fieldValueBytes(targetDef, fromDef == null ? op.value() : parseField(fromDef, source));
                if (Arrays.equals(current, encoded)) return null;
                Map<byte[], byte[]> updated = new LinkedHashMap<>(existing);
                putField(updated, op.field(), encoded);
                return updated;
            });
        }
        return changed;
    }

    private record IndexEntry(String key, double score, String member) {}

    private List<IndexEntry> indexEntries(String prefix, Entity<?, ?, ?> kind, Map<byte[], byte[]> hash) {
        if (hash.isEmpty()) return List.of();
        EncodeShape shape = kind.codec().shape();
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        Object record = decodeHash(kind, hash);
        String id = RedisKeys.encode(codec.readField(record, idFieldIndex(shape)));
        List<IndexEntry> entries = new ArrayList<>();
        for (IndexSpec spec : kind.secondaryIndexes()) {
            Object leading = codec.readField(record, fieldIndex(shape, stripDir(spec.fields().get(0))));
            if (leading == null) continue;
            String normalized = normalizedIndexValue(spec, leading);
            entries.add(new IndexEntry(exactIndexKey(prefix, spec, normalized), orderedScoreFromRecord(shape, codec, record, spec), id));
            if (spec.caseInsensitivePrefix()) entries.add(new IndexEntry(lexIndexKey(prefix, spec), 0, lexMember(normalized, id)));
        }
        return entries;
    }

    private long mutateAtomically(String prefix, Entity<?, ?, ?> kind, String id,
            java.util.function.Function<Map<byte[], byte[]>, Map<byte[], byte[]>> update) {
        byte[] key = bytes(prefix + id);
        try (var j = pool.getResource()) {
            for (int attempt = 0; attempt < 32; attempt++) {
                j.watch(key);
                try {
                    Map<byte[], byte[]> existing = j.hgetAll(key);
                    Map<byte[], byte[]> next = update.apply(existing);
                    if (next == null) return 0L;
                    List<IndexEntry> before = indexEntries(prefix, kind, existing);
                    List<IndexEntry> after = indexEntries(prefix, kind, next);
                    try (var tx = j.multi()) {
                        for (IndexEntry index : before) tx.zrem(index.key(), index.member());
                        tx.del(key);
                        tx.hset(key, next);
                        for (IndexEntry index : after) tx.zadd(index.key(), index.score(), index.member());
                        if (tx.exec() != null) return 1L;
                    }
                } finally {
                    j.unwatch();
                }
            }
        }
        throw new IllegalStateException("Redis entity write conflicted repeatedly");
    }

    private static void putField(Map<byte[], byte[]> hash, String name, byte[] value) {
        byte[] key = bytes(name);
        hash.entrySet().removeIf(entry -> Arrays.equals(entry.getKey(), key));
        if (value != null) hash.put(key, value);
    }

    private void deleteById(String prefix, Entity<?, ?, ?> kind, EntityOps.DeleteByIdOp<?> op) {
        EncodeShape shape = kind.codec().shape();
        String key = prefix + RedisKeys.encode(op.id());
        try (var j = pool.getResource()) {
            Map<byte[], byte[]> existing = j.hgetAll(bytes(key));
            if (existing != null && !existing.isEmpty()) {
                removeIndexEntries(j, prefix, kind, shape, existing);
            }
            j.del(bytes(key));
        }
    }

    private void deleteByIndex(String prefix, Entity<?, ?, ?> kind, EntityOps.DeleteByIndexOp op) {
        IndexSpec spec = requireIndex(kind, op.indexName());
        String indexKey = exactIndexKey(prefix, spec, normalizedIndexValue(spec, op.key()));
        try (var j = pool.getResource()) {
            List<String> ids = j.zrange(indexKey, 0, -1);
            for (String id : ids) {
                Map<byte[], byte[]> existing = j.hgetAll(bytes(prefix + id));
                if (existing != null && !existing.isEmpty()) {
                    removeIndexEntries(j, prefix, kind, kind.codec().shape(), existing);
                }
                j.del(bytes(prefix + id));
            }
            j.del(indexKey);
        }
    }

    @SuppressWarnings("unchecked")
    private <R> Page<R> pageFromIds(String prefix, Entity<?, ?, ?> kind, List<String> ids,
                                    int pageSize, int offset) {
        boolean hasMore = ids.size() > pageSize;
        int limit = hasMore ? pageSize : ids.size();
        List<R> out = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            String id = ids.get(i);
            try (var j = pool.getResource()) {
                Map<byte[], byte[]> hash = j.hgetAll(bytes(prefix + id));
                if (hash == null || hash.isEmpty()) continue;
                out.add((R) decodeHash(kind, hash));
            }
        }
        Cursor next = hasMore ? new Cursor("redis:" + (offset + pageSize)) : null;
        return new Page<>(out, next);
    }

    private void addIndexEntries(redis.clients.jedis.Jedis j, String prefix, Entity<?, ?, ?> kind,
                                 EncodeShape shape, BsonCodec codec, Object record, Object idVal) {
        String id = RedisKeys.encode(idVal);
        for (IndexSpec spec : kind.secondaryIndexes()) {
            String leadingField = stripDir(spec.fields().get(0));
            Object leading = codec.readField(record, fieldIndex(shape, leadingField));
            if (leading == null) continue;
            String normalizedLeading = normalizedIndexValue(spec, leading);
            j.zadd(exactIndexKey(prefix, spec, normalizedLeading),
                orderedScoreFromRecord(shape, codec, record, spec), id);
            if (spec.caseInsensitivePrefix()) {
                j.zadd(lexIndexKey(prefix, spec), 0.0, lexMember(normalizedLeading, id));
            }
        }
    }

    private static void deleteIndexKeys(redis.clients.jedis.Jedis j, String prefix) {
        String cursor = redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;
        redis.clients.jedis.params.ScanParams params = new redis.clients.jedis.params.ScanParams()
            .match(prefix + "__idx:*")
            .count(1000);
        do {
            redis.clients.jedis.resps.ScanResult<String> result = j.scan(cursor, params);
            List<String> keys = result.getResult();
            if (!keys.isEmpty()) j.del(keys.toArray(String[]::new));
            cursor = result.getCursor();
        } while (!redis.clients.jedis.params.ScanParams.SCAN_POINTER_START.equals(cursor));
    }

    private void removeIndexEntries(redis.clients.jedis.Jedis j, String prefix, Entity<?, ?, ?> kind,
                                    EncodeShape shape, Map<byte[], byte[]> hash) {
        byte[] rawId = hashGet(hash, shape.idField());
        if (rawId == null) return;
        EncodeShape.FieldDef idDef = shape.fields().get(fieldIndex(shape, shape.idField()));
        String id = RedisKeys.encode(parseField(idDef, rawId));
        for (IndexSpec spec : kind.secondaryIndexes()) {
            byte[] leading = hashGet(hash, stripDir(spec.fields().get(0)));
            if (leading == null) continue;
            EncodeShape.FieldDef leadingDef = shape.fields().get(fieldIndex(shape, stripDir(spec.fields().get(0))));
            String normalizedLeading = normalizedIndexValue(spec, parseField(leadingDef, leading));
            j.zrem(exactIndexKey(prefix, spec, normalizedLeading), id);
            if (spec.caseInsensitivePrefix()) {
                j.zrem(lexIndexKey(prefix, spec), lexMember(normalizedLeading, id));
            }
        }
    }

    private Object decodeHash(Entity<?, ?, ?> kind, Map<byte[], byte[]> hash) {
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        EncodeShape shape = kind.codec().shape();
        Object[] args = new Object[shape.fields().size()];
        for (int i = 0; i < shape.fields().size(); i++) {
            EncodeShape.FieldDef f = shape.fields().get(i);
            byte[] raw = hashGet(hash, f.name());
            args[i] = raw == null ? null : parseField(f, raw);
        }
        return codec.decodeFromValues(args);
    }

    private static IndexSpec requireIndex(Entity<?, ?, ?> kind, String name) {
        for (IndexSpec spec : kind.secondaryIndexes()) {
            if (spec.name().equals(name)) return spec;
        }
        throw new IllegalArgumentException("entity " + kind.name() + " has no index " + name);
    }

    private static String exactIndexKey(String prefix, IndexSpec spec, String leading) {
        return prefix + "__idx:" + spec.name() + ":eq:" + leading;
    }

    private static String lexIndexKey(String prefix, IndexSpec spec) {
        return prefix + "__idx:" + spec.name() + ":lex";
    }

    private static String lexMember(String leading, String id) {
        return leading + '\0' + id;
    }

    private static String idFromLexMember(String member) {
        int sep = member.indexOf('\0');
        return sep < 0 ? member : member.substring(sep + 1);
    }

    private static String normalizedIndexValue(IndexSpec spec, Object value) {
        String encoded = RedisKeys.encode(value);
        return spec.caseInsensitivePrefix() ? encoded.toLowerCase(Locale.ROOT) : encoded;
    }

    private static double orderedScoreFromRecord(EncodeShape shape, BsonCodec codec,
                                                 Object record, IndexSpec spec) {
        if (spec.fields().size() <= 1) return 0.0;
        String field = spec.fields().get(1);
        Object value = codec.readField(record, fieldIndex(shape, stripDir(field)));
        return score(field, value);
    }

    private static double score(String field, Object value) {
        if (value == null) return 0.0;
        double score;
        if (value instanceof Number n) {
            score = n.doubleValue();
        } else {
            score = Double.parseDouble(RedisKeys.encode(value));
        }
        return field.startsWith("-") ? -score : score;
    }

    private static int cursorOffset(Cursor cursor) {
        if (cursor == null) return 0;
        String token = cursor.token();
        if (token.startsWith("redis:")) return Integer.parseInt(token.substring("redis:".length()));
        return Integer.parseInt(token);
    }

    private static String stripDir(String field) {
        return field.startsWith("-") ? field.substring(1) : field;
    }

    private static int fieldIndex(EncodeShape shape, String name) {
        for (int i = 0; i < shape.fields().size(); i++) {
            if (shape.fields().get(i).name().equals(name)) return i;
        }
        throw new IllegalArgumentException("shape has no field " + name);
    }

    static Object parseFieldStatic(@NotNull EncodeShape.FieldDef f, @NotNull String raw) {
        return parseField(f, raw);
    }

    static Object parseFieldBytesStatic(@NotNull EncodeShape.FieldDef f, byte[] raw) {
        return parseField(f, raw);
    }

    private static Object parseField(@NotNull EncodeShape.FieldDef f, byte[] raw) {
        if (f.javaType() == byte[].class) return raw;
        return parseField(f, new String(raw, java.nio.charset.StandardCharsets.UTF_8));
    }

    private static Object parseField(@NotNull EncodeShape.FieldDef f, @NotNull String raw) {
        Class<?> t = f.javaType();
        if (t == long.class || t == Long.class) return Long.parseLong(raw);
        if (t == int.class || t == Integer.class) return Integer.parseInt(raw);
        if (t == double.class || t == Double.class) return Double.parseDouble(raw);
        if (t == float.class || t == Float.class) return Float.parseFloat(raw);
        if (t == boolean.class || t == Boolean.class) return Boolean.parseBoolean(raw);
        if (t == UUID.class) return UUID.fromString(raw);
        if (t == String.class) return raw;
        if (t.isEnum()) {
            int ordinal = Integer.parseInt(raw);
            return t.getEnumConstants()[ordinal];
        }
        return raw;
    }

    private static byte[] fieldValueBytes(@NotNull EncodeShape.FieldDef f, @NotNull Object value) {
        if (f.javaType() == byte[].class) return (byte[]) value;
        return bytes(RedisKeys.encode(value));
    }

    private static byte[] bytes(@NotNull String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    static byte[] hashGet(@NotNull Map<byte[], byte[]> hash, @NotNull String field) {
        byte[] wanted = bytes(field);
        for (Map.Entry<byte[], byte[]> entry : hash.entrySet()) {
            if (Arrays.equals(entry.getKey(), wanted)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static int idFieldIndex(@NotNull EncodeShape shape) {
        for (int i = 0; i < shape.fields().size(); i++) {
            if (shape.fields().get(i).name().equals(shape.idField())) return i;
        }
        return 0;
    }
}
