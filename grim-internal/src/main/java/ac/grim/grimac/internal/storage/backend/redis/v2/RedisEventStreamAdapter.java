package ac.grim.grimac.internal.storage.backend.redis.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EventStreamOps;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodec;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodecs;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.resps.Tuple;

import java.util.*;
import java.util.logging.Logger;

/**
 * Redis adapter for {@link EventStream} stores (e.g. violations).
 * Each event is a Redis HASH: {@code <prefix><storeId>:<eventId>}.
 * Partition indexes are sorted sets:
 * {@code <prefix><storeId>:by-<partition>:<partitionValue>}
 * scored by timestamp for range + pagination queries.
 */
@ApiStatus.Internal
public final class RedisEventStreamAdapter implements KindAdapter<EventStream<?, ?>> {

    private final @NotNull JedisPool pool;
    private final @NotNull String keyPrefix;
    private final @NotNull Logger logger;

    public RedisEventStreamAdapter(@NotNull JedisPool pool, @NotNull String keyPrefix,
                                   @NotNull Logger logger) {
        this.pool = pool;
        this.keyPrefix = keyPrefix;
        this.logger = logger;
    }

    @SuppressWarnings("unchecked")
    @Override public @NotNull Class<EventStream<?, ?>> kindType() {
        return (Class<EventStream<?, ?>>) (Class<?>) EventStream.class;
    }

    @Override public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_EVENT_STREAM);
    }

    @Override public void ensureStore(@NotNull StoreId id, @NotNull EventStream<?, ?> kind) {}
    @Override public void dropStore(@NotNull StoreId id, @NotNull EventStream<?, ?> kind) {}

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id, @NotNull EventStream<?, ?> kind, @NotNull Category<E> category) {
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        EncodeShape shape = kind.codec().shape();
        String storePrefix = keyPrefix + id.name() + ":";
        int idIdx = idFieldIndex(shape);
        int tsIdx = tsFieldIndex(shape, kind.timestampField());
        List<String> partitions = kind.partitionFields();

        return (event, sequence, endOfBatch) -> {
            Object record = ((EventStream) kind).eventToRecord().apply(event);
            Object idVal = codec.readField(record, idIdx);
            String eventId = RedisKeys.encode(idVal);
            String hashKey = storePrefix + eventId;

            Map<byte[], byte[]> fields = new LinkedHashMap<>();
            for (int i = 0; i < shape.fields().size(); i++) {
                Object v = codec.readField(record, i);
                if (v != null) {
                    EncodeShape.FieldDef f = shape.fields().get(i);
                    fields.put(bytes(f.name()), fieldValueBytes(f, v));
                }
            }

            Object tsVal = codec.readField(record, tsIdx);
            double score = tsVal instanceof Number n ? n.doubleValue() : 0.0;

            try (var j = pool.getResource()) {
                j.hset(bytes(hashKey), fields);
                for (String part : partitions) {
                    int partIdx = fieldIndex(shape, part);
                    Object partVal = codec.readField(record, partIdx);
                    if (partVal != null) {
                        String indexKey = storePrefix + "by-" + part + ":" + RedisKeys.encode(partVal);
                        j.zadd(indexKey, score, eventId);
                    }
                }
            }
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull EventStream<?, ?> kind,
                         @NotNull Operation<R> op) throws BackendException {
        String storePrefix = keyPrefix + id.name() + ":";
        try {
            if (op instanceof EventStreamOps.CountOp c)
                return (R) Long.valueOf(count(storePrefix, c));
            if (op instanceof EventStreamOps.DeleteByPartitionOp d)
                { deleteByPartition(storePrefix, kind, d); return null; }
            if (op instanceof EventStreamOps.DeleteOlderThanOp d)
                { deleteOlderThan(storePrefix, kind, d); return null; }
            if (op instanceof EventStreamOps.CountDistinctOp c)
                return (R) Long.valueOf(0L); // TODO: expensive scan
            if (op instanceof EventStreamOps.PageOp<?> p)
                return (R) page(storePrefix, kind, p);
            throw new UnsupportedOperationException("RedisEventStreamAdapter: " + op.getClass().getName());
        } catch (RuntimeException e) {
            throw new BackendException("redis event stream failed", e);
        }
    }

    @Override public @NotNull List<Migration<EventStream<?, ?>>> migrations(@NotNull EventStream<?, ?> kind) { return List.of(); }

    private long count(String prefix, EventStreamOps.CountOp op) {
        String indexKey = prefix + "by-" + op.partition() + ":" + RedisKeys.encode(op.key());
        try (var j = pool.getResource()) {
            return j.zcard(indexKey);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <R> Page<R> page(String prefix, EventStream<?, ?> kind, EventStreamOps.PageOp<R> op) {
        String indexKey = prefix + "by-" + op.partition() + ":" + RedisKeys.encode(op.key());
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        EncodeShape shape = kind.codec().shape();

        try (var j = pool.getResource()) {
            List<Tuple> members = j.zrangeWithScores(indexKey, 0, op.pageSize());
            List<R> items = new ArrayList<>(members.size());
            for (Tuple t : members) {
                String eventId = t.getElement();
                Map<byte[], byte[]> hash = j.hgetAll(bytes(prefix + eventId));
                if (hash == null || hash.isEmpty()) continue;
                Object[] args = new Object[shape.fields().size()];
                for (int i = 0; i < shape.fields().size(); i++) {
                    byte[] raw = RedisEntityAdapter.hashGet(hash, shape.fields().get(i).name());
                    args[i] = raw == null ? null : RedisEntityAdapter.parseFieldBytesStatic(shape.fields().get(i), raw);
                }
                items.add((R) codec.decodeFromValues(args));
            }
            Cursor nextCursor = members.size() > op.pageSize() ? new Cursor("next") : null;
            return new Page<>(items, nextCursor);
        }
    }

    private void deleteByPartition(String prefix, EventStream<?, ?> kind, EventStreamOps.DeleteByPartitionOp op) {
        String indexKey = prefix + "by-" + op.partition() + ":" + RedisKeys.encode(op.key());
        try (var j = pool.getResource()) {
            List<String> members = j.zrange(indexKey, 0, -1);
            for (String eventId : members) {
                j.del(bytes(prefix + eventId));
            }
            j.del(indexKey);
        }
    }

    private void deleteOlderThan(String prefix, EventStream<?, ?> kind, EventStreamOps.DeleteOlderThanOp op) {
        // Would need to scan all partition indexes. Simplified: no-op
        // for Redis (Redis TTL on individual keys is the native pattern).
        // TODO: implement via ZRANGEBYSCORE + DEL
    }

    private static int idFieldIndex(EncodeShape shape) {
        for (int i = 0; i < shape.fields().size(); i++)
            if (shape.fields().get(i).name().equals(shape.idField())) return i;
        return 0;
    }

    private static int tsFieldIndex(EncodeShape shape, String tsField) {
        for (int i = 0; i < shape.fields().size(); i++)
            if (shape.fields().get(i).name().equals(tsField)) return i;
        return 0;
    }

    private static int fieldIndex(EncodeShape shape, String name) {
        for (int i = 0; i < shape.fields().size(); i++)
            if (shape.fields().get(i).name().equals(name)) return i;
        return 0;
    }

    private static byte[] fieldValueBytes(@NotNull EncodeShape.FieldDef f, @NotNull Object value) {
        if (f.javaType() == byte[].class) return (byte[]) value;
        return bytes(RedisKeys.encode(value));
    }

    private static byte[] bytes(@NotNull String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
