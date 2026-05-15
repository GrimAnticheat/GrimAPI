package ac.grim.grimac.internal.storage.backend.redis;

import ac.grim.grimac.api.storage.check.CheckCatalogPersistence;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApiStatus.Internal
final class RedisCheckCatalogPersistence implements CheckCatalogPersistence {

    private static final String UPSERT_SCRIPT =
            "local existingId = redis.call('GET', KEYS[1]); "
                    + "if existingId and existingId ~= ARGV[1] then return {'stable', existingId}; end; "
                    + "local existingStable = redis.call('HGET', KEYS[2], 'stable_key'); "
                    + "if existingStable and existingStable ~= ARGV[2] then return {'id', existingStable}; end; "
                    + "redis.call('SET', KEYS[1], ARGV[1]); "
                    + "redis.call('DEL', KEYS[2]); "
                    + "redis.call('HSET', KEYS[2], "
                    + "'check_id', ARGV[1], 'stable_key', ARGV[2], 'introduced_at', ARGV[9]); "
                    + "if ARGV[3] == '1' then redis.call('HSET', KEYS[2], 'display', ARGV[4]); end; "
                    + "if ARGV[5] == '1' then redis.call('HSET', KEYS[2], 'description', ARGV[6]); end; "
                    + "if ARGV[7] == '1' then redis.call('HSET', KEYS[2], 'introduced_version', ARGV[8]); end; "
                    + "redis.call('SADD', KEYS[3], ARGV[1]); "
                    + "local cur = redis.call('HGET', KEYS[4], 'check_id_seq'); "
                    + "if (not cur) or (tonumber(cur) < tonumber(ARGV[1])) then "
                    + "redis.call('HSET', KEYS[4], 'check_id_seq', ARGV[1]); "
                    + "end; "
                    + "return {'ok'};";

    private final JedisPool pool;
    private final String metaKey;
    private final String checksKey;

    RedisCheckCatalogPersistence(JedisPool pool, String metaKey, String checksKey) {
        this.pool = pool;
        this.metaKey = metaKey;
        this.checksKey = checksKey;
    }

    @Override
    public Iterable<CheckCatalogRow> loadAll() {
        List<CheckCatalogRow> out = new ArrayList<>();
        try (Jedis j = pool.getResource()) {
            for (String id : j.smembers(idsKey())) {
                Map<String, String> h = j.hgetAll(rowKey(id));
                CheckCatalogRow row = toRow(id, h);
                if (row != null) out.add(row);
            }
        }
        return out;
    }

    @Override
    public int insert(String stableKey,
                      @Nullable String display,
                      @Nullable String description,
                      @Nullable String introducedVersion,
                      long introducedAt) {
        try (Jedis j = pool.getResource()) {
            String stableKeyIndex = stableKeyIndex(stableKey);
            String existing = j.get(stableKeyIndex);
            if (existing != null) return Integer.parseInt(existing);

            long id = j.hincrBy(metaKey, "check_id_seq", 1);
            String idString = Long.toString(id);
            // If another writer wins the stable-key SETNX race, this id is
            // intentionally skipped. Sequence holes are harmless; remapping a
            // stable key is not.
            if (j.setnx(stableKeyIndex, idString) == 0L) {
                String raced = j.get(stableKeyIndex);
                if (raced != null) return Integer.parseInt(raced);
                throw new IllegalStateException("Redis check stable-key index vanished during insert");
            }

            Map<String, String> h = new HashMap<>();
            h.put("check_id", idString);
            h.put("stable_key", stableKey);
            if (display != null) h.put("display", display);
            if (description != null) h.put("description", description);
            if (introducedVersion != null) h.put("introduced_version", introducedVersion);
            h.put("introduced_at", Long.toString(introducedAt));
            j.hset(rowKey(idString), h);
            j.sadd(idsKey(), idString);
            return (int) id;
        }
    }

    @Override
    public void upsert(CheckCatalogRow row) {
        try (Jedis j = pool.getResource()) {
            String id = Integer.toString(row.checkId());
            Object result = j.eval(UPSERT_SCRIPT,
                    List.of(stableKeyIndex(row.stableKey()), rowKey(id), idsKey(), metaKey),
                    List.of(
                            id,
                            row.stableKey(),
                            row.display() == null ? "0" : "1",
                            row.display() == null ? "" : row.display(),
                            row.description() == null ? "0" : "1",
                            row.description() == null ? "" : row.description(),
                            row.introducedVersion() == null ? "0" : "1",
                            row.introducedVersion() == null ? "" : row.introducedVersion(),
                            Long.toString(row.introducedAt())));
            handleUpsertResult(row, id, result);
        }
    }

    @Override
    public void updateDisplayAndDescription(int checkId,
                                            @Nullable String display,
                                            @Nullable String description) {
        try (Jedis j = pool.getResource()) {
            String key = rowKey(Integer.toString(checkId));
            if (display == null) j.hdel(key, "display");
            else j.hset(key, "display", display);
            if (description == null) j.hdel(key, "description");
            else j.hset(key, "description", description);
        }
    }

    private String idsKey() {
        return checksKey + ":ids";
    }

    private String rowKey(String id) {
        return checksKey + ":" + id;
    }

    private String stableKeyIndex(String stableKey) {
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(stableKey.getBytes(StandardCharsets.UTF_8));
        return checksKey + ":by-stable:" + encoded;
    }

    private static @Nullable CheckCatalogRow toRow(String id, Map<String, String> h) {
        if (h.isEmpty()) return null;
        String stableKey = h.get("stable_key");
        if (stableKey == null) return null;
        return new CheckCatalogRow(
                Integer.parseInt(h.getOrDefault("check_id", id)),
                stableKey,
                h.get("display"),
                h.get("description"),
                h.get("introduced_version"),
                parseLong(h.get("introduced_at")));
    }

    private static void handleUpsertResult(CheckCatalogRow row, String id, Object result) {
        if (!(result instanceof List<?> parts) || parts.isEmpty()) {
            throw new IllegalStateException("Redis check catalog upsert returned " + result);
        }
        String status = redisString(parts.get(0));
        if ("ok".equals(status)) return;
        String value = parts.size() > 1 ? redisString(parts.get(1)) : "unknown";
        if ("stable".equals(status)) {
            throw new IllegalStateException("stable key " + row.stableKey()
                    + " already maps to check_id " + value + ", cannot import as " + id);
        }
        if ("id".equals(status)) {
            throw new IllegalStateException("check_id " + id
                    + " already maps to stable key " + value + ", cannot import " + row.stableKey());
        }
        throw new IllegalStateException("Redis check catalog upsert returned status " + status);
    }

    private static String redisString(Object value) {
        if (value instanceof byte[] bytes) return new String(bytes, StandardCharsets.UTF_8);
        return String.valueOf(value);
    }

    private static long parseLong(@Nullable String s) {
        if (s == null) return 0L;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {
            return 0L;
        }
    }
}
