package ac.grim.grimac.internal.storage.backend.redis.v2;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RedisEntityAdapterTest {

    @Test
    void hashGetMatchesByteArrayFieldNamesByContent() {
        byte[] expected = new byte[] { 1, 2, 3 };
        Map<byte[], byte[]> hash = new LinkedHashMap<>();
        hash.put("verbose".getBytes(StandardCharsets.UTF_8), expected);

        assertArrayEquals(expected, RedisEntityAdapter.hashGet(hash, "verbose"));
        assertNull(RedisEntityAdapter.hashGet(hash, "missing"));
    }
}
