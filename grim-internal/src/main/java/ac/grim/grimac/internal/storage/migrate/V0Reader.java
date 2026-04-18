package ac.grim.grimac.internal.storage.migrate;

import org.jetbrains.annotations.ApiStatus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Streams legacy v0 ({@code grim_history_*}) tables in order of
 * {@code (player_uuid, created_at)} so the session reconstructor can bucket by time-gap
 * without loading the whole dataset into memory.
 * <p>
 * Column layout is read from the original 2.0 SQLite schema:
 * <ul>
 *   <li>grim_history_violations — id, server_id, uuid (BLOB/BINARY(16)), check_name_id,
 *       verbose, vl, created_at, grim_version_id, client_brand_id, client_version_id,
 *       server_version_id</li>
 *   <li>lookup tables (grim_history_servers / _check_names / _versions / _client_brands
 *       / _client_versions / _server_versions) — (id, string)</li>
 * </ul>
 */
@ApiStatus.Internal
public final class V0Reader {

    private final String jdbcUrl;

    public V0Reader(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /** @return true iff the v0 database exists and has the expected tables. */
    public boolean isLegacyStorePresent() {
        try (Connection c = DriverManager.getConnection(jdbcUrl);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT name FROM sqlite_master WHERE type='table' AND name='grim_history_violations'");
             ResultSet rs = ps.executeQuery()) {
            return rs.next();
        } catch (SQLException e) {
            return false;
        }
    }

    public Map<Integer, String> loadLookup(String table, String stringColumn) throws SQLException {
        Map<Integer, String> out = new LinkedHashMap<>();
        try (Connection c = DriverManager.getConnection(jdbcUrl);
             PreparedStatement ps = c.prepareStatement("SELECT id, " + stringColumn + " FROM " + table);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.put(rs.getInt(1), rs.getString(2));
        }
        return out;
    }

    public long maxViolationId() throws SQLException {
        try (Connection c = DriverManager.getConnection(jdbcUrl);
             PreparedStatement ps = c.prepareStatement("SELECT COALESCE(MAX(id), 0) FROM grim_history_violations");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    /**
     * Stream violations with id &gt; {@code afterId}, ordered by
     * {@code (player_uuid, created_at, id)}. Emits one {@link V0Violation} per row.
     * Stops after {@code limit} rows so the caller can page + checkpoint between chunks.
     */
    public List<V0Violation> readChunk(long afterId, int limit) throws SQLException {
        List<V0Violation> out = new ArrayList<>(limit);
        try (Connection c = DriverManager.getConnection(jdbcUrl);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT id, server_id, uuid, check_name_id, verbose, vl, created_at, "
                             + "grim_version_id, client_brand_id, client_version_id, server_version_id "
                             + "FROM grim_history_violations WHERE id > ? "
                             + "ORDER BY uuid ASC, created_at ASC, id ASC LIMIT ?")) {
            ps.setLong(1, afterId);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(mapRow(rs));
            }
        }
        return out;
    }

    /**
     * Stream every violation in ascending (player_uuid, created_at, id) order without
     * intermediate accumulation. The consumer decides when to batch-commit.
     */
    public void streamAll(Consumer<V0Violation> consumer) throws SQLException {
        try (Connection c = DriverManager.getConnection(jdbcUrl);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT id, server_id, uuid, check_name_id, verbose, vl, created_at, "
                             + "grim_version_id, client_brand_id, client_version_id, server_version_id "
                             + "FROM grim_history_violations "
                             + "ORDER BY uuid ASC, created_at ASC, id ASC")) {
            ps.setFetchSize(1000);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) consumer.accept(mapRow(rs));
            }
        }
    }

    private static V0Violation mapRow(ResultSet rs) throws SQLException {
        long id = rs.getLong(1);
        int serverId = rs.getInt(2);
        UUID player = parseLegacyUuid(rs.getBytes(3));
        int checkId = rs.getInt(4);
        String verbose = rs.getString(5);
        double vl = rs.getDouble(6);
        long createdAt = rs.getLong(7);
        int grimV = rs.getInt(8);
        int brandId = rs.getInt(9);
        int clientV = rs.getInt(10);
        int serverV = rs.getInt(11);
        return new V0Violation(id, player, serverId, checkId, grimV, brandId, clientV, serverV, vl, createdAt, verbose);
    }

    private static UUID parseLegacyUuid(byte[] bytes) {
        if (bytes == null || bytes.length != 16) {
            throw new IllegalStateException("v0 uuid bytes must be 16, got " + (bytes == null ? -1 : bytes.length));
        }
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(bytes);
        return new UUID(bb.getLong(), bb.getLong());
    }

    public record V0Violation(
            long legacyId,
            UUID playerUuid,
            int serverId,
            int checkNameId,
            int grimVersionId,
            int clientBrandId,
            int clientVersionId,
            int serverVersionId,
            double vl,
            long createdAtEpochMs,
            String verbose) {}
}
