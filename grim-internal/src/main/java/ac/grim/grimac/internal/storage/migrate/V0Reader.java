package ac.grim.grimac.internal.storage.migrate;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
 * {@code (player_uuid, created_at)} so the session reconstructor can bucket by
 * time-gap without loading the whole dataset into memory.
 * <p>
 * JDBC-source-agnostic — the caller supplies any URL. The old 2.0 plugin wrote
 * the same {@code grim_history_*} schema through three dialects (SQLite, MySQL,
 * PostgreSQL); the SQL in this class sticks to the portable subset that all
 * three parse identically.
 * <p>
 * Driver responsibility lives with the caller / host classpath. The running
 * Paper server that used the legacy stack must have the same JDBC driver on
 * its classpath that it did before cutover — if the driver is missing,
 * {@link #isLegacyStorePresent()} returns {@code false} (SQLException → null
 * presence) and migration cleanly no-ops rather than crashing Grim.
 */
@ApiStatus.Internal
public final class V0Reader {

    private final String jdbcUrl;
    private final @Nullable String username;
    private final @Nullable String password;

    public V0Reader(String jdbcUrl) {
        this(jdbcUrl, null, null);
    }

    public V0Reader(String jdbcUrl, @Nullable String username, @Nullable String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    private Connection open() throws SQLException {
        if (username == null && password == null) return DriverManager.getConnection(jdbcUrl);
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    /**
     * True iff the legacy v0 database can be opened and contains the expected
     * {@code grim_history_violations} table. Uses portable JDBC metadata so
     * MySQL/Postgres case-folding and SQLite's case-sensitive identifiers both
     * match.
     */
    public boolean isLegacyStorePresent() {
        try (Connection c = open()) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getTables(null, null, "grim_history_violations", null)) {
                if (rs.next()) return true;
            }
            try (ResultSet rs = md.getTables(null, null, "GRIM_HISTORY_VIOLATIONS", null)) {
                return rs.next();
            }
        } catch (SQLException e) {
            return false;
        }
    }

    public Map<Integer, String> loadLookup(String table, String stringColumn) throws SQLException {
        Map<Integer, String> out = new LinkedHashMap<>();
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement("SELECT id, " + stringColumn + " FROM " + table);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.put(rs.getInt(1), rs.getString(2));
        }
        return out;
    }

    public long maxViolationId() throws SQLException {
        try (Connection c = open();
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
        try (Connection c = open();
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
        try (Connection c = open();
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
