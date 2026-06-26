package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.instance.OwnershipClaimResult;
import ac.grim.grimac.api.storage.instance.OwnershipRenewResult;
import ac.grim.grimac.api.storage.instance.ServerOwnershipAdapter;
import ac.grim.grimac.api.storage.instance.ServerOwnershipMetadata;
import ac.grim.grimac.api.storage.instance.ServerOwnershipSnapshot;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SQL implementation of the authoritative server-ownership lease primitive.
 * Claim uses a transaction and row lock on multi-writer SQL engines; SQLite is
 * kept as local best-effort only.
 */
@ApiStatus.Internal
public final class SqlServerOwnershipAdapter implements ServerOwnershipAdapter {

    private final @NotNull DataSource ds;
    private final @NotNull SqlDialect dialect;
    private final @NotNull Logger logger;

    public SqlServerOwnershipAdapter(
            @NotNull DataSource ds,
            @NotNull SqlDialect dialect,
            @NotNull Logger logger) {
        this.ds = ds;
        this.dialect = dialect;
        this.logger = logger;
    }

    @Override
    public void ensureStore(@NotNull StoreId id) throws BackendException {
        String table = q(id.name());
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE IF NOT EXISTS " + table + " ("
                    + q("persistent_id") + " " + textColumnType() + " PRIMARY KEY, "
                    + q("owner_startup_id") + " " + textColumnType() + " NOT NULL, "
                    + q("fence") + " " + textColumnType() + " NOT NULL, "
                    + q("lease_expires_at_ms") + " BIGINT NOT NULL, "
                    + q("last_renewed_at_ms") + " BIGINT NOT NULL, "
                    + q("closed_at_ms") + " BIGINT NOT NULL, "
                    + q("close_reason") + " " + textColumnType() + ", "
                    + q("server_name") + " " + textColumnType() + ", "
                    + q("hostname") + " " + textColumnType() + ", "
                    + q("grim_version") + " " + textColumnType() + ", "
                    + q("server_version") + " " + textColumnType()
                    + ")");
        } catch (SQLException e) {
            throw new BackendException("failed to ensure server ownership table " + id.name(), e);
        }
    }

    @Override
    public long dbNowEpochMs() throws BackendException {
        try (Connection c = ds.getConnection()) {
            return dbNowEpochMs(c);
        } catch (SQLException e) {
            throw new BackendException("failed to read DB time", e);
        }
    }

    @Override
    public @NotNull OwnershipClaimResult claimOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs,
            @NotNull ServerOwnershipMetadata metadata) throws BackendException {
        SQLException lastDuplicate = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try (Connection c = ds.getConnection()) {
                boolean originalAutoCommit = c.getAutoCommit();
                try {
                    c.setAutoCommit(false);
                    long now = dbNowEpochMs(c);
                    ServerOwnershipSnapshot previous = select(c, id, persistentId, true).orElse(null);
                    if (previous == null) {
                        insert(c, id, persistentId, startupId, fence, now, now + ttlMs, metadata);
                        c.commit();
                        return OwnershipClaimResult.claimed(
                                persistentId, startupId, fence, now, now + ttlMs, null);
                    }

                    boolean alreadyMine = startupId.equals(previous.ownerStartupId())
                            && fence.equals(previous.fence());
                    boolean claimable = alreadyMine
                            || previous.closedAtEpochMs() != ServerOwnershipSnapshot.OPEN
                            || previous.leaseExpiresAtEpochMs() <= now;
                    if (!claimable) {
                        c.commit();
                        return OwnershipClaimResult.denied(
                                persistentId, startupId, fence, now, previous);
                    }

                    updateOwner(c, id, persistentId, startupId, fence, now, now + ttlMs, metadata);
                    c.commit();
                    return OwnershipClaimResult.claimed(
                            persistentId, startupId, fence, now, now + ttlMs, previous);
                } catch (SQLException e) {
                    rollbackQuietly(c);
                    if (isDuplicate(e)) {
                        lastDuplicate = e;
                        continue;
                    }
                    throw e;
                } finally {
                    try { c.setAutoCommit(originalAutoCommit); }
                    catch (SQLException ignored) {}
                }
            } catch (SQLException e) {
                throw new BackendException("failed to claim server ownership", e);
            }
        }
        throw new BackendException("failed to claim server ownership after duplicate-key retries", lastDuplicate);
    }

    @Override
    public @NotNull OwnershipRenewResult renewOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs) throws BackendException {
        try (Connection c = ds.getConnection()) {
            long now = dbNowEpochMs(c);
            String sql = "UPDATE " + q(id.name()) + " SET "
                    + q("lease_expires_at_ms") + " = ?, "
                    + q("last_renewed_at_ms") + " = ?, "
                    + q("closed_at_ms") + " = 0, "
                    + q("close_reason") + " = NULL "
                    + "WHERE " + q("persistent_id") + " = ? "
                    + "AND " + q("owner_startup_id") + " = ? "
                    + "AND " + q("fence") + " = ? "
                    + "AND " + q("closed_at_ms") + " = 0 "
                    + "AND " + q("lease_expires_at_ms") + " > ?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setLong(1, now + ttlMs);
                ps.setLong(2, now);
                bindUuid(ps, 3, persistentId);
                bindUuid(ps, 4, startupId);
                bindUuid(ps, 5, fence);
                ps.setLong(6, now);
                int rows = ps.executeUpdate();
                if (rows == 1) {
                    return OwnershipRenewResult.renewed(
                            persistentId, startupId, fence, now, now + ttlMs);
                }
                return OwnershipRenewResult.lost(persistentId, startupId, fence, now);
            }
        } catch (SQLException e) {
            throw new BackendException("failed to renew server ownership", e);
        }
    }

    @Override
    public boolean closeOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            @NotNull String reason) throws BackendException {
        try (Connection c = ds.getConnection()) {
            long now = dbNowEpochMs(c);
            String sql = "UPDATE " + q(id.name()) + " SET "
                    + q("closed_at_ms") + " = ?, "
                    + q("close_reason") + " = ? "
                    + "WHERE " + q("persistent_id") + " = ? "
                    + "AND " + q("owner_startup_id") + " = ? "
                    + "AND " + q("fence") + " = ? "
                    + "AND " + q("closed_at_ms") + " = 0";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setLong(1, now);
                ps.setString(2, reason);
                bindUuid(ps, 3, persistentId);
                bindUuid(ps, 4, startupId);
                bindUuid(ps, 5, fence);
                return ps.executeUpdate() == 1;
            }
        } catch (SQLException e) {
            throw new BackendException("failed to close server ownership", e);
        }
    }

    @Override
    public @NotNull Optional<ServerOwnershipSnapshot> readOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId) throws BackendException {
        try (Connection c = ds.getConnection()) {
            return select(c, id, persistentId, false);
        } catch (SQLException e) {
            throw new BackendException("failed to read server ownership", e);
        }
    }

    private long dbNowEpochMs(@NotNull Connection c) throws SQLException {
        String sql = switch (dialect.name()) {
            case "postgres" -> "SELECT CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)";
            case "mysql" -> "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS UNSIGNED)";
            case "sqlite" -> "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";
            default -> "SELECT CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)";
        };
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) throw new SQLException("DB time query returned no rows");
            return rs.getLong(1);
        }
    }

    private Optional<ServerOwnershipSnapshot> select(
            @NotNull Connection c,
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            boolean forUpdate) throws SQLException {
        String sql = "SELECT * FROM " + q(id.name())
                + " WHERE " + q("persistent_id") + " = ?"
                + (forUpdate && !"sqlite".equals(dialect.name()) ? " FOR UPDATE" : "");
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            bindUuid(ps, 1, persistentId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(readSnapshot(rs));
            }
        }
    }

    private void insert(
            @NotNull Connection c,
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long now,
            long leaseExpiresAt,
            @NotNull ServerOwnershipMetadata metadata) throws SQLException {
        String sql = "INSERT INTO " + q(id.name()) + " ("
                + q("persistent_id") + ", "
                + q("owner_startup_id") + ", "
                + q("fence") + ", "
                + q("lease_expires_at_ms") + ", "
                + q("last_renewed_at_ms") + ", "
                + q("closed_at_ms") + ", "
                + q("close_reason") + ", "
                + q("server_name") + ", "
                + q("hostname") + ", "
                + q("grim_version") + ", "
                + q("server_version")
                + ") VALUES (?, ?, ?, ?, ?, 0, NULL, ?, ?, ?, ?)";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            bindOwnerFields(ps, persistentId, startupId, fence, leaseExpiresAt, now, metadata);
            ps.executeUpdate();
        }
    }

    private void updateOwner(
            @NotNull Connection c,
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long now,
            long leaseExpiresAt,
            @NotNull ServerOwnershipMetadata metadata) throws SQLException {
        String sql = "UPDATE " + q(id.name()) + " SET "
                + q("owner_startup_id") + " = ?, "
                + q("fence") + " = ?, "
                + q("lease_expires_at_ms") + " = ?, "
                + q("last_renewed_at_ms") + " = ?, "
                + q("closed_at_ms") + " = 0, "
                + q("close_reason") + " = NULL, "
                + q("server_name") + " = ?, "
                + q("hostname") + " = ?, "
                + q("grim_version") + " = ?, "
                + q("server_version") + " = ? "
                + "WHERE " + q("persistent_id") + " = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            bindUuid(ps, 1, startupId);
            bindUuid(ps, 2, fence);
            ps.setLong(3, leaseExpiresAt);
            ps.setLong(4, now);
            ps.setString(5, metadata.serverName());
            ps.setString(6, metadata.hostname());
            ps.setString(7, metadata.grimVersion());
            ps.setString(8, metadata.serverVersionString());
            bindUuid(ps, 9, persistentId);
            ps.executeUpdate();
        }
    }

    private void bindOwnerFields(
            @NotNull PreparedStatement ps,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long leaseExpiresAt,
            long now,
            @NotNull ServerOwnershipMetadata metadata) throws SQLException {
        bindUuid(ps, 1, persistentId);
        bindUuid(ps, 2, startupId);
        bindUuid(ps, 3, fence);
        ps.setLong(4, leaseExpiresAt);
        ps.setLong(5, now);
        ps.setString(6, metadata.serverName());
        ps.setString(7, metadata.hostname());
        ps.setString(8, metadata.grimVersion());
        ps.setString(9, metadata.serverVersionString());
    }

    private @NotNull ServerOwnershipSnapshot readSnapshot(@NotNull ResultSet rs) throws SQLException {
        return new ServerOwnershipSnapshot(
                readUuid(rs, "persistent_id"),
                readUuid(rs, "owner_startup_id"),
                readUuid(rs, "fence"),
                rs.getLong("lease_expires_at_ms"),
                rs.getLong("last_renewed_at_ms"),
                rs.getLong("closed_at_ms"),
                rs.getString("close_reason"),
                rs.getString("server_name"),
                rs.getString("hostname"),
                rs.getString("grim_version"),
                rs.getString("server_version"));
    }

    private void bindUuid(@NotNull PreparedStatement ps, int idx, @NotNull UUID uuid) throws SQLException {
        ps.setString(idx, uuid.toString());
    }

    private @NotNull UUID readUuid(@NotNull ResultSet rs, @NotNull String column) throws SQLException {
        Object value = rs.getObject(column);
        if (value instanceof UUID u) return u;
        if (value instanceof String s) return UUID.fromString(s);
        throw new SQLException("cannot read UUID column " + column + " from "
                + (value == null ? "null" : value.getClass().getName()));
    }

    private @NotNull String textColumnType() {
        return "mysql".equals(dialect.name()) ? "VARCHAR(512)" : "TEXT";
    }

    private @NotNull String q(@NotNull String identifier) {
        return dialect.quoteIdentifier(identifier);
    }

    private static void rollbackQuietly(@NotNull Connection c) {
        try { c.rollback(); }
        catch (SQLException ignored) {}
    }

    private boolean isDuplicate(@NotNull SQLException e) {
        String state = e.getSQLState();
        if (state != null && state.startsWith("23")) return true;
        String message = e.getMessage();
        boolean duplicate = message != null
                && message.toLowerCase(Locale.ROOT).contains("duplicate");
        if (duplicate) {
            logger.log(Level.FINE, "duplicate ownership insert race; retrying", e);
        }
        return duplicate;
    }
}
