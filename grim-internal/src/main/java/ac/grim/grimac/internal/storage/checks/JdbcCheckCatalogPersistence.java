package ac.grim.grimac.internal.storage.checks;

import ac.grim.grimac.api.storage.check.CheckCatalogPersistence;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

@ApiStatus.Internal
public final class JdbcCheckCatalogPersistence implements CheckCatalogPersistence {

    @FunctionalInterface
    public interface ConnectionFactory {
        Connection open() throws SQLException;
    }

    private final ConnectionFactory connections;
    private final String checksTable;
    private final String sequenceAlignmentSql;

    public JdbcCheckCatalogPersistence(ConnectionFactory connections, String checksTable) {
        this(connections, checksTable, null);
    }

    public JdbcCheckCatalogPersistence(ConnectionFactory connections,
                                       String checksTable,
                                       @Nullable String sequenceAlignmentSql) {
        this.connections = connections;
        this.checksTable = checksTable;
        this.sequenceAlignmentSql = sequenceAlignmentSql;
    }

    @Override
    public Iterable<CheckCatalogRow> loadAll() {
        List<CheckCatalogRow> out = new ArrayList<>();
        try (Connection c = connections.open();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT check_id, stable_key, display, description, introduced_version, introduced_at "
                             + "FROM " + checksTable);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long introducedAt = rs.getLong(6);
                if (rs.wasNull()) introducedAt = 0L;
                out.add(new CheckCatalogRow(
                        rs.getInt(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getString(5),
                        introducedAt));
            }
        } catch (SQLException e) {
            throw new RuntimeException("failed to load " + checksTable, e);
        }
        return out;
    }

    @Override
    public int insert(String stableKey,
                      @Nullable String display,
                      @Nullable String description,
                      @Nullable String introducedVersion,
                      long introducedAt) {
        SQLException lastFailure = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try (Connection c = connections.open()) {
                boolean priorAutoCommit = c.getAutoCommit();
                c.setAutoCommit(false);
                try {
                    Integer existing = findExistingId(c, stableKey);
                    if (existing != null) {
                        c.commit();
                        return existing;
                    }

                    int checkId = nextCheckId(c);
                    try (PreparedStatement ps = c.prepareStatement(
                            "INSERT INTO " + checksTable
                                    + "(check_id, stable_key, display, description, introduced_version, introduced_at) "
                                    + "VALUES (?, ?, ?, ?, ?, ?)")) {
                        ps.setInt(1, checkId);
                        ps.setString(2, stableKey);
                        bindNullableString(ps, 3, display);
                        bindNullableString(ps, 4, description);
                        bindNullableString(ps, 5, introducedVersion);
                        ps.setLong(6, introducedAt);
                        ps.executeUpdate();
                    }
                    alignSequence(c);
                    c.commit();
                    return checkId;
                } catch (SQLException e) {
                    lastFailure = e;
                    try { c.rollback(); } catch (SQLException ignored) {}
                    Integer existing = findExistingId(stableKey);
                    if (existing != null) return existing;
                } finally {
                    try { c.setAutoCommit(priorAutoCommit); } catch (SQLException ignored) {}
                }
            } catch (SQLException e) {
                lastFailure = e;
            }
        }
        throw new RuntimeException("failed to insert " + checksTable + " row for " + stableKey, lastFailure);
    }

    @Override
    public void upsert(CheckCatalogRow row) {
        validateNoConflict(row);
        try (Connection c = connections.open()) {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + checksTable
                            + "(check_id, stable_key, display, description, introduced_version, introduced_at) "
                            + "VALUES (?, ?, ?, ?, ?, ?)")) {
                bindRow(ps, row);
                ps.executeUpdate();
            } catch (SQLException duplicateOrConflict) {
                validateNoConflict(row);
                try (PreparedStatement ps = c.prepareStatement(
                        "UPDATE " + checksTable
                                + " SET stable_key = ?, display = ?, description = ?, introduced_version = ?, introduced_at = ? "
                                + "WHERE check_id = ?")) {
                    ps.setString(1, row.stableKey());
                    bindNullableString(ps, 2, row.display());
                    bindNullableString(ps, 3, row.description());
                    bindNullableString(ps, 4, row.introducedVersion());
                    ps.setLong(5, row.introducedAt());
                    ps.setInt(6, row.checkId());
                    if (ps.executeUpdate() == 0) throw duplicateOrConflict;
                }
            }
            alignSequence(c);
        } catch (SQLException e) {
            throw new RuntimeException("failed to upsert " + checksTable + " row for " + row.stableKey(), e);
        }
    }

    private void validateNoConflict(CheckCatalogRow row) {
        String stableKeyForId = findStableKeyById(row.checkId());
        if (stableKeyForId != null && !stableKeyForId.equals(row.stableKey())) {
            throw new IllegalStateException("check_id " + row.checkId()
                    + " already maps to stable key " + stableKeyForId + ", cannot import " + row.stableKey());
        }
        Integer idForStableKey = findExistingId(row.stableKey());
        if (idForStableKey != null && idForStableKey != row.checkId()) {
            throw new IllegalStateException("stable key " + row.stableKey()
                    + " already maps to check_id " + idForStableKey + ", cannot import as " + row.checkId());
        }
    }

    @Override
    public void updateDisplayAndDescription(int checkId,
                                            @Nullable String display,
                                            @Nullable String description) {
        try (Connection c = connections.open();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE " + checksTable + " SET display = ?, description = ? WHERE check_id = ?")) {
            bindNullableString(ps, 1, display);
            bindNullableString(ps, 2, description);
            ps.setInt(3, checkId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("failed to update " + checksTable + " display/description", e);
        }
    }

    private Integer findExistingId(String stableKey) {
        try (Connection c = connections.open();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT check_id FROM " + checksTable + " WHERE stable_key = ?")) {
            ps.setString(1, stableKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        } catch (SQLException ignore) {
        }
        return null;
    }

    private Integer findExistingId(Connection c, String stableKey) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT check_id FROM " + checksTable + " WHERE stable_key = ?")) {
            ps.setString(1, stableKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private int nextCheckId(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT COALESCE(MAX(check_id), 0) + 1 FROM " + checksTable);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
        }
        return 1;
    }

    private String findStableKeyById(int checkId) {
        try (Connection c = connections.open();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT stable_key FROM " + checksTable + " WHERE check_id = ?")) {
            ps.setInt(1, checkId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString(1);
            }
        } catch (SQLException ignore) {
        }
        return null;
    }

    private void bindRow(PreparedStatement ps, CheckCatalogRow row) throws SQLException {
        ps.setInt(1, row.checkId());
        ps.setString(2, row.stableKey());
        bindNullableString(ps, 3, row.display());
        bindNullableString(ps, 4, row.description());
        bindNullableString(ps, 5, row.introducedVersion());
        ps.setLong(6, row.introducedAt());
    }

    private void alignSequence(Connection c) throws SQLException {
        if (sequenceAlignmentSql == null || sequenceAlignmentSql.isBlank()) return;
        try (Statement s = c.createStatement()) {
            s.execute(sequenceAlignmentSql);
        }
    }

    private static void bindNullableString(PreparedStatement ps, int index, @Nullable String value)
            throws SQLException {
        if (value == null) ps.setNull(index, Types.VARCHAR);
        else ps.setString(index, value);
    }
}
