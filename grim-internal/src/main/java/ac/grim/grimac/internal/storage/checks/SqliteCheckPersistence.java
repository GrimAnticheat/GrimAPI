package ac.grim.grimac.internal.storage.checks;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * SQLite-backed {@link CheckRegistry.CheckPersistence}. Opens a fresh connection per op
 * (same pattern as {@link ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackend}
 * reads) so no writer-lock ordering issues.
 */
@ApiStatus.Internal
public final class SqliteCheckPersistence implements CheckRegistry.CheckPersistence {

    private final String jdbcUrl;
    private final String checksTable;

    /**
     * @param jdbcUrl JDBC URL for the backing SQLite database.
     * @param checksTable Name of the {@code checks} table — read from the
     *                    SQLite backend's {@link TableNames} so overrides
     *                    stay consistent with the backend's schema.
     */
    public SqliteCheckPersistence(String jdbcUrl, String checksTable) {
        this.jdbcUrl = jdbcUrl;
        this.checksTable = checksTable;
    }

    /** Back-compat ctor using default {@link TableNames#DEFAULT_CHECKS}. */
    public SqliteCheckPersistence(String jdbcUrl) {
        this(jdbcUrl, TableNames.DEFAULT_CHECKS);
    }

    @Override
    public Iterable<CheckRegistry.CheckRow> loadAll() {
        List<CheckRegistry.CheckRow> out = new ArrayList<>();
        try (Connection c = DriverManager.getConnection(jdbcUrl);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT check_id, stable_key, display, description, introduced_version, introduced_at "
                             + "FROM " + checksTable);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long introducedAt = rs.getLong(6);
                if (rs.wasNull()) introducedAt = 0L;
                out.add(new CheckRegistry.CheckRow(
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
        try (Connection c = DriverManager.getConnection(jdbcUrl);
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO " + checksTable + "(stable_key, display, description, introduced_version, introduced_at) "
                             + "VALUES (?, ?, ?, ?, ?)",
                     Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, stableKey);
            if (display == null) ps.setNull(2, Types.VARCHAR); else ps.setString(2, display);
            if (description == null) ps.setNull(3, Types.VARCHAR); else ps.setString(3, description);
            if (introducedVersion == null) ps.setNull(4, Types.VARCHAR); else ps.setString(4, introducedVersion);
            ps.setLong(5, introducedAt);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) return keys.getInt(1);
                throw new SQLException("no generated key for " + checksTable + " insert");
            }
        } catch (SQLException e) {
            // Race: another writer may have interned between our check and this insert.
            // Return the existing row's id instead of failing.
            try (Connection c = DriverManager.getConnection(jdbcUrl);
                 PreparedStatement ps = c.prepareStatement("SELECT check_id FROM " + checksTable + " WHERE stable_key = ?")) {
                ps.setString(1, stableKey);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return rs.getInt(1);
                }
            } catch (SQLException ignore) {}
            throw new RuntimeException("failed to insert " + checksTable + " row for " + stableKey, e);
        }
    }

    @Override
    public void updateDisplayAndDescription(int checkId,
                                            @Nullable String display,
                                            @Nullable String description) {
        try (Connection c = DriverManager.getConnection(jdbcUrl);
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE " + checksTable + " SET display = ?, description = ? WHERE check_id = ?")) {
            if (display == null) ps.setNull(1, Types.VARCHAR); else ps.setString(1, display);
            if (description == null) ps.setNull(2, Types.VARCHAR); else ps.setString(2, description);
            ps.setInt(3, checkId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("failed to update " + checksTable + " display/description", e);
        }
    }
}
