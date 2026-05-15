package ac.grim.grimac.internal.storage.checks;

import ac.grim.grimac.api.storage.check.CheckCatalogRepairResult;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

@ApiStatus.Internal
public final class JdbcCheckCatalogRepair {

    public static final String STUB_VERSION = "0.0.0-stub";

    private JdbcCheckCatalogRepair() {}

    public static @NotNull CheckCatalogRepairResult run(
            @NotNull JdbcCheckCatalogPersistence.ConnectionFactory connections,
            @NotNull String checksTable,
            @NotNull String violationsTable,
            @NotNull Map<Integer, Integer> legacyToCatalogCheckIds,
            @Nullable String introducedVersionReplacement) throws SQLException {
        try (Connection c = connections.open()) {
            boolean autoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                long violationsUpdated = rewriteViolationIds(c, violationsTable, legacyToCatalogCheckIds);
                long versionsUpdated = repairStubVersions(c, checksTable, introducedVersionReplacement);
                c.commit();
                return new CheckCatalogRepairResult(
                        legacyToCatalogCheckIds.size(),
                        violationsUpdated,
                        versionsUpdated);
            } catch (SQLException | RuntimeException e) {
                try { c.rollback(); } catch (SQLException ignored) {}
                throw e;
            } finally {
                try { c.setAutoCommit(autoCommit); } catch (SQLException ignored) {}
            }
        }
    }

    private static long rewriteViolationIds(
            Connection c,
            String violationsTable,
            Map<Integer, Integer> legacyToCatalogCheckIds) throws SQLException {
        if (legacyToCatalogCheckIds.isEmpty()) return 0L;
        long updated = 0L;
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE " + violationsTable + " SET check_id = ? WHERE check_id = ?")) {
            for (Map.Entry<Integer, Integer> e : legacyToCatalogCheckIds.entrySet()) {
                int legacyId = e.getKey();
                int catalogId = e.getValue();
                if (legacyId == catalogId) continue;
                ps.setInt(1, catalogId);
                ps.setInt(2, legacyId);
                updated += ps.executeUpdate();
            }
        }
        return updated;
    }

    private static long repairStubVersions(
            Connection c,
            String checksTable,
            @Nullable String introducedVersionReplacement) throws SQLException {
        if (introducedVersionReplacement == null || introducedVersionReplacement.isBlank()) return 0L;
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE " + checksTable
                        + " SET introduced_version = ? WHERE introduced_version = ?")) {
            ps.setString(1, introducedVersionReplacement);
            ps.setString(2, STUB_VERSION);
            return ps.executeUpdate();
        }
    }
}
