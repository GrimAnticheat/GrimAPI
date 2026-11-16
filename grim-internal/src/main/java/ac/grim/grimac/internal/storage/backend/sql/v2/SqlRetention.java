package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import ac.grim.grimac.internal.storage.retention.DeleteOlderThanOp;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** {@link DeleteOlderThanOp} for the SQL entity and event-stream adapters. */
@ApiStatus.Internal
final class SqlRetention {

    private SqlRetention() {}

    static long deleteOlderThan(@NotNull DataSource ds, @NotNull SqlDialect dialect, @NotNull String table,
                                @NotNull String idColumn, @NotNull DeleteOlderThanOp op) throws SQLException {
        StringBuilder where = new StringBuilder();
        for (String field : op.fields()) {
            if (where.length() > 0) where.append(" AND ");
            where.append(dialect.quoteIdentifier(field)).append(" < ?");
        }
        String sql = op.limit() == 0
            ? "DELETE FROM " + dialect.quoteIdentifier(table) + " WHERE " + where
            : dialect.deleteLimitedSql(table, idColumn, where.toString(), op.limit());
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < op.fields().size(); i++) ps.setLong(i + 1, op.cutoffEpochMs());
            return ps.executeUpdate();
        }
    }
}
