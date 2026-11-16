package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.internal.storage.backend.IndexCoverage;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The indexes a SQL table already has, read once per {@code ensureStore}, so the
 * SQL adapters create a default index only when no existing index covers it, and
 * report one that cannot be created instead of ignoring the error.
 */
@ApiStatus.Internal
final class CoveringIndexes {

    private final @Nullable List<IndexCoverage.Existing> existing;
    private final @NotNull SqlDialect dialect;
    private final @NotNull String table;
    private final @NotNull Logger logger;
    private @Nullable Boolean hasRows;

    private CoveringIndexes(@Nullable List<IndexCoverage.Existing> existing, @NotNull SqlDialect dialect,
                            @NotNull String table, @NotNull Logger logger) {
        this.existing = existing;
        this.dialect = dialect;
        this.table = table;
        this.logger = logger;
    }

    /** Reads the table's indexes. If they cannot be read, every default index is created as before. */
    static @NotNull CoveringIndexes read(@NotNull Connection connection, @NotNull SqlDialect dialect,
                                         @NotNull String table, @NotNull Logger logger) {
        try {
            return new CoveringIndexes(dialect.existingIndexes(connection, table), dialect, table, logger);
        } catch (SQLException | RuntimeException e) {
            logger.log(Level.FINE, e, () -> "could not read the indexes of " + table
                + "; creating the default indexes without checking for covering ones");
            return new CoveringIndexes(null, dialect, table, logger);
        }
    }

    /** True when an existing index covers {@code fields}; logs which one, so the skipped default is visible. */
    boolean covered(@NotNull String defaultIndex, @NotNull List<String> fields, boolean unique) {
        if (existing == null) return false;
        IndexCoverage.Existing covering = null;
        for (IndexCoverage.Existing index : existing) {
            if (!IndexCoverage.covers(index, fields, unique)) continue;
            // The default itself (dialects may prefix its name with the table) needs no log line.
            if (isDefault(index.name(), defaultIndex)) return true;
            if (covering == null) covering = index;
        }
        if (covering == null) return false;
        String coveringName = covering.name();
        logger.info(() -> "Not creating index " + defaultIndex + " on " + table + " " + fields
            + ": existing index " + coveringName + " already covers it");
        return true;
    }

    /**
     * Creates the default index {@code defaultIndex} with {@code createSql} unless an existing
     * index covers it. An "already exists" error (another server won the race) is expected; any
     * other failure is logged as a warning, because reads that filter on {@code fields} then scan
     * the whole table. Never throws: a missing index must not stop the store from starting.
     */
    void ensure(@NotNull Statement statement, @NotNull String defaultIndex, @NotNull List<String> fields,
                boolean unique, @NotNull String createSql) {
        if (covered(defaultIndex, fields, unique)) return;
        create(statement, defaultIndex, fields, createSql);
    }

    /**
     * Like {@link #ensure} for an index no other index can cover (an expression index):
     * skipped only when the default itself exists.
     */
    void ensureUnchecked(@NotNull Statement statement, @NotNull String defaultIndex, @NotNull List<String> fields,
                         @NotNull String createSql) {
        if (existing != null) {
            for (IndexCoverage.Existing index : existing) {
                if (isDefault(index.name(), defaultIndex)) return;
            }
        }
        create(statement, defaultIndex, fields, createSql);
    }

    private void create(@NotNull Statement statement, @NotNull String defaultIndex, @NotNull List<String> fields,
                        @NotNull String createSql) {
        if (hasRows(statement)) {
            logger.info(() -> "[grim-datastore] creating index " + defaultIndex + " on " + table + " " + fields
                + "; on a large table this takes a while");
        }
        try {
            statement.executeUpdate(createSql);
        } catch (SQLException e) {
            if (dialect.isDuplicateIndex(e)) {
                logger.log(Level.FINE, () -> "index " + defaultIndex + " on " + table + " already exists");
                return;
            }
            logger.log(Level.WARNING, "[grim-datastore] could not create index " + defaultIndex + " on " + table
                + " " + fields + " (" + e.getMessage() + "); reads that filter on " + fields
                + " will scan the whole table. Statement: " + createSql, e);
        }
    }

    private boolean hasRows(@NotNull Statement statement) {
        if (hasRows == null) {
            try (ResultSet rs = statement.executeQuery(
                    "SELECT 1 FROM " + dialect.quoteIdentifier(table) + " LIMIT 1")) {
                hasRows = rs.next();
            } catch (SQLException e) {
                hasRows = false;
            }
        }
        return hasRows;
    }

    /**
     * Exactly the default: its own name (the event-stream defaults are already table-qualified) or the name the
     * dialect gives it. A suffix match would let an unrelated index such as {@code legacy_by_name} stand in for
     * {@code by_name}, and an expression index is never covered by anything else.
     */
    private boolean isDefault(@NotNull String name, @NotNull String defaultIndex) {
        return name.equalsIgnoreCase(defaultIndex) || name.equalsIgnoreCase(dialect.physicalIndexName(table, defaultIndex));
    }
}
