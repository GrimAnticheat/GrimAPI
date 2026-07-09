package ac.grim.grimac.internal.storage.backend.sql.v2.dialect;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.kind.IndexSpec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Locale;

/**
 * Per-database SQL fragment generator. Lets {@code SqlEntityAdapter}
 * (and future SQL adapters) emit DDL + DML that's portable across
 * Postgres / MySQL / SQLite without scattering vendor checks
 * throughout the adapter code.
 *
 * <p>Each method returns a complete SQL fragment ready to execute or
 * embed. Implementations handle vendor quirks like:
 * <ul>
 *   <li>Postgres uses native {@code UUID} type; MySQL/SQLite store
 *       16-byte {@code BINARY(16)} / {@code BLOB}.</li>
 *   <li>{@code ON CONFLICT (id) DO UPDATE} (Postgres/SQLite) vs.
 *       {@code ON DUPLICATE KEY UPDATE} (MySQL).</li>
 *   <li>{@code CREATE INDEX IF NOT EXISTS} is a MariaDB extension —
 *       real MySQL needs an {@code information_schema} probe or
 *       try/catch on duplicate-key error 1061. (Tracked as #2667.)</li>
 * </ul>
 *
 * <p>Implementations are stateless and thread-safe; one instance per
 * vendor, reused across all tables / adapters.
 */
@ApiStatus.Internal
public interface SqlDialect {

    /** Vendor identifier: {@code "postgres"}, {@code "mysql"}, {@code "sqlite"}. */
    @NotNull String name();

    /**
     * Generate a {@code CREATE TABLE IF NOT EXISTS} statement for an
     * Entity store, deriving columns from the shape's persistent
     * field definitions. The primary key is the shape's {@code idField}.
     */
    @NotNull String createTableSql(@NotNull String tableName, @NotNull EncodeShape shape);

    /**
     * Generate a {@code CREATE INDEX} statement for one secondary
     * index. Implementations handle vendor differences in
     * {@code IF NOT EXISTS} support (real MySQL doesn't allow it on
     * {@code CREATE INDEX}; SQLite and Postgres do).
     */
    @NotNull String createIndexSql(@NotNull String tableName, @NotNull IndexSpec spec);

    /**
     * Map a {@link ac.grim.grimac.api.storage.codec.FieldKind} +
     * Java type to the vendor's column type string (e.g.
     * {@code "BIGINT NOT NULL"}, {@code "TEXT"}, {@code "UUID"}).
     * Used by {@link #createTableSql}.
     */
    @NotNull String columnTypeSql(@NotNull EncodeShape.FieldDef field);

    /**
     * True when a JDBC metadata type name can safely round-trip
     * {@code PreparedStatement#setBytes} / {@code ResultSet#getBytes}
     * without charset conversion.
     */
    default boolean isBinaryColumnType(@NotNull String typeName) {
        String t = typeName.toLowerCase(Locale.ROOT);
        return t.contains("blob")
            || t.contains("bytea")
            || t.contains("binary")
            || t.contains("varbinary");
    }

    /**
     * Vendor-specific DDL for converting an existing column to the
     * current binary type. Dialects that do not need or cannot safely
     * perform the conversion return an empty list.
     */
    default @NotNull List<String> convertColumnToBinarySql(@NotNull String tableName,
                                                           @NotNull EncodeShape.FieldDef field,
                                                           @NotNull String actualTypeName) {
        return List.of();
    }

    /**
     * True when this dialect opts into rebuilding an EventStream table
     * whose on-disk id column cannot store the shape's UUID id. SQLite
     * needs this: pre-UUIDv7 schemas declared {@code id INTEGER PRIMARY
     * KEY} — the strict rowid alias — so every 16-byte UUID insert fails
     * with SQLITE_MISMATCH, and {@code ALTER TABLE} can't change a
     * column's type, so the table must be rebuilt row-by-row.
     */
    default boolean rebuildsUuidIdColumns() { return false; }

    /**
     * Vendor-correct identifier quoting (table/column names). Postgres
     * uses {@code "double quotes"}; MySQL uses {@code `backticks`};
     * SQLite accepts both but prefers double-quotes. Embedded delimiter
     * characters are doubled per SQL convention.
     */
    @NotNull String quoteIdentifier(@NotNull String name);

    /**
     * Generate an atomic upsert statement template for an Entity store —
     * {@code INSERT INTO ... VALUES (?, ?, ...) ON CONFLICT (id) DO UPDATE
     * SET col2 = EXCLUDED.col2, ...} on Postgres/SQLite, or
     * {@code INSERT ... ON DUPLICATE KEY UPDATE ...} on MySQL.
     * <p>
     * Parameter ordering matches {@link EncodeShape#fields()} so callers
     * can iterate the shape's field list and bind values positionally
     * via {@code PreparedStatement.setXxx(i+1, value)}.
     * <p>
     * The id column is included in the VALUES list (it must be set on
     * insert) but excluded from the UPDATE SET clause (immutable across
     * upserts — Postgres rejects {@code SET id = ...} for the conflict
     * target column).
     */
    @NotNull String upsertSql(@NotNull String tableName, @NotNull EncodeShape shape);

    /**
     * True when SQLite must avoid modern UPSERT / RETURNING syntax.
     * SQLite engines before 3.24 reject {@code ON CONFLICT DO UPDATE},
     * which is still common on old Bukkit-family servers.
     */
    default boolean usesLegacySqliteUpsert() { return false; }

    /**
     * Whether this dialect supports {@code RETURNING <columns>} on
     * INSERT/UPDATE. Postgres and SQLite 3.35+ do; MySQL does not.
     * When false, adapters that need post-write values (e.g. counter
     * IncrementByOp) must use a transaction with a follow-up SELECT.
     */
    default boolean supportsReturning() { return false; }

    /**
     * The incoming-row reference in an ON CONFLICT / ON DUPLICATE KEY
     * UPDATE clause. Postgres/SQLite use {@code EXCLUDED.<col>}; MySQL
     * uses {@code VALUES(<col>)} (deprecated in 8.0.20+ but universally
     * supported). Returns just the prefix; callers append the column.
     * <p>
     * Examples: Postgres → {@code "EXCLUDED."}, MySQL → {@code "VALUES("}.
     * For MySQL the caller must also append {@code ")"} after the column.
     */
    default @NotNull String excludedRef(@NotNull String quotedCol) {
        return "EXCLUDED." + quotedCol;
    }

    /**
     * Vendor-correct {@code GREATEST(a, b)} function. MySQL and Postgres
     * both support {@code GREATEST}; SQLite uses {@code MAX}.
     */
    default @NotNull String greatestFn(@NotNull String a, @NotNull String b) {
        return "GREATEST(" + a + ", " + b + ")";
    }

    /**
     * Generate an atomic counter-increment upsert. Default uses
     * Postgres/SQLite ON CONFLICT syntax. MySQL overrides with
     * ON DUPLICATE KEY UPDATE.
     */
    default @NotNull String counterIncrementSql(@NotNull String quotedTable) {
        return "INSERT INTO " + quotedTable + " (id, value) VALUES (?, ?) "
            + "ON CONFLICT (id) DO UPDATE SET value = " + quotedTable + ".value + " + excludedRef(quoteIdentifier("value"));
    }

    /**
     * Binary column type for the KV {@code value} column. Default
     * {@code BLOB} (MySQL / SQLite); Postgres overrides to {@code BYTEA}.
     * Stored opaquely so KV values round-trip arbitrary bytes (e.g.
     * encoded {@link ac.grim.grimac.api.storage.model.SettingRecord#value}
     * payloads). Read/write uses {@code ps.setBytes} / {@code rs.getBytes}
     * which JDBC drivers map uniformly across BLOB / BYTEA / VARBINARY.
     */
    default @NotNull String kvValueColumnType() { return "BLOB"; }

    /**
     * Generate a KV upsert statement. Default uses Postgres/SQLite
     * ON CONFLICT syntax. MySQL overrides with ON DUPLICATE KEY UPDATE.
     */
    default @NotNull String kvUpsertSql(@NotNull String quotedTable) {
        String scope = quoteIdentifier("scope");
        String scopeKey = quoteIdentifier("scope_key");
        String key = quoteIdentifier("key");
        String value = quoteIdentifier("value");
        String updatedAt = quoteIdentifier("updated_at");
        return "INSERT INTO " + quotedTable + " (" + scope + ", " + scopeKey + ", " + key
            + ", " + value + ", " + updatedAt + ") "
            + "VALUES (?, ?, ?, ?, ?) "
            + "ON CONFLICT (" + scope + ", " + scopeKey + ", " + key + ") DO UPDATE SET "
            + value + " = " + excludedRef(value)
            + ", " + updatedAt + " = " + excludedRef(updatedAt);
    }
}
