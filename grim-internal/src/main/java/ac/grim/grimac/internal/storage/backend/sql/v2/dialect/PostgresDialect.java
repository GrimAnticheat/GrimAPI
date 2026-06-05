package ac.grim.grimac.internal.storage.backend.sql.v2.dialect;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.codec.MergeMode;
import ac.grim.grimac.api.storage.kind.IndexSpec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.UUID;

/**
 * Postgres-flavoured SQL fragment generator. Uses native {@code UUID}
 * type for UUID fields (so cursor seeks compare byte-for-byte without
 * encoding through {@code BYTEA}), {@code JSONB} for nested values
 * (Phase 6), and {@code ON CONFLICT (id) DO UPDATE} for atomic upserts.
 *
 * <p>{@code CREATE TABLE IF NOT EXISTS} + {@code CREATE INDEX IF NOT EXISTS}
 * are both natively supported on Postgres — no exotic shenanigans
 * required (unlike MySQL, where {@code IF NOT EXISTS} on indexes is a
 * MariaDB extension).
 */
@ApiStatus.Internal
public final class PostgresDialect implements SqlDialect {

    @Override public @NotNull String name() { return "postgres"; }

    @Override public @NotNull String kvValueColumnType() { return "BYTEA"; }

    @Override
    public @NotNull String createTableSql(@NotNull String tableName, @NotNull EncodeShape shape) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(quoteId(tableName)).append(" (\n");
        boolean first = true;
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (!first) sb.append(",\n");
            sb.append("    ").append(quoteId(f.name())).append(' ').append(columnTypeSql(f));
            if (f.name().equals(shape.idField())) {
                sb.append(" PRIMARY KEY");
            }
            first = false;
        }
        sb.append("\n)");
        return sb.toString();
    }

    @Override
    public @NotNull String createIndexSql(@NotNull String tableName, @NotNull IndexSpec spec) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (spec.unique()) sb.append("UNIQUE ");
        // Postgres index names are SCHEMA-global, not table-local like
        // Mongo. CREATE INDEX IF NOT EXISTS "by_name" on a second
        // table with the same spec name would silently no-op. Prefix
        // the name with the table to scope it; trim to Postgres'
        // 63-character NAMEDATALEN cap if the combined string overflows.
        String physical = qualifiedIndexName(tableName, spec.name());
        sb.append("INDEX IF NOT EXISTS ").append(quoteId(physical));
        sb.append(" ON ").append(quoteId(tableName)).append(" (");
        boolean first = true;
        for (int i = 0; i < spec.fields().size(); i++) {
            String f = spec.fields().get(i);
            if (!first) sb.append(", ");
            boolean desc = f.startsWith("-");
            String name = desc ? f.substring(1) : f;
            // For a caseInsensitivePrefix index, wrap the LEADING column
            // (the prefix-scan target) in LOWER() so this is a functional
            // index. SqlEntityAdapter.prefixIndex routes queries through
            // LOWER(col) LIKE LOWER(prefix) || '%' so the planner uses
            // this index. Trailing columns of a compound index keep
            // their natural form.
            if (i == 0 && spec.caseInsensitivePrefix()) {
                sb.append("LOWER(").append(quoteId(name)).append(')');
            } else {
                sb.append(quoteId(name));
            }
            if (desc) sb.append(" DESC");
            first = false;
        }
        sb.append(')');
        return sb.toString();
    }

    /**
     * Combine {@code tableName + "_" + indexName} and truncate to
     * Postgres' 63-character {@code NAMEDATALEN} default. Truncation
     * keeps the leading characters of the index name (so the prefix
     * is preserved); collisions after truncation are accepted as a
     * known limitation — operators with very long table names should
     * pick shorter index names.
     */
    static @NotNull String qualifiedIndexName(@NotNull String tableName, @NotNull String indexName) {
        String combined = tableName + "_" + indexName;
        return combined.length() <= 63 ? combined : combined.substring(0, 63);
    }

    @Override
    public @NotNull String columnTypeSql(@NotNull EncodeShape.FieldDef field) {
        Class<?> t = field.javaType();
        String base;
        if (t == long.class || t == Long.class)               base = "BIGINT";
        else if (t == int.class || t == Integer.class)        base = "INTEGER";
        else if (t == double.class || t == Double.class)      base = "DOUBLE PRECISION";
        else if (t == float.class || t == Float.class)        base = "REAL";
        else if (t == boolean.class || t == Boolean.class)    base = "BOOLEAN";
        else if (t == String.class)                           base = "TEXT";
        else if (t == UUID.class)                             base = "UUID";
        else if (t == byte[].class)                           base = "BYTEA";
        else if (t.isEnum())                                  base = "INTEGER";
        else throw new IllegalArgumentException(
            "PostgresDialect: unsupported field type " + t.getName() + " on " + field.name());
        // Nullability: primitive types are never null; reference types
        // respect the @Nullable annotation captured in the FieldDef.
        boolean primitive = t.isPrimitive();
        return primitive || !field.nullable() ? base + " NOT NULL" : base;
    }

    @Override
    public @NotNull List<String> convertColumnToBinarySql(@NotNull String tableName,
                                                          @NotNull EncodeShape.FieldDef field,
                                                          @NotNull String actualTypeName) {
        if (field.javaType() != byte[].class || isBinaryColumnType(actualTypeName)) {
            return List.of();
        }
        String column = quoteId(field.name());
        return List.of("ALTER TABLE " + quoteId(tableName)
            + " ALTER COLUMN " + column + " TYPE BYTEA USING convert_to(" + column + ", 'UTF8')");
    }

    @Override
    public @NotNull String quoteIdentifier(@NotNull String name) {
        return quoteId(name);
    }

    @Override
    public @NotNull String upsertSql(@NotNull String tableName, @NotNull EncodeShape shape) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteId(tableName)).append(" (");
        boolean first = true;
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (!first) sb.append(", ");
            sb.append(quoteId(f.name()));
            first = false;
        }
        sb.append(") VALUES (");
        for (int i = 0; i < shape.fields().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append('?');
        }
        sb.append(") ON CONFLICT (").append(quoteId(shape.idField())).append(") DO UPDATE SET ");
        first = true;
        String tableQuoted = quoteId(tableName);
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (f.name().equals(shape.idField())) continue; // id is immutable
            // INSERT_ONLY: omit from SET so the existing value is kept
            // on conflict. The id-column case below already handles the
            // first-insert case (EXCLUDED set during INSERT phase before
            // ON CONFLICT short-circuits to DO UPDATE).
            if (f.mergeMode() == MergeMode.INSERT_ONLY) continue;
            if (!first) sb.append(", ");
            String col = quoteId(f.name());
            String existing = tableQuoted + "." + col;   // pre-update value
            String incoming = "EXCLUDED." + col;          // VALUES row
            switch (f.mergeMode()) {
                case OVERWRITE -> sb.append(col).append(" = ").append(incoming);
                case PRESERVE_ON_NON_NULL -> sb.append(col)
                    .append(" = COALESCE(").append(existing).append(", ").append(incoming).append(')');
                case PRESERVE_ON_NON_SENTINEL -> {
                    // CASE WHEN existing IS NULL OR existing = <sentinel>
                    //   THEN incoming ELSE existing END
                    // The sentinel value is a compile-time long
                    // constant from @Sentinel; embedded inline so the
                    // statement parameter count stays stable per shape
                    // (the SqlEntityHandler binds shape.fields() count
                    // params, no extra slot for sentinel).
                    sb.append(col).append(" = CASE WHEN ").append(existing)
                      .append(" IS NULL OR ").append(existing).append(" = ").append(f.sentinelValue())
                      .append(" THEN ").append(incoming).append(" ELSE ").append(existing).append(" END");
                }
                case MAX -> sb.append(col)
                    .append(" = GREATEST(").append(existing).append(", ").append(incoming).append(')');
                case MIN -> sb.append(col)
                    .append(" = LEAST(").append(existing).append(", ").append(incoming).append(')');
                case INSERT_ONLY -> { /* unreachable — skipped above */ }
            }
            first = false;
        }
        // If every non-id column is INSERT_ONLY, the SET clause would
        // be empty. Use DO NOTHING in that degenerate case so the
        // statement stays valid.
        if (first) {
            // Trim trailing "DO UPDATE SET " and replace with DO NOTHING.
            int set = sb.lastIndexOf(" DO UPDATE SET ");
            sb.setLength(set);
            sb.append(" DO NOTHING");
        }
        return sb.toString();
    }

    /**
     * Postgres identifier quoting — wrap in double quotes, escape any
     * embedded double quote by doubling. Used for table and column
     * names so reserved words ({@code user}, {@code order}, etc.)
     * round-trip cleanly through DDL.
     */
    @Override public boolean supportsReturning() { return true; }

    static @NotNull String quoteId(@NotNull String name) {
        StringBuilder sb = new StringBuilder(name.length() + 2);
        sb.append('"');
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == '"') sb.append('"').append('"');
            else sb.append(c);
        }
        sb.append('"');
        return sb.toString();
    }
}
