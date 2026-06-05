package ac.grim.grimac.internal.storage.backend.sql.v2.dialect;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.codec.MergeMode;
import ac.grim.grimac.api.storage.kind.IndexSpec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.UUID;

/**
 * MySQL 8-flavoured SQL fragment generator. Uses {@code BINARY(16)} for
 * UUIDs, backtick identifier quoting, and {@code ON DUPLICATE KEY UPDATE}
 * for upserts (MySQL's variant of ON CONFLICT). {@code CREATE INDEX}
 * uses try/catch at the call site because MySQL doesn't support
 * {@code IF NOT EXISTS} on index creation.
 */
@ApiStatus.Internal
public final class MysqlDialect implements SqlDialect {

    @Override public @NotNull String name() { return "mysql"; }

    @Override
    public @NotNull String createTableSql(@NotNull String tableName, @NotNull EncodeShape shape) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(q(tableName)).append(" (\n");
        boolean first = true;
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (!first) sb.append(",\n");
            sb.append("    ").append(q(f.name())).append(' ').append(columnTypeSql(f));
            if (f.name().equals(shape.idField())) sb.append(" PRIMARY KEY");
            first = false;
        }
        sb.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        return sb.toString();
    }

    @Override
    public @NotNull String createIndexSql(@NotNull String tableName, @NotNull IndexSpec spec) {
        StringBuilder sb = new StringBuilder("CREATE ");
        if (spec.unique()) sb.append("UNIQUE ");
        sb.append("INDEX ").append(q(spec.name()));
        sb.append(" ON ").append(q(tableName)).append(" (");
        boolean first = true;
        for (String f : spec.fields()) {
            if (!first) sb.append(", ");
            boolean desc = f.startsWith("-");
            String name = desc ? f.substring(1) : f;
            sb.append(q(name));
            if (desc) sb.append(" DESC");
            // MySQL doesn't support functional indexes like LOWER(col).
            // caseInsensitivePrefix indexes use a generated column at
            // the adapter level (Phase 5 follow-up).
            first = false;
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    public @NotNull String columnTypeSql(@NotNull EncodeShape.FieldDef field) {
        Class<?> t = field.javaType();
        String base;
        if (t == long.class || t == Long.class)               base = "BIGINT";
        else if (t == int.class || t == Integer.class)        base = "INT";
        else if (t == double.class || t == Double.class)      base = "DOUBLE";
        else if (t == float.class || t == Float.class)        base = "FLOAT";
        else if (t == boolean.class || t == Boolean.class)    base = "TINYINT(1)";
        else if (t == String.class)                           base = "VARCHAR(512)";
        else if (t == UUID.class)                             base = "BINARY(16)";
        else if (t == byte[].class)                           base = "BLOB";
        else if (t.isEnum())                                  base = "INT";
        else throw new IllegalArgumentException("MysqlDialect: unsupported type " + t.getName());
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
        return List.of("ALTER TABLE " + q(tableName)
            + " MODIFY COLUMN " + q(field.name()) + " " + columnTypeSql(field).replace(" NOT NULL", ""));
    }

    @Override
    public @NotNull String quoteIdentifier(@NotNull String name) { return q(name); }

    @Override
    public @NotNull String upsertSql(@NotNull String tableName, @NotNull EncodeShape shape) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(q(tableName)).append(" (");
        boolean first = true;
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (!first) sb.append(", ");
            sb.append(q(f.name()));
            first = false;
        }
        sb.append(") VALUES (");
        for (int i = 0; i < shape.fields().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append('?');
        }
        // MySQL 8.0.20+ uses alias syntax: AS new(col1, col2, ...)
        // followed by SET col = new.col in the UPDATE clause. For
        // simplicity we use VALUES(col) which is deprecated but still
        // supported in MySQL 8.x and all MariaDB versions.
        sb.append(") ON DUPLICATE KEY UPDATE ");
        first = true;
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (f.name().equals(shape.idField())) continue;
            if (f.mergeMode() == MergeMode.INSERT_ONLY) continue;
            if (!first) sb.append(", ");
            String col = q(f.name());
            @SuppressWarnings("deprecation")
            String incoming = "VALUES(" + col + ")";
            switch (f.mergeMode()) {
                case OVERWRITE -> sb.append(col).append(" = ").append(incoming);
                case PRESERVE_ON_NON_NULL -> sb.append(col)
                    .append(" = COALESCE(").append(col).append(", ").append(incoming).append(')');
                case PRESERVE_ON_NON_SENTINEL -> sb.append(col)
                    .append(" = CASE WHEN ").append(col).append(" IS NULL OR ").append(col)
                    .append(" = ").append(f.sentinelValue())
                    .append(" THEN ").append(incoming).append(" ELSE ").append(col).append(" END");
                case MAX -> sb.append(col).append(" = GREATEST(").append(col)
                    .append(", ").append(incoming).append(')');
                case MIN -> sb.append(col).append(" = LEAST(").append(col)
                    .append(", ").append(incoming).append(')');
                case INSERT_ONLY -> { /* unreachable */ }
            }
            first = false;
        }
        if (first) {
            int upd = sb.lastIndexOf(" ON DUPLICATE KEY UPDATE ");
            sb.setLength(upd);
            sb.append(" ON DUPLICATE KEY UPDATE ").append(q(shape.idField())).append(" = ")
              .append(q(shape.idField()));
        }
        return sb.toString();
    }

    @Override public boolean supportsReturning() { return false; }

    @Override public @NotNull String excludedRef(@NotNull String quotedCol) {
        return "VALUES(" + quotedCol + ")";
    }

    @Override public @NotNull String counterIncrementSql(@NotNull String quotedTable) {
        return "INSERT INTO " + quotedTable + " (`id`, `value`) VALUES (?, ?) "
            + "ON DUPLICATE KEY UPDATE `value` = `value` + VALUES(`value`)";
    }

    @Override public @NotNull String kvUpsertSql(@NotNull String quotedTable) {
        return "INSERT INTO " + quotedTable + " (`scope`, `scope_key`, `key`, `value`, `updated_at`) "
            + "VALUES (?, ?, ?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE `value` = VALUES(`value`), `updated_at` = VALUES(`updated_at`)";
    }

    private static @NotNull String q(@NotNull String name) {
        StringBuilder sb = new StringBuilder(name.length() + 2);
        sb.append('`');
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == '`') sb.append('`').append('`');
            else sb.append(c);
        }
        sb.append('`');
        return sb.toString();
    }
}
