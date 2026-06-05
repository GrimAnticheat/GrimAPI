package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;

/**
 * Shared per-type {@link PreparedStatement} bindings and
 * {@link ResultSet} extraction helpers for the v2 SQL adapters. Keeps
 * the per-vendor type-mapping logic adjacent to the dialect's column
 * type strings so any future column type added to a dialect lands
 * here too.
 */
@ApiStatus.Internal
public final class SqlBindings {

    private SqlBindings() {}

    /**
     * Bind a single record field's value to a prepared statement
     * parameter. The {@code value} is the boxed result of
     * {@code BsonCodec.readField}; primitives arrive auto-boxed.
     * <p>
     * Null handling: nullable reference fields with {@code null}
     * value bind {@link Types#NULL} on a best-effort SQL type
     * derived from {@link EncodeShape.FieldDef#javaType()}. Primitive
     * fields can't be null and {@code value} is always non-null on
     * the codec read path.
     */
    public static void bind(@NotNull PreparedStatement ps,
                            int parameterIndex,
                            @NotNull EncodeShape.FieldDef field,
                            @Nullable Object value) throws SQLException {
        Class<?> t = field.javaType();
        if (value == null) {
            if (t.isPrimitive()) {
                // Should not happen — primitive fields can't be null.
                // Hard-fail with a clear error rather than binding 0 and
                // silently corrupting the column.
                throw new SQLException("null value for primitive field " + field.name());
            }
            ps.setNull(parameterIndex, sqlTypeFor(t));
            return;
        }
        if (t == long.class || t == Long.class) {
            ps.setLong(parameterIndex, ((Number) value).longValue());
        } else if (t == int.class || t == Integer.class) {
            ps.setInt(parameterIndex, ((Number) value).intValue());
        } else if (t == double.class || t == Double.class) {
            ps.setDouble(parameterIndex, ((Number) value).doubleValue());
        } else if (t == float.class || t == Float.class) {
            // codec.readField returns Double for TypeTag.FLOAT (it routes
            // through ToDoubleFunction); coerce via Number rather than
            // casting to Float to avoid ClassCastException.
            ps.setFloat(parameterIndex, ((Number) value).floatValue());
        } else if (t == boolean.class || t == Boolean.class) {
            ps.setBoolean(parameterIndex, (Boolean) value);
        } else if (t == String.class) {
            ps.setString(parameterIndex, (String) value);
        } else if (t == UUID.class) {
            UUID uuid = (UUID) value;
            // Detect native UUID support by checking if the connection
            // is Postgres (the only JDBC driver with native UUID type).
            // All others (MySQL, SQLite, H2) store UUIDs as BINARY(16).
            String driverName = "";
            try { driverName = ps.getConnection().getMetaData().getDriverName(); }
            catch (SQLException ignored) {}
            if (driverName.toLowerCase(java.util.Locale.ROOT).contains("postgresql")) {
                ps.setObject(parameterIndex, uuid);
            } else {
                java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(16);
                buf.putLong(uuid.getMostSignificantBits());
                buf.putLong(uuid.getLeastSignificantBits());
                ps.setBytes(parameterIndex, buf.array());
            }
        } else if (t == byte[].class) {
            ps.setBytes(parameterIndex, (byte[]) value);
        } else if (t.isEnum()) {
            ps.setInt(parameterIndex, ((Enum<?>) value).ordinal());
        } else {
            throw new SQLException("SqlBindings: unsupported field type " + t.getName()
                + " for field " + field.name());
        }
    }

    /**
     * Extract a single field's value from a {@link ResultSet} by
     * column name. Result is boxed (primitives auto-boxed) so it can
     * feed directly into the codec's argument array on decode.
     * Returns {@code null} when the column is SQL NULL.
     */
    public static @Nullable Object extract(@NotNull ResultSet rs,
                                           @NotNull EncodeShape.FieldDef field) throws SQLException {
        Class<?> t = field.javaType();
        if (t == long.class || t == Long.class) {
            long v = rs.getLong(field.name());
            return rs.wasNull() ? null : v;
        }
        if (t == int.class || t == Integer.class) {
            int v = rs.getInt(field.name());
            return rs.wasNull() ? null : v;
        }
        if (t == double.class || t == Double.class) {
            double v = rs.getDouble(field.name());
            return rs.wasNull() ? null : v;
        }
        if (t == float.class || t == Float.class) {
            // Round-trip a float-typed column back to Double so the
            // codec's ToDoubleFunction accessor for TypeTag.FLOAT can
            // accept it (the codec stores boxed Double in args[] and
            // unboxes via doubleValue() on construct).
            float v = rs.getFloat(field.name());
            return rs.wasNull() ? null : (double) v;
        }
        if (t == boolean.class || t == Boolean.class) {
            boolean v = rs.getBoolean(field.name());
            return rs.wasNull() ? null : v;
        }
        if (t == String.class) {
            return rs.getString(field.name());
        }
        if (t == UUID.class) {
            Object v = rs.getObject(field.name());
            if (v == null) return null;
            if (v instanceof UUID u) return u;
            if (v instanceof byte[] b && b.length == 16) {
                // MySQL/SQLite path once their dialects land.
                java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(b);
                return new UUID(buf.getLong(), buf.getLong());
            }
            if (v instanceof String s) {
                return UUID.fromString(s);
            }
            throw new SQLException("can't decode UUID from " + v.getClass().getName()
                + " on field " + field.name());
        }
        if (t == byte[].class) {
            return rs.getBytes(field.name());
        }
        if (t.isEnum()) {
            int ordinal = rs.getInt(field.name());
            if (rs.wasNull()) return null;
            @SuppressWarnings({"rawtypes", "unchecked"})
            Object[] constants = ((Class<Enum>) t).getEnumConstants();
            if (ordinal < 0 || ordinal >= constants.length) {
                throw new SQLException("enum ordinal " + ordinal + " out of range for "
                    + t.getName() + " on field " + field.name());
            }
            return constants[ordinal];
        }
        throw new SQLException("SqlBindings: unsupported field type " + t.getName()
            + " for field " + field.name());
    }

    private static int sqlTypeFor(@NotNull Class<?> t) {
        if (t == Long.class)    return Types.BIGINT;
        if (t == Integer.class) return Types.INTEGER;
        if (t == Double.class)  return Types.DOUBLE;
        if (t == Float.class)   return Types.REAL;
        if (t == Boolean.class) return Types.BOOLEAN;
        if (t == String.class)  return Types.VARCHAR;
        if (t == UUID.class)    return Types.OTHER; // Postgres native UUID
        if (t == byte[].class)  return Types.BINARY;
        if (t.isEnum())         return Types.INTEGER;
        return Types.OTHER;
    }

    /**
     * Serialize an id-field value to a canonical byte sequence for the
     * portable {@link ac.grim.grimac.api.storage.query.Cursors} payload.
     * UUIDs become 16 bytes big-endian (MSB || LSB) — identical to the
     * BSON UUID_STANDARD encoding so cursors round-trip across Mongo and
     * SQL. Longs/ints become big-endian fixed-width. Strings use UTF-8.
     * The byte form is used as the cursor's id payload and decoded back
     * via {@link #bytesToId} before re-binding to a query.
     */
    public static byte @NotNull [] idToBytes(@NotNull EncodeShape.FieldDef field, @NotNull Object value) {
        Class<?> t = field.javaType();
        if (t == UUID.class) {
            UUID u = (UUID) value;
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(16);
            buf.putLong(u.getMostSignificantBits());
            buf.putLong(u.getLeastSignificantBits());
            return buf.array();
        }
        if (t == byte[].class) return (byte[]) value;
        if (t == String.class) return ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (t == Long.class || t == long.class) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(8);
            buf.putLong(((Number) value).longValue());
            return buf.array();
        }
        if (t == Integer.class || t == int.class) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(4);
            buf.putInt(((Number) value).intValue());
            return buf.array();
        }
        throw new IllegalArgumentException(
            "SqlBindings.idToBytes: unsupported id type " + t.getName() + " for field " + field.name());
    }

    /**
     * Inverse of {@link #idToBytes}. Reconstructs the id-field's Java
     * value from a canonical byte payload. Throws on malformed input
     * (e.g. a 4-byte payload for a UUID id) so a corrupted cursor fails
     * fast at the boundary instead of producing a silently-wrong query.
     */
    public static @NotNull Object bytesToId(@NotNull EncodeShape.FieldDef field, byte @NotNull [] bytes) {
        Class<?> t = field.javaType();
        if (t == UUID.class) {
            if (bytes.length != 16) {
                throw new IllegalArgumentException(
                    "UUID id payload must be 16 bytes, got " + bytes.length);
            }
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);
            return new UUID(buf.getLong(), buf.getLong());
        }
        if (t == byte[].class) return bytes;
        if (t == String.class) return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        if (t == Long.class || t == long.class) {
            if (bytes.length != 8) {
                throw new IllegalArgumentException(
                    "Long id payload must be 8 bytes, got " + bytes.length);
            }
            return java.nio.ByteBuffer.wrap(bytes).getLong();
        }
        if (t == Integer.class || t == int.class) {
            if (bytes.length != 4) {
                throw new IllegalArgumentException(
                    "Integer id payload must be 4 bytes, got " + bytes.length);
            }
            return java.nio.ByteBuffer.wrap(bytes).getInt();
        }
        throw new IllegalArgumentException(
            "SqlBindings.bytesToId: unsupported id type " + t.getName() + " for field " + field.name());
    }
}
