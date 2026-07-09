package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodec;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodecs;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EventStreamOps;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Cursors;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import ac.grim.grimac.internal.storage.util.UuidCodec;
import ac.grim.grimac.internal.storage.util.UuidV7;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * SQL adapter for {@link EventStream} stores (e.g. violations). Uses a
 * regular table (not timeseries — SQL doesn't have native timeseries)
 * with per-field columns matching the record's {@link EncodeShape}.
 * Retention is handled via explicit {@code DELETE WHERE timestamp < ?}
 * (no TTL monitor).
 *
 * <p>Table columns are derived from the record's EncodeShape fields at
 * ensureStore time, mirroring SqlEntityAdapter's DDL pattern. Partition
 * fields become indexed columns for efficient filtered reads.
 */
@ApiStatus.Internal
public final class SqlEventStreamAdapter implements KindAdapter<EventStream<?, ?>> {

    private static final int SQL_BATCH_SIZE = 256;

    private final @NotNull DataSource ds;
    private final @NotNull SqlDialect dialect;
    private final @NotNull Logger logger;

    public SqlEventStreamAdapter(@NotNull DataSource ds, @NotNull SqlDialect dialect,
                                 @NotNull Logger logger) {
        this.ds = ds;
        this.dialect = dialect;
        this.logger = logger;
    }

    @SuppressWarnings("unchecked")
    @Override public @NotNull Class<EventStream<?, ?>> kindType() {
        return (Class<EventStream<?, ?>>) (Class<?>) EventStream.class;
    }

    @Override public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_EVENT_STREAM);
    }

    @Override
    public void ensureStore(@NotNull StoreId id, @NotNull EventStream<?, ?> kind) throws BackendException {
        EncodeShape shape = kind.codec().shape();
        String table = dialect.quoteIdentifier(id.name());
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate(createTableDdl(id.name(), shape));
            // Add missing columns from legacy tables (same pattern as SqlEntityAdapter)
            addMissingColumns(c, s, id.name(), shape);
            maybeRebuildForUuidId(c, id.name(), kind, shape);
            for (String partition : kind.partitionFields()) {
                String idxName = "idx_" + id.name() + "_" + partition;
                String createIdx = "CREATE INDEX IF NOT EXISTS " + dialect.quoteIdentifier(idxName)
                    + " ON " + table + " (" + dialect.quoteIdentifier(partition) + ")";
                try { s.executeUpdate(createIdx); } catch (SQLException ignore) {}
            }
            String tsIdx = "idx_" + id.name() + "_ts";
            String createTsIdx = "CREATE INDEX IF NOT EXISTS " + dialect.quoteIdentifier(tsIdx)
                + " ON " + table + " (" + dialect.quoteIdentifier(kind.timestampField()) + ")";
            try { s.executeUpdate(createTsIdx); } catch (SQLException ignore) {}
        } catch (SQLException e) {
            throw new BackendException("failed to ensure event stream table " + id, e);
        }
    }

    @Override
    public void dropStore(@NotNull StoreId id, @NotNull EventStream<?, ?> kind) throws BackendException {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("DROP TABLE IF EXISTS " + dialect.quoteIdentifier(id.name()));
        } catch (SQLException e) {
            throw new BackendException("failed to drop event stream table " + id, e);
        }
    }

    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id, @NotNull EventStream<?, ?> kind, @NotNull Category<E> category) {
        EncodeShape shape = kind.codec().shape();
        String table = dialect.quoteIdentifier(id.name());
        StringBuilder cols = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < shape.fields().size(); i++) {
            if (i > 0) { cols.append(", "); placeholders.append(", "); }
            cols.append(dialect.quoteIdentifier(shape.fields().get(i).name()));
            placeholders.append('?');
        }
        String insertSql = "INSERT INTO " + table + " (" + cols + ") VALUES (" + placeholders + ")";
        return new EventStreamWriteHandler<>(insertSql, kind, shape);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull EventStream<?, ?> kind,
                         @NotNull Operation<R> op) throws BackendException {
        try {
            if (op instanceof EventStreamOps.PageOp<?> p)           return (R) page(id, kind, p);
            if (op instanceof EventStreamOps.RangeByTimeOp<?> r)    return (R) rangeByTime(id, kind, r);
            if (op instanceof EventStreamOps.CountOp c)             return (R) Long.valueOf(count(id, kind, c));
            if (op instanceof EventStreamOps.CountManyOp<?> c)      return (R) countMany(id, kind, c);
            if (op instanceof EventStreamOps.CountDistinctOp c)     return (R) Long.valueOf(countDistinct(id, kind, c));
            if (op instanceof EventStreamOps.DeleteByPartitionOp d) { deleteByPartition(id, kind, d); return null; }
            if (op instanceof EventStreamOps.DeleteOlderThanOp d)   { deleteOlderThan(id, kind, d); return null; }
            throw new UnsupportedOperationException(
                "SqlEventStreamAdapter does not handle " + op.getClass().getName());
        } catch (SQLException e) {
            throw new BackendException("sql event stream execute failed for " + op.getClass().getSimpleName(), e);
        }
    }

    @Override
    public @NotNull List<Migration<EventStream<?, ?>>> migrations(@NotNull EventStream<?, ?> kind) {
        return List.of();
    }

    // ============================== writeHandler ==============================

    @SuppressWarnings({"unchecked", "rawtypes"})
    private final class EventStreamWriteHandler<E> implements StorageEventHandler<E> {
        private final String insertSql;
        private final EventStream kind;
        private final EncodeShape shape;
        private final BsonCodec codec;
        private final List<Object> pendingRecords = new ArrayList<>(SQL_BATCH_SIZE);

        EventStreamWriteHandler(String insertSql, EventStream<?, ?> kind, EncodeShape shape) {
            this.insertSql = insertSql;
            this.kind = kind;
            this.shape = shape;
            this.codec = BsonCodecs.regular(kind.recordType());
        }

        @Override
        public void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            Object record;
            try {
                record = kind.eventToRecord().apply(event);
            } catch (RuntimeException e) {
                throw new BackendException("event stream write failed", e);
            }
            try {
                pendingRecords.add(record);
                if (endOfBatch || pendingRecords.size() >= SQL_BATCH_SIZE) flushBatch();
            } catch (Exception e) {
                throw new BackendException("event stream write failed", e);
            }
        }

        private void flushBatch() throws SQLException {
            if (pendingRecords.isEmpty()) return;
            try (Connection c = ds.getConnection()) {
                boolean priorAutoCommit = c.getAutoCommit();
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement(insertSql)) {
                    for (Object record : pendingRecords) {
                        bindRecord(ps, record);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                    c.commit();
                } catch (SQLException | RuntimeException e) {
                    try { c.rollback(); } catch (SQLException ignored) {}
                    throw e;
                } finally {
                    c.setAutoCommit(priorAutoCommit);
                }
            } finally {
                pendingRecords.clear();
            }
        }

        private void bindRecord(@NotNull PreparedStatement ps, @NotNull Object record) throws SQLException {
            int idx = 1;
            for (int i = 0; i < shape.fields().size(); i++) {
                SqlBindings.bind(ps, idx++, shape.fields().get(i), codec.readField(record, i));
            }
        }
    }

    // ============================== execute dispatch ==============================

    @SuppressWarnings("unchecked")
    private <R> Page<R> page(@NotNull StoreId id, @NotNull EventStream<?, ?> kind,
                             @NotNull EventStreamOps.PageOp<R> op) throws SQLException {
        EncodeShape shape = kind.codec().shape();
        String table = dialect.quoteIdentifier(id.name());
        String tsCol = dialect.quoteIdentifier(kind.timestampField());
        String idCol = dialect.quoteIdentifier(shape.idField());
        String partCol = dialect.quoteIdentifier(op.partition());
        EncodeShape.FieldDef partField = fieldDef(shape, op.partition());
        EncodeShape.FieldDef idField = fieldDef(shape, shape.idField());

        boolean hasCursor = op.cursor() != null;
        String sql;
        if (hasCursor) {
            // Keyset pagination: seek past the (timestamp, id) of the
            // last row in the previous page.
            sql = "SELECT * FROM " + table
                + " WHERE " + partCol + " = ?"
                + " AND (" + tsCol + " > ? OR (" + tsCol + " = ? AND " + idCol + " > ?))"
                + " ORDER BY " + tsCol + " ASC, " + idCol + " ASC"
                + " LIMIT ?";
        } else {
            sql = "SELECT * FROM " + table
                + " WHERE " + partCol + " = ?"
                + " ORDER BY " + tsCol + " ASC, " + idCol + " ASC"
                + " LIMIT ?";
        }
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            int idx = 1;
            SqlBindings.bind(ps, idx++, partField, op.key());
            if (hasCursor) {
                Cursors.Decoded d = Cursors.decode(op.cursor());
                Object idValue = SqlBindings.bytesToId(idField, d.idBytes());
                ps.setLong(idx++, d.orderedKey()); // ts >
                ps.setLong(idx++, d.orderedKey()); // ts =
                SqlBindings.bind(ps, idx++, idField, idValue); // id >
            }
            ps.setInt(idx, op.pageSize() + 1);
            return readPageWithCursor(ps, kind, op.pageSize());
        }
    }

    @SuppressWarnings("unchecked")
    private <R> Page<R> rangeByTime(@NotNull StoreId id, @NotNull EventStream<?, ?> kind,
                                    @NotNull EventStreamOps.RangeByTimeOp<R> op) throws SQLException {
        EncodeShape shape = kind.codec().shape();
        String table = dialect.quoteIdentifier(id.name());
        String tsCol = dialect.quoteIdentifier(kind.timestampField());
        String idCol = dialect.quoteIdentifier(shape.idField());
        EncodeShape.FieldDef idField = fieldDef(shape, shape.idField());

        boolean hasCursor = op.cursor() != null;
        // Exclusive upper bound (< toEpochMs) to match Mongo semantics.
        // When a cursor is present, also seek past the (timestamp, id) of
        // the previous page's last row.
        String sql = hasCursor
            ? "SELECT * FROM " + table
                + " WHERE " + tsCol + " >= ? AND " + tsCol + " < ?"
                + " AND (" + tsCol + " > ? OR (" + tsCol + " = ? AND " + idCol + " > ?))"
                + " ORDER BY " + tsCol + " ASC, " + idCol + " ASC"
                + " LIMIT ?"
            : "SELECT * FROM " + table
                + " WHERE " + tsCol + " >= ? AND " + tsCol + " < ?"
                + " ORDER BY " + tsCol + " ASC, " + idCol + " ASC"
                + " LIMIT ?";
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            int idx = 1;
            ps.setLong(idx++, op.fromEpochMs());
            ps.setLong(idx++, op.toEpochMs());
            if (hasCursor) {
                Cursors.Decoded d = Cursors.decode(op.cursor());
                Object idValue = SqlBindings.bytesToId(idField, d.idBytes());
                ps.setLong(idx++, d.orderedKey()); // ts >
                ps.setLong(idx++, d.orderedKey()); // ts =
                SqlBindings.bind(ps, idx++, idField, idValue); // id >
            }
            ps.setInt(idx, op.pageSize() + 1);
            return readPageWithCursor(ps, kind, op.pageSize());
        }
    }

    private long count(@NotNull StoreId id, @NotNull EventStream<?, ?> kind,
                       @NotNull EventStreamOps.CountOp op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        String sql = "SELECT COUNT(*) FROM " + table
            + " WHERE " + dialect.quoteIdentifier(op.partition()) + " = ?";
        EncodeShape shape = kind.codec().shape();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            SqlBindings.bind(ps, 1, fieldDef(shape, op.partition()), op.key());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <K> Map<K, Long> countMany(@NotNull StoreId id, @NotNull EventStream<?, ?> kind,
                                       @NotNull EventStreamOps.CountManyOp<K> op) throws SQLException {
        Collection<K> keys = op.keys();
        if (keys.isEmpty()) return Map.of();
        EncodeShape shape = kind.codec().shape();
        String table = dialect.quoteIdentifier(id.name());
        String partCol = dialect.quoteIdentifier(op.partition());
        String placeholders = String.join(", ", keys.stream().map(k -> "?").toList());
        String sql = "SELECT " + partCol + ", COUNT(*) AS cnt FROM " + table
            + " WHERE " + partCol + " IN (" + placeholders + ")"
            + " GROUP BY " + partCol;
        Map<K, Long> out = new LinkedHashMap<>(keys.size() * 4 / 3 + 1);
        for (K k : keys) out.put(k, 0L);
        EncodeShape.FieldDef partField = fieldDef(shape, op.partition());
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            int idx = 1;
            for (K k : keys) SqlBindings.bind(ps, idx++, partField, k);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    @SuppressWarnings("unchecked")
                    K key = (K) SqlBindings.extract(rs, partField);
                    if (key != null) out.put(key, rs.getLong("cnt"));
                }
            }
        }
        return out;
    }

    private long countDistinct(@NotNull StoreId id, @NotNull EventStream<?, ?> kind,
                               @NotNull EventStreamOps.CountDistinctOp op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        String sql = "SELECT COUNT(DISTINCT " + dialect.quoteIdentifier(op.field()) + ") FROM " + table
            + " WHERE " + dialect.quoteIdentifier(op.partition()) + " = ?";
        EncodeShape shape = kind.codec().shape();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            SqlBindings.bind(ps, 1, fieldDef(shape, op.partition()), op.key());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    private void deleteByPartition(@NotNull StoreId id, @NotNull EventStream<?, ?> kind,
                                   @NotNull EventStreamOps.DeleteByPartitionOp op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        EncodeShape shape = kind.codec().shape();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "DELETE FROM " + table + " WHERE " + dialect.quoteIdentifier(op.partition()) + " = ?")) {
            SqlBindings.bind(ps, 1, fieldDef(shape, op.partition()), op.key());
            ps.executeUpdate();
        }
    }

    private void deleteOlderThan(@NotNull StoreId id, @NotNull EventStream<?, ?> kind,
                                 @NotNull EventStreamOps.DeleteOlderThanOp op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        String tsField = dialect.quoteIdentifier(kind.timestampField());
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "DELETE FROM " + table + " WHERE " + tsField + " < ?")) {
            ps.setLong(1, op.cutoffEpochMs());
            ps.executeUpdate();
        }
    }

    // ============================== helpers ==============================

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <R> Page<R> readPageWithCursor(@NotNull PreparedStatement ps,
                                           @NotNull EventStream<?, ?> kind,
                                           int pageSize) throws SQLException {
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        EncodeShape shape = kind.codec().shape();
        String tsFieldName = kind.timestampField();
        String idFieldName = shape.idField();
        EncodeShape.FieldDef idField = fieldDef(shape, idFieldName);
        List<R> items = new ArrayList<>(pageSize);
        Cursor nextCursor = null;
        long lastTs = 0;
        Object lastIdValue = null;
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                if (items.size() >= pageSize) {
                    // Overflow row: encode (lastTs, lastIdBytes) of the
                    // final page row as the next-page cursor. Portable
                    // shape — Mongo/SQL/Redis share decode via
                    // Cursors.decode.
                    nextCursor = lastIdValue == null
                        ? null
                        : Cursors.encode(lastTs, SqlBindings.idToBytes(idField, lastIdValue));
                    break;
                }
                Object[] args = new Object[shape.fields().size()];
                for (int i = 0; i < shape.fields().size(); i++) {
                    EncodeShape.FieldDef f = shape.fields().get(i);
                    args[i] = SqlBindings.extract(rs, f);
                    if (f.name().equals(tsFieldName) && args[i] instanceof Number n) {
                        lastTs = n.longValue();
                    }
                    if (f.name().equals(idFieldName) && args[i] != null) {
                        lastIdValue = args[i];
                    }
                }
                items.add((R) codec.decodeFromValues(args));
            }
        }
        return new Page<>(items, nextCursor);
    }

    private @NotNull String createTableDdl(@NotNull String tableName, @NotNull EncodeShape shape) {
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
            .append(dialect.quoteIdentifier(tableName)).append(" (");
        boolean first = true;
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (!first) ddl.append(", ");
            first = false;
            ddl.append(dialect.quoteIdentifier(f.name())).append(' ').append(dialect.columnTypeSql(f));
            if (f.name().equals(shape.idField())) ddl.append(" PRIMARY KEY");
        }
        return ddl.append(')').toString();
    }

    /**
     * Detect an EventStream table whose id column predates the UUID id
     * scheme and rebuild it in place. Shipped pre-UUIDv7 schemas declared
     * the violation id as {@code INTEGER PRIMARY KEY AUTOINCREMENT}; on
     * SQLite that column is the strict rowid alias, so once the record id
     * became a UUIDv7 every insert fails with SQLITE_MISMATCH
     * (GrimAnticheat/Grim#2768). The versioned SqliteSchema migration that
     * handled this shape (v3 → v4) only runs on the legacy phase-1 backend,
     * which the v2-only cutover no longer starts — so the v2 path must
     * detect and repair the legacy shape itself.
     */
    private void maybeRebuildForUuidId(@NotNull Connection c, @NotNull String tableName,
                                       @NotNull EventStream<?, ?> kind,
                                       @NotNull EncodeShape shape) throws SQLException {
        EncodeShape.FieldDef idField = fieldDef(shape, shape.idField());
        if (idField.javaType() != UUID.class) return;
        if (!dialect.rebuildsUuidIdColumns()) return;
        String actualType = columnType(c, tableName, idField.name());
        if (actualType == null || dialect.isBinaryColumnType(actualType)) return;
        rebuildForUuidId(c, tableName, kind, shape, idField, actualType);
    }

    private void rebuildForUuidId(@NotNull Connection c, @NotNull String tableName,
                                  @NotNull EventStream<?, ?> kind, @NotNull EncodeShape shape,
                                  @NotNull EncodeShape.FieldDef idField,
                                  @NotNull String actualIdType) throws SQLException {
        String table = dialect.quoteIdentifier(tableName);
        String tmpName = tableName + "_uuid_id_rebuild";
        String tmp = dialect.quoteIdentifier(tmpName);
        logger.info(() -> "[v2-ensureStore] " + tableName + "." + idField.name() + " is "
            + actualIdType + " but the record id is a UUID; rebuilding the table with a binary id"
            + " (legacy numeric ids become deterministic UUIDv7s)");

        // Columns not in the current shape (e.g. the legacy 'metadata'
        // column) are carried over verbatim so the rebuild drops no data.
        Map<String, String> extras = new LinkedHashMap<>();
        try (ResultSet cols = c.getMetaData().getColumns(null, null, tableName, null)) {
            while (cols.next()) {
                String name = cols.getString("COLUMN_NAME");
                if (fieldDefOrNull(shape, name) == null) extras.put(name, cols.getString("TYPE_NAME"));
            }
        }

        StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(tmp).append(" (");
        StringBuilder colList = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        List<String> copyCols = new ArrayList<>(shape.fields().size() + extras.size());
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (!copyCols.isEmpty()) { ddl.append(", "); colList.append(", "); placeholders.append(", "); }
            ddl.append(dialect.quoteIdentifier(f.name())).append(' ').append(dialect.columnTypeSql(f));
            if (f.name().equals(shape.idField())) ddl.append(" PRIMARY KEY");
            colList.append(dialect.quoteIdentifier(f.name()));
            placeholders.append('?');
            copyCols.add(f.name());
        }
        for (Map.Entry<String, String> e : extras.entrySet()) {
            ddl.append(", ").append(dialect.quoteIdentifier(e.getKey())).append(' ').append(e.getValue());
            colList.append(", ").append(dialect.quoteIdentifier(e.getKey()));
            placeholders.append(", ?");
            copyCols.add(e.getKey());
        }
        ddl.append(')');

        long copied = 0;
        long skipped = 0;
        boolean priorAutoCommit = c.getAutoCommit();
        c.setAutoCommit(false);
        boolean committed = false;
        try {
            try (Statement s = c.createStatement()) {
                s.executeUpdate("DROP TABLE IF EXISTS " + tmp);
                s.executeUpdate(ddl.toString());
            }
            try (PreparedStatement sel = c.prepareStatement("SELECT " + colList + " FROM " + table);
                 PreparedStatement ins = c.prepareStatement(
                     "INSERT INTO " + tmp + " (" + colList + ") VALUES (" + placeholders + ")");
                 ResultSet rs = sel.executeQuery()) {
                int batched = 0;
                while (rs.next()) {
                    UUID newId = rebuiltId(rs, kind.timestampField(), idField.name());
                    if (newId == null) { skipped++; continue; }
                    int idx = 1;
                    for (String col : copyCols) {
                        if (col.equals(idField.name())) {
                            ins.setBytes(idx++, UuidCodec.toBytes(newId));
                            continue;
                        }
                        EncodeShape.FieldDef f = fieldDefOrNull(shape, col);
                        ins.setObject(idx++, normalizedCopyValue(f, rs.getObject(col)));
                    }
                    ins.addBatch();
                    copied++;
                    if (++batched % 1024 == 0) ins.executeBatch();
                }
                ins.executeBatch();
            }
            try (Statement s = c.createStatement()) {
                s.executeUpdate("DROP TABLE " + table);
                s.executeUpdate("ALTER TABLE " + tmp + " RENAME TO " + table);
            }
            c.commit();
            committed = true;
        } finally {
            if (!committed) {
                try { c.rollback(); } catch (SQLException ignored) {}
            }
            c.setAutoCommit(priorAutoCommit);
        }
        long copiedFinal = copied;
        long skippedFinal = skipped;
        logger.info(() -> "[v2-ensureStore] rebuilt " + tableName + ": " + copiedFinal
            + " row(s) carried over" + (skippedFinal > 0
                ? ", " + skippedFinal + " row(s) skipped (unreadable id)" : ""));
    }

    /**
     * Mint the replacement id for one legacy row. Numeric ids become
     * deterministic UUIDv7s seeded from (timestamp, old id) so same-ms
     * rows keep their old monotonic order — identical to the phase-1
     * v3 → v4 migration. Binary and textual UUIDs pass through.
     */
    private static @Nullable UUID rebuiltId(@NotNull ResultSet rs, @NotNull String tsField,
                                            @NotNull String idColumn) throws SQLException {
        Object raw = rs.getObject(idColumn);
        if (raw instanceof Number n) return UuidV7.fromTimestampMs(rs.getLong(tsField), n.longValue());
        if (raw instanceof byte[] b && b.length == 16) return UuidCodec.fromBytes(b);
        if (raw instanceof String s) {
            try { return UUID.fromString(s); } catch (IllegalArgumentException ignored) { return null; }
        }
        return null;
    }

    /**
     * Normalize one legacy value for the rebuilt row. UUID fields stored
     * as text are re-encoded as 16-byte blobs — the query paths bind UUID
     * partition keys via {@link SqlBindings#bind} as blobs, so a raw TEXT
     * carry-over would never match and the row would vanish from partition
     * reads. Unparseable values pass through raw: the data survives even
     * if that row stays unqueryable by partition.
     */
    private static @Nullable Object normalizedCopyValue(@Nullable EncodeShape.FieldDef f,
                                                        @Nullable Object v) {
        if (f == null) return v; // extra column carried over verbatim
        if (v == null) return f.nullable() ? null : nonNullDefault(f);
        if (f.javaType() == UUID.class && v instanceof String s) {
            try {
                return UuidCodec.toBytes(UUID.fromString(s.trim()));
            } catch (IllegalArgumentException ignored) {
                return v;
            }
        }
        return v;
    }

    /**
     * Fallback for a NULL read from a legacy row into a NOT NULL column
     * of the rebuilt table (e.g. a column addMissingColumns added without
     * the constraint). Types with no safe stand-in return null and let
     * the insert surface the constraint violation.
     */
    private static @Nullable Object nonNullDefault(@NotNull EncodeShape.FieldDef f) {
        Class<?> t = f.javaType();
        if (t == long.class || t == Long.class || t == int.class || t == Integer.class
            || t == boolean.class || t == Boolean.class || t.isEnum()) return 0;
        if (t == double.class || t == Double.class || t == float.class || t == Float.class) return 0.0;
        if (t == String.class) return "";
        return null;
    }

    private static @Nullable EncodeShape.FieldDef fieldDefOrNull(@NotNull EncodeShape shape,
                                                                 @NotNull String fieldName) {
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (f.name().equalsIgnoreCase(fieldName)) return f;
        }
        return null;
    }

    private void addMissingColumns(@NotNull Connection c, @NotNull java.sql.Statement s,
                                   @NotNull String table, @NotNull EncodeShape shape) throws SQLException {
        java.util.Map<String, String> existing = new java.util.HashMap<>();
        try (java.sql.ResultSet cols = c.getMetaData().getColumns(null, null, table, null)) {
            while (cols.next()) {
                existing.put(
                    cols.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT),
                    cols.getString("TYPE_NAME"));
            }
        }
        if (existing.isEmpty()) return;
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (!existing.containsKey(f.name().toLowerCase(java.util.Locale.ROOT))) {
                String alter = "ALTER TABLE " + dialect.quoteIdentifier(table)
                    + " ADD COLUMN " + dialect.quoteIdentifier(f.name())
                    + " " + dialect.columnTypeSql(f).replace(" NOT NULL", "");
                logger.info(() -> "[v2-ensureStore] adding missing column: " + alter);
                try { s.executeUpdate(alter); } catch (SQLException ignored) {}
            }
        }
        for (EncodeShape.FieldDef f : shape.fields()) {
            String actualTypeName = existing.get(f.name().toLowerCase(java.util.Locale.ROOT));
            if (actualTypeName == null) continue;
            for (String alter : dialect.convertColumnToBinarySql(table, f, actualTypeName)) {
                logger.info(() -> "[v2-ensureStore] converting column to binary: " + alter);
                try {
                    s.executeUpdate(alter);
                } catch (SQLException e) {
                    String refreshedType = columnType(c, table, f.name());
                    if (refreshedType == null || !dialect.isBinaryColumnType(refreshedType)) {
                        throw e;
                    }
                }
            }
        }
    }

    private @Nullable String columnType(@NotNull Connection c, @NotNull String table,
                                        @NotNull String column) throws SQLException {
        try (java.sql.ResultSet cols = c.getMetaData().getColumns(null, null, table, null)) {
            while (cols.next()) {
                if (column.equalsIgnoreCase(cols.getString("COLUMN_NAME"))) {
                    return cols.getString("TYPE_NAME");
                }
            }
        }
        return null;
    }

    private static @NotNull EncodeShape.FieldDef fieldDef(@NotNull EncodeShape shape,
                                                          @NotNull String fieldName) {
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (f.name().equals(fieldName)) return f;
        }
        throw new IllegalArgumentException("no field '" + fieldName + "' in shape");
    }
}
