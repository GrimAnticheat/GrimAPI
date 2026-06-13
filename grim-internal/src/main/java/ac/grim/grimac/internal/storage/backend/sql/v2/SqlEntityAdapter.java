package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.codec.MergeMode;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.IndexSpec;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Cursors;
import ac.grim.grimac.api.storage.query.Page;
import org.jetbrains.annotations.Nullable;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodec;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodecs;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generic SQL adapter for {@link Entity} stores; vendor-specific SQL
 * fragments are produced by an injected {@link SqlDialect} so the same
 * adapter binary serves Postgres, MySQL, and SQLite once their dialects
 * are wired.
 *
 * <p><strong>Merge semantics.</strong> {@link #writeHandler} issues
 * {@code INSERT … ON CONFLICT (id) DO UPDATE SET …} where the SET
 * clause compiles per-field from the codec shape's
 * {@link ac.grim.grimac.api.storage.codec.MergeMode}:
 * <ul>
 *   <li>{@code OVERWRITE} → {@code col = EXCLUDED.col}</li>
 *   <li>{@code INSERT_ONLY} → column omitted from SET (existing
 *       value preserved on conflict, EXCLUDED still lands on first
 *       insert via the INSERT phase). Use for surrogate keys
 *       referenced elsewhere — e.g.
 *       {@code CheckCatalogRecord.checkId}.</li>
 *   <li>{@code PRESERVE_ON_NON_NULL} →
 *       {@code col = COALESCE(tbl.col, EXCLUDED.col)}. Use for
 *       fields where a prior non-null writer should beat a later
 *       null-carrying heartbeat — e.g.
 *       {@code SessionRecord.instanceId}.</li>
 *   <li>{@code MAX} → {@code col = GREATEST(tbl.col, EXCLUDED.col)}.
 *       Monotonic-increasing (e.g.
 *       {@code PlayerIdentity.lastSeenEpochMs}).</li>
 *   <li>{@code MIN} → {@code col = LEAST(tbl.col, EXCLUDED.col)}.
 *       Monotonic-decreasing (e.g.
 *       {@code PlayerIdentity.firstSeenEpochMs}).</li>
 * </ul>
 * Edge: when ALL non-id columns are {@code INSERT_ONLY}, the SET
 * clause would be empty, so {@link PostgresDialect#upsertSql} emits
 * {@code DO NOTHING} instead. Built-in records currently exercise
 * all five modes.
 *
 * <p>{@link ac.grim.grimac.api.storage.codec.Sentinel @Sentinel}
 * extends preserve semantics to primitive-long fields with an
 * "unset" sentinel (e.g. {@code SessionRecord.closedAtEpochMs} with
 * {@code OPEN = 0L}): the SET clause emits
 * {@code col = CASE WHEN tbl.col IS NULL OR tbl.col = <sentinel>
 * THEN EXCLUDED.col ELSE tbl.col END}. Sentinel value is inlined as
 * a compile-time literal so the prepared-statement parameter count
 * stays stable per shape (no extra bind slot).
 *
 * <p><strong>Mixed-writer caveat.</strong> The {@code @Sentinel}
 * CASE-WHEN canonicalises legacy SQL NULL rows by writing the
 * sentinel value (e.g. {@code 0L}) on the first v2 upsert. If the
 * legacy SQL writers (still using {@code COALESCE(closed_at, ?)} +
 * {@code setNull} when 0L) are running concurrently with v2, the
 * legacy {@code COALESCE} would see the now-non-null sentinel value
 * as "already set" and refuse to write a real close timestamp.
 * Cutover must be clean — stop the legacy writers before starting
 * v2 writers. Documented but not enforced here; the
 * {@code DataStoreLifecycle} wiring (Phase 1.4c) is responsible for
 * choosing one path at a time. Mirrors the same caveat on
 * {@code MongoEntityAdapter}.
 *
 * <p>Phase 5a sequence:
 * <ul>
 *   <li>5a.0 — shell + ensureStore + dropStore.</li>
 *   <li>5a.1 — writeHandler (INSERT … ON CONFLICT DO UPDATE) for
 *       atomic upserts via the codec's field accessors.</li>
 *   <li>5a.2 — execute GetById / GetMany / DeleteById via SELECT +
 *       prepared statements bound from the codec.</li>
 *   <li>5a.3 — FindByIndex / PrefixIndex / CountByIndex with keyset
 *       cursor seek, mirroring the Mongo adapter's
 *       {@code orderedColumnIndex} rule for compound indexes.</li>
 * </ul>
 */
@ApiStatus.Internal
public final class SqlEntityAdapter implements KindAdapter<Entity<?, ?, ?>> {

    private static final int SQL_BATCH_SIZE = 256;

    private final @NotNull DataSource ds;
    private final @NotNull SqlDialect dialect;
    private final @NotNull Logger logger;

    public SqlEntityAdapter(@NotNull DataSource ds, @NotNull SqlDialect dialect, @NotNull Logger logger) {
        this.ds = ds;
        this.dialect = dialect;
        this.logger = logger;
    }

    @SuppressWarnings("unchecked")
    @Override
    public @NotNull Class<Entity<?, ?, ?>> kindType() {
        return (Class<Entity<?, ?, ?>>) (Class<?>) Entity.class;
    }

    @Override
    public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_ENTITY, Capability.ATOMIC_UPSERT);
    }

    @Override
    public void ensureStore(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind) throws BackendException {
        String table = id.name();
        EncodeShape shape = kind.codec().shape();
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            String createTable = dialect.createTableSql(table, shape);
            logger.log(Level.FINE, () -> "ensureStore DDL: " + createTable);
            s.executeUpdate(createTable);

            // Schema evolution: if the table was created by an older
            // version (e.g. the legacy backend) it may be missing
            // columns that the current v2 EncodeShape declares. Add
            // them non-destructively so the upsert/read paths work
            // against both old and new rows.
            addMissingColumns(c, s, table, shape);

            for (IndexSpec spec : kind.secondaryIndexes()) {
                String createIdx = dialect.createIndexSql(table, spec);
                logger.log(Level.FINE, () -> "ensureStore DDL: " + createIdx);
                try { s.executeUpdate(createIdx); }
                catch (SQLException ignored) {
                    // Index may already exist under a different name
                    // (legacy layout). Swallow and continue.
                }
            }
        } catch (SQLException e) {
            throw new BackendException("failed to ensure " + dialect.name() + " entity table " + table, e);
        }
    }

    /**
     * Compare the table's actual columns (from DatabaseMetaData) against
     * the EncodeShape's fields. For any field in the shape that doesn't
     * exist as a column, issue ALTER TABLE ADD COLUMN. This lets v2
     * ensureStore upgrade a legacy-created table without dropping data.
     */
    private void addMissingColumns(@NotNull Connection c, @NotNull Statement s,
                                   @NotNull String table, @NotNull EncodeShape shape) throws SQLException {
        java.util.Map<String, String> existing = new java.util.HashMap<>();
        try (java.sql.ResultSet cols = c.getMetaData().getColumns(null, null, table, null)) {
            while (cols.next()) {
                existing.put(
                    cols.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT),
                    cols.getString("TYPE_NAME"));
            }
        }
        if (existing.isEmpty()) return; // table was just created — all columns present
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (!existing.containsKey(f.name().toLowerCase(java.util.Locale.ROOT))) {
                String alter = "ALTER TABLE " + dialect.quoteIdentifier(table)
                    + " ADD COLUMN " + dialect.quoteIdentifier(f.name())
                    + " " + dialect.columnTypeSql(f);
                // Remove NOT NULL for added columns — existing rows
                // don't have a value. The column needs to be nullable
                // so ALTER succeeds without a default.
                alter = alter.replace(" NOT NULL", "");
                String alterFinal = alter;
                logger.info(() -> "[v2-ensureStore] adding missing column: " + alterFinal);
                try { s.executeUpdate(alter); }
                catch (SQLException ignored) {
                    // Column may already exist (race between multiple
                    // instances, or case-sensitivity differences).
                }
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

    @Override
    public void dropStore(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind) throws BackendException {
        String table = id.name();
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            // DROP TABLE IF EXISTS works on every supported vendor;
            // the dialect provides vendor-correct identifier quoting.
            s.executeUpdate("DROP TABLE IF EXISTS " + dialect.quoteIdentifier(table));
        } catch (SQLException e) {
            throw new BackendException("failed to drop " + dialect.name() + " entity table " + table, e);
        }
    }

    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id,
            @NotNull Entity<?, ?, ?> kind,
            @NotNull Category<E> category) {
        @SuppressWarnings("unchecked")
        Entity<?, E, ?> typed = (Entity<?, E, ?>) kind;
        if (dialect.usesLegacySqliteUpsert()) {
            return new LegacySqliteEntityHandler<>(id, typed);
        }
        return new SqlEntityHandler<>(id, typed);
    }

    /**
     * Atomic upsert handler. One PreparedStatement template per
     * (table, shape) cached in {@link #upsertSql}; the handler
     * accumulates records until the Disruptor batch ends (or a fixed
     * cap is reached), then writes them through one JDBC batch.
     */
    private final class SqlEntityHandler<E> implements StorageEventHandler<E> {

        private final @NotNull StoreId id;
        private final @NotNull String upsertSql;
        @SuppressWarnings("rawtypes")
        private final @NotNull BsonCodec codec;
        private final @NotNull EncodeShape shape;
        private final @NotNull Function<E, Object> eventToRecord;
        private final @NotNull List<Object> pendingRecords = new ArrayList<>(SQL_BATCH_SIZE);

        @SuppressWarnings({"unchecked", "rawtypes"})
        SqlEntityHandler(@NotNull StoreId id, @NotNull Entity<?, E, ?> kind) {
            this.id = id;
            this.shape = kind.codec().shape();
            this.upsertSql = dialect.upsertSql(id.name(), shape);
            // Per-shape codec lookup: BsonCodec exposes readField for
            // typed access. We use it here just for the boxed Object
            // read; primitive accessors are an optimization for later.
            this.codec = BsonCodecs.regular(kind.recordType());
            this.eventToRecord = (Function) kind.eventToRecord();
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            Object record;
            try {
                record = eventToRecord.apply(event);
            } catch (RuntimeException e) {
                throw new BackendException("sql entity upsert failed for " + id, e);
            }
            try {
                pendingRecords.add(record);
                if (endOfBatch || pendingRecords.size() >= SQL_BATCH_SIZE) flushBatch();
            } catch (SQLException | RuntimeException e) {
                throw new BackendException("sql entity upsert failed for " + id, e);
            }
        }

        private void flushBatch() throws SQLException {
            if (pendingRecords.isEmpty()) return;
            try (Connection c = ds.getConnection()) {
                boolean priorAutoCommit = c.getAutoCommit();
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement(upsertSql)) {
                    for (Object record : pendingRecords) {
                        bindAllFields(ps, record);
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

        private void bindAllFields(@NotNull PreparedStatement ps, @NotNull Object record)
                throws SQLException {
            bindRecordFields(ps, shape, codec, record);
        }
    }

    /**
     * Pre-3.24 SQLite cannot parse {@code ON CONFLICT DO UPDATE}. Use
     * the same row semantics with a two-step transaction selected once
     * at handler construction: {@code INSERT OR IGNORE}, then
     * {@code UPDATE} with the same per-field merge expressions.
     */
    private final class LegacySqliteEntityHandler<E> implements StorageEventHandler<E> {

        private final @NotNull StoreId id;
        private final @NotNull String insertSql;
        private final @Nullable String updateSql;
        private final @NotNull List<Integer> updateIndexes;
        @SuppressWarnings("rawtypes")
        private final @NotNull BsonCodec codec;
        private final @NotNull EncodeShape shape;
        private final int idIndex;
        private final @NotNull Function<E, Object> eventToRecord;
        private final @NotNull List<Object> pendingRecords = new ArrayList<>(SQL_BATCH_SIZE);

        @SuppressWarnings({"unchecked", "rawtypes"})
        LegacySqliteEntityHandler(@NotNull StoreId id, @NotNull Entity<?, E, ?> kind) {
            this.id = id;
            this.shape = kind.codec().shape();
            this.insertSql = legacyInsertSql(id.name(), shape);
            this.updateIndexes = legacyUpdateIndexes(shape);
            this.updateSql = updateIndexes.isEmpty() ? null : legacyUpdateSql(id.name(), shape, updateIndexes);
            this.idIndex = idFieldIndex(shape);
            this.codec = BsonCodecs.regular(kind.recordType());
            this.eventToRecord = (Function) kind.eventToRecord();
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            Object record;
            try {
                record = eventToRecord.apply(event);
            } catch (RuntimeException e) {
                throw new BackendException("sql entity upsert failed for " + id, e);
            }
            try {
                pendingRecords.add(record);
                if (endOfBatch || pendingRecords.size() >= SQL_BATCH_SIZE) flushBatch();
            } catch (SQLException | RuntimeException e) {
                throw new BackendException("sql entity upsert failed for " + id, e);
            }
        }

        private void flushBatch() throws SQLException {
            if (pendingRecords.isEmpty()) return;
            try (Connection c = ds.getConnection()) {
                boolean priorAutoCommit = c.getAutoCommit();
                c.setAutoCommit(false);
                try {
                    try (PreparedStatement insert = c.prepareStatement(insertSql)) {
                        for (Object record : pendingRecords) {
                            bindAllFields(insert, record);
                            insert.addBatch();
                        }
                        insert.executeBatch();
                    }
                    if (updateSql != null) {
                        try (PreparedStatement update = c.prepareStatement(updateSql)) {
                            for (Object record : pendingRecords) {
                                bindUpdate(update, record);
                                update.addBatch();
                            }
                            update.executeBatch();
                        }
                    }
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

        private void bindAllFields(@NotNull PreparedStatement ps, @NotNull Object record)
                throws SQLException {
            bindRecordFields(ps, shape, codec, record);
        }

        private void bindUpdate(@NotNull PreparedStatement ps, @NotNull Object record) throws SQLException {
            bindLegacyUpdate(ps, shape, codec, record, updateIndexes, idIndex);
        }
    }

    private @NotNull String legacyInsertSql(@NotNull String tableName, @NotNull EncodeShape shape) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT OR IGNORE INTO ").append(dialect.quoteIdentifier(tableName)).append(" (");
        boolean first = true;
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (!first) sb.append(", ");
            sb.append(dialect.quoteIdentifier(f.name()));
            first = false;
        }
        sb.append(") VALUES (");
        for (int i = 0; i < shape.fields().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append('?');
        }
        sb.append(')');
        return sb.toString();
    }

    private @NotNull List<Integer> legacyUpdateIndexes(@NotNull EncodeShape shape) {
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < shape.fields().size(); i++) {
            EncodeShape.FieldDef f = shape.fields().get(i);
            if (f.name().equals(shape.idField())) continue;
            if (f.mergeMode() == MergeMode.INSERT_ONLY) continue;
            indexes.add(i);
        }
        return indexes;
    }

    private @NotNull String legacyUpdateSql(
            @NotNull String tableName,
            @NotNull EncodeShape shape,
            @NotNull List<Integer> updateIndexes) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(dialect.quoteIdentifier(tableName)).append(" SET ");
        boolean first = true;
        for (int index : updateIndexes) {
            if (!first) sb.append(", ");
            EncodeShape.FieldDef f = shape.fields().get(index);
            String col = dialect.quoteIdentifier(f.name());
            sb.append(col).append(" = ");
            switch (f.mergeMode()) {
                case OVERWRITE -> sb.append('?');
                case PRESERVE_ON_NON_NULL -> sb.append("COALESCE(").append(col).append(", ?)");
                case PRESERVE_ON_NON_SENTINEL -> sb.append("CASE WHEN ")
                    .append(col).append(" IS NULL OR ").append(col).append(" = ")
                    .append(f.sentinelValue()).append(" THEN ? ELSE ").append(col).append(" END");
                case MAX -> sb.append("MAX(").append(col).append(", ?)");
                case MIN -> sb.append("MIN(").append(col).append(", ?)");
                case INSERT_ONLY -> { /* excluded above */ }
            }
            first = false;
        }
        sb.append(" WHERE ").append(dialect.quoteIdentifier(shape.idField())).append(" = ?");
        return sb.toString();
    }

    private static int idFieldIndex(@NotNull EncodeShape shape) {
        for (int i = 0; i < shape.fields().size(); i++) {
            if (shape.fields().get(i).name().equals(shape.idField())) return i;
        }
        throw new IllegalArgumentException("EncodeShape id field missing: " + shape.idField());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind, @NotNull Operation<R> op) throws BackendException {
        try {
            if (op instanceof EntityOps.UpsertOp u)       { upsertRecord(id, kind, u.record()); return null; }
            if (op instanceof EntityOps.GetByIdOp g)         return (R) getById(id, kind, g);
            if (op instanceof EntityOps.GetManyOp g)         return (R) getMany(id, kind, g);
            if (op instanceof EntityOps.DeleteByIdOp d)    { deleteById(id, kind, d); return null; }
            if (op instanceof EntityOps.DeleteByIndexOp d) { deleteByIndex(id, kind, d); return null; }
            if (op instanceof EntityOps.FindByIndexOp f)     return (R) findByIndex(id, kind, f);
            if (op instanceof EntityOps.PrefixIndexOp p)     return (R) prefixIndex(id, kind, p);
            if (op instanceof EntityOps.CountByIndexOp c)    return (R) Long.valueOf(countByIndex(id, kind, c));
            throw new UnsupportedOperationException(
                "SqlEntityAdapter does not handle " + op.getClass().getSimpleName());
        } catch (java.sql.SQLException | RuntimeException e) {
            throw new BackendException("sql entity execute failed for "
                + op.getClass().getSimpleName() + " on " + id, e);
        }
    }

    // ============================== execute dispatch ==============================

    private void upsertRecord(
            @NotNull StoreId id,
            @NotNull Entity<?, ?, ?> kind,
            @NotNull Object record) throws SQLException {
        if (!kind.recordType().isInstance(record)) {
            throw new IllegalArgumentException("record type " + record.getClass().getName()
                    + " does not match entity " + kind.recordType().getName());
        }
        EncodeShape shape = kind.codec().shape();
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        if (dialect.usesLegacySqliteUpsert()) {
            upsertLegacySqliteRecord(id, shape, codec, record);
        } else {
            upsertModernSqlRecord(id, shape, codec, record);
        }
    }

    private void upsertModernSqlRecord(
            @NotNull StoreId id,
            @NotNull EncodeShape shape,
            @NotNull BsonCodec codec,
            @NotNull Object record) throws SQLException {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(dialect.upsertSql(id.name(), shape))) {
            bindRecordFields(ps, shape, codec, record);
            ps.executeUpdate();
        }
    }

    private void upsertLegacySqliteRecord(
            @NotNull StoreId id,
            @NotNull EncodeShape shape,
            @NotNull BsonCodec codec,
            @NotNull Object record) throws SQLException {
        List<Integer> updateIndexes = legacyUpdateIndexes(shape);
        String updateSql = updateIndexes.isEmpty() ? null : legacyUpdateSql(id.name(), shape, updateIndexes);
        int idIndex = idFieldIndex(shape);
        try (Connection c = ds.getConnection()) {
            boolean priorAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                try (PreparedStatement insert = c.prepareStatement(legacyInsertSql(id.name(), shape))) {
                    bindRecordFields(insert, shape, codec, record);
                    insert.executeUpdate();
                }
                if (updateSql != null) {
                    try (PreparedStatement update = c.prepareStatement(updateSql)) {
                        bindLegacyUpdate(update, shape, codec, record, updateIndexes, idIndex);
                        update.executeUpdate();
                    }
                }
                c.commit();
            } catch (SQLException | RuntimeException e) {
                try { c.rollback(); } catch (SQLException ignored) {}
                throw e;
            } finally {
                c.setAutoCommit(priorAutoCommit);
            }
        }
    }

    private static void bindRecordFields(
            @NotNull PreparedStatement ps,
            @NotNull EncodeShape shape,
            @NotNull BsonCodec codec,
            @NotNull Object record) throws SQLException {
        int n = shape.fields().size();
        for (int i = 0; i < n; i++) {
            EncodeShape.FieldDef f = shape.fields().get(i);
            SqlBindings.bind(ps, i + 1, f, codec.readField(record, i));
        }
    }

    private static void bindLegacyUpdate(
            @NotNull PreparedStatement ps,
            @NotNull EncodeShape shape,
            @NotNull BsonCodec codec,
            @NotNull Object record,
            @NotNull List<Integer> updateIndexes,
            int idIndex) throws SQLException {
        int param = 1;
        for (int fieldIndex : updateIndexes) {
            EncodeShape.FieldDef f = shape.fields().get(fieldIndex);
            SqlBindings.bind(ps, param++, f, codec.readField(record, fieldIndex));
        }
        EncodeShape.FieldDef idField = shape.fields().get(idIndex);
        SqlBindings.bind(ps, param, idField, codec.readField(record, idIndex));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <ID, R> Optional<R> getById(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind,
                                        @NotNull EntityOps.GetByIdOp<ID, R> op) throws java.sql.SQLException {
        EncodeShape shape = kind.codec().shape();
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        String sql = "SELECT * FROM " + dialect.quoteIdentifier(id.name())
            + " WHERE " + dialect.quoteIdentifier(shape.idField()) + " = ?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            SqlBindings.bind(ps, 1, idFieldDef(shape), op.id());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                R record = (R) decodeRow(codec, shape, rs);
                return Optional.of(record);
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <ID, R> List<R> getMany(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind,
                                    @NotNull EntityOps.GetManyOp<ID, R> op) throws java.sql.SQLException {
        if (op.ids().isEmpty()) return List.of();
        EncodeShape shape = kind.codec().shape();
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        StringBuilder sql = new StringBuilder()
            .append("SELECT * FROM ").append(dialect.quoteIdentifier(id.name()))
            .append(" WHERE ").append(dialect.quoteIdentifier(shape.idField())).append(" IN (");
        for (int i = 0; i < op.ids().size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append('?');
        }
        sql.append(')');
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql.toString())) {
            EncodeShape.FieldDef idDef = idFieldDef(shape);
            int i = 1;
            for (ID idVal : op.ids()) SqlBindings.bind(ps, i++, idDef, idVal);
            List<R> out = new ArrayList<>(op.ids().size());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add((R) decodeRow(codec, shape, rs));
            }
            return out;
        }
    }

    private <ID> void deleteById(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind,
                                 @NotNull EntityOps.DeleteByIdOp<ID> op) throws java.sql.SQLException {
        EncodeShape shape = kind.codec().shape();
        String sql = "DELETE FROM " + dialect.quoteIdentifier(id.name())
            + " WHERE " + dialect.quoteIdentifier(shape.idField()) + " = ?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            SqlBindings.bind(ps, 1, idFieldDef(shape), op.id());
            ps.executeUpdate();
        }
    }

    private void deleteByIndex(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind,
                               @NotNull EntityOps.DeleteByIndexOp op) throws java.sql.SQLException {
        IndexSpec spec = requireIndex(kind, op.indexName());
        EncodeShape shape = kind.codec().shape();
        // Leading column equality — same contract as FindByIndexOp.
        // SQL planner picks the index by its leading column on a
        // straight equality predicate.
        String leading = stripDir(spec.fields().get(0));
        String sql = "DELETE FROM " + dialect.quoteIdentifier(id.name())
            + " WHERE " + dialect.quoteIdentifier(leading) + " = ?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            SqlBindings.bind(ps, 1, fieldDef(shape, leading), op.key());
            ps.executeUpdate();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object decodeRow(@NotNull BsonCodec codec, @NotNull EncodeShape shape,
                             @NotNull ResultSet rs) throws java.sql.SQLException {
        Object[] args = new Object[shape.fields().size()];
        for (int i = 0; i < shape.fields().size(); i++) {
            args[i] = SqlBindings.extract(rs, shape.fields().get(i));
        }
        return codec.decodeFromValues(args);
    }

    private static @NotNull EncodeShape.FieldDef idFieldDef(@NotNull EncodeShape shape) {
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (f.name().equals(shape.idField())) return f;
        }
        throw new IllegalStateException("shape has no field matching idField " + shape.idField());
    }

    // ============================== cursor-driven ops ==============================

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <R> Page<R> findByIndex(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind,
                                    @NotNull EntityOps.FindByIndexOp<R> op) throws java.sql.SQLException {
        EncodeShape shape = kind.codec().shape();
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        IndexSpec spec = requireIndex(kind, op.indexName());
        // Equality pins the leading column; cursor seeks on the NEXT
        // ordered column (or _id-only if no further columns).
        String leadingCol = stripDir(spec.fields().get(0));
        EncodeShape.FieldDef leadingDef = fieldDef(shape, leadingCol);
        String leadingExpr = spec.caseInsensitivePrefix()
            ? "LOWER(" + dialect.quoteIdentifier(leadingCol) + ")"
            : dialect.quoteIdentifier(leadingCol);
        Object leadingValue = spec.caseInsensitivePrefix() && op.key() instanceof String s
            ? s.toLowerCase(java.util.Locale.ROOT)
            : op.key();
        List<BoundValue> params = new ArrayList<>();
        StringBuilder where = new StringBuilder();
        where.append(leadingExpr).append(" = ?");
        params.add(new BoundValue(leadingDef, leadingValue));
        appendCursorWhere(where, params, shape, spec, /*equalityColumnIndex=*/0, op.cursor());
        return pageRead(id, kind, shape, codec, spec, /*equalityColumnIndex=*/0, where, params, op.pageSize());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <R> Page<R> prefixIndex(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind,
                                    @NotNull EntityOps.PrefixIndexOp<R> op) throws java.sql.SQLException {
        EncodeShape shape = kind.codec().shape();
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        IndexSpec spec = requireIndex(kind, op.indexName());
        String leadingCol = stripDir(spec.fields().get(0));
        EncodeShape.FieldDef leadingDef = fieldDef(shape, leadingCol);
        // caseInsensitivePrefix: target the functional index created at
        // ensureStore time via LOWER(col) on both filter and ORDER BY,
        // and lowercase the prefix value. Without caseInsensitivePrefix
        // we hit the plain column.
        String filterExpr = spec.caseInsensitivePrefix()
            ? "LOWER(" + dialect.quoteIdentifier(leadingCol) + ")"
            : dialect.quoteIdentifier(leadingCol);
        String prefix = spec.caseInsensitivePrefix()
            ? op.prefix().toLowerCase(java.util.Locale.ROOT)
            : op.prefix();
        // Escape LIKE metachars (%, _, !) in the user-provided prefix
        // so a name like "abc_123" doesn't accidentally match "abcX123".
        String escaped = escapeLike(prefix);
        StringBuilder where = new StringBuilder();
        // Use ! rather than backslash for portability across MySQL,
        // Postgres, and SQLite string-literal escape rules.
        where.append(filterExpr).append(" LIKE ? ESCAPE '!'");
        List<BoundValue> params = new ArrayList<>();
        params.add(new BoundValue(stringFieldDef(), escaped + "%"));
        // Cursor: range scan on leading column, no equality pin, so
        // ordered column is field 0 itself (equalityColumnIndex = -1).
        appendCursorWhere(where, params, shape, spec, /*equalityColumnIndex=*/-1, op.cursor());
        return pageRead(id, kind, shape, codec, spec, /*equalityColumnIndex=*/-1, where, params, op.pageSize());
    }

    private long countByIndex(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind,
                              @NotNull EntityOps.CountByIndexOp op) throws java.sql.SQLException {
        EncodeShape shape = kind.codec().shape();
        IndexSpec spec = requireIndex(kind, op.indexName());
        String leadingCol = stripDir(spec.fields().get(0));
        String sql = "SELECT COUNT(*) FROM " + dialect.quoteIdentifier(id.name())
            + " WHERE " + dialect.quoteIdentifier(leadingCol) + " = ?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            SqlBindings.bind(ps, 1, fieldDef(shape, leadingCol), op.key());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <R> Page<R> pageRead(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind,
                                 @NotNull EncodeShape shape, @NotNull BsonCodec codec,
                                 @NotNull IndexSpec spec, int equalityColumnIndex,
                                 @NotNull StringBuilder where, @NotNull List<BoundValue> params,
                                 int pageSize) throws java.sql.SQLException {
        int ps = Math.max(1, pageSize);
        String orderBy = buildOrderBy(spec, shape);
        String sql = "SELECT * FROM " + dialect.quoteIdentifier(id.name())
            + " WHERE " + where + " " + orderBy + " LIMIT " + (ps + 1);
        try (Connection c = ds.getConnection(); PreparedStatement st = c.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                BoundValue p = params.get(i);
                SqlBindings.bind(st, i + 1, p.def, p.value);
            }
            List<R> items = new ArrayList<>(ps);
            Object lastRecord = null;
            try (ResultSet rs = st.executeQuery()) {
                // Loop conditions must put the size check FIRST so that
                // rs.next() isn't called when the page is already full
                // — otherwise the LIMIT ps+1 sentinel row is consumed
                // by this loop, hasMore = rs.next() advances past it,
                // and the cursor never gets emitted.
                while (items.size() < ps && rs.next()) {
                    R rec = (R) decodeRow(codec, shape, rs);
                    items.add(rec);
                    lastRecord = rec;
                }
                boolean hasMore = rs.next();
                Cursor next = (hasMore && lastRecord != null)
                    ? encodeCursorFromRecord(lastRecord, shape, codec, spec, equalityColumnIndex)
                    : null;
                return new Page<>(items, next);
            }
        }
    }

    /**
     * Build the {@code ORDER BY} fragment for an index spec. Mirrors
     * the spec's direction prefixes and always tie-breaks by the id
     * column ASC so pagination is deterministic.
     */
    private @NotNull String buildOrderBy(@NotNull IndexSpec spec, @NotNull EncodeShape shape) {
        StringBuilder sb = new StringBuilder("ORDER BY ");
        for (int i = 0; i < spec.fields().size(); i++) {
            String f = spec.fields().get(i);
            if (i > 0) sb.append(", ");
            boolean desc = f.startsWith("-");
            String name = desc ? f.substring(1) : f;
            // Wrap LEADING column of a caseInsensitivePrefix index in
            // LOWER() so the planner uses the functional index for
            // ordering too (matches the WHERE clause).
            if (i == 0 && spec.caseInsensitivePrefix()) {
                sb.append("LOWER(").append(dialect.quoteIdentifier(name)).append(')');
            } else {
                sb.append(dialect.quoteIdentifier(name));
            }
            sb.append(desc ? " DESC" : " ASC");
        }
        // Deterministic tie-break: id ASC.
        sb.append(", ").append(dialect.quoteIdentifier(shape.idField())).append(" ASC");
        return sb.toString();
    }

    private @NotNull Cursor encodeCursorFromRecord(@NotNull Object record, @NotNull EncodeShape shape,
                                                    @SuppressWarnings("rawtypes") @NotNull BsonCodec codec,
                                                    @NotNull IndexSpec spec, int equalityColumnIndex) {
        int orderedIdx = orderedColumnIndex(spec, equalityColumnIndex);
        byte[] idBytes = idToBytes(codec.readField(record, fieldIndex(shape, shape.idField())));
        if (orderedIdx < 0) {
            return Cursors.encode(0L, idBytes);
        }
        String orderedCol = stripDir(spec.fields().get(orderedIdx));
        int idx = fieldIndex(shape, orderedCol);
        Object orderedVal = codec.readField(record, idx);
        Class<?> t = shape.fields().get(idx).javaType();
        if (orderedVal == null) {
            // null ordered values fall back to id-only cursor; comparing
            // null in SQL keyset is fraught (NULLS FIRST/LAST varies) and
            // a real schema would mark the ordered column NOT NULL.
            return Cursors.encode(0L, idBytes);
        }
        if (t == long.class || t == Long.class || t == int.class || t == Integer.class
                || t.isEnum() || t == boolean.class || t == Boolean.class) {
            long ord = (orderedVal instanceof Number n) ? n.longValue()
                : (orderedVal instanceof Enum<?> e) ? e.ordinal()
                : (orderedVal instanceof Boolean b) ? (b ? 1L : 0L)
                : 0L;
            return Cursors.encode(ord, idBytes);
        }
        if (t == double.class || t == Double.class || t == float.class || t == Float.class) {
            double d = ((Number) orderedVal).doubleValue();
            byte[] payload = new byte[8];
            java.nio.ByteBuffer.wrap(payload).putDouble(d);
            return Cursors.encodeTyped(Cursors.TYPE_DOUBLE, payload, idBytes);
        }
        if (t == String.class) {
            String s = (String) orderedVal;
            // For caseInsensitivePrefix the comparison is on LOWER(col),
            // so the cursor value must mirror that to align ordering.
            if (spec.caseInsensitivePrefix() && orderedIdx == 0) {
                s = s.toLowerCase(java.util.Locale.ROOT);
            }
            return Cursors.encodeTyped(Cursors.TYPE_STRING,
                s.getBytes(java.nio.charset.StandardCharsets.UTF_8), idBytes);
        }
        if (t == java.util.UUID.class) {
            byte[] payload = uuidToBytes((java.util.UUID) orderedVal);
            return Cursors.encodeTyped(Cursors.TYPE_BINARY,
                packBinary((byte) 0x04 /*UUID_STANDARD*/, payload), idBytes);
        }
        if (t == byte[].class) {
            return Cursors.encodeTyped(Cursors.TYPE_BINARY,
                packBinary((byte) 0x00, (byte[]) orderedVal), idBytes);
        }
        // Fallback: stringify (deterministic, lossless for the types
        // we expect at indexed columns).
        return Cursors.encodeTyped(Cursors.TYPE_STRING,
            String.valueOf(orderedVal).getBytes(java.nio.charset.StandardCharsets.UTF_8), idBytes);
    }

    /**
     * Append the cursor seek predicate to the WHERE builder, binding
     * the seek-key value(s) into {@code params}. Mirrors the
     * MongoEntityAdapter ordered-past-OR-tied-then-id-past pattern.
     */
    private void appendCursorWhere(@NotNull StringBuilder where,
                                   @NotNull List<BoundValue> params,
                                   @NotNull EncodeShape shape,
                                   @NotNull IndexSpec spec,
                                   int equalityColumnIndex,
                                   @Nullable Cursor cursor) {
        if (cursor == null) return;
        int orderedIdx = orderedColumnIndex(spec, equalityColumnIndex);
        EncodeShape.FieldDef idDef = fieldDef(shape, shape.idField());
        byte schema = Cursors.peekSchema(cursor);
        if (orderedIdx < 0) {
            byte[] idBytes = (schema == Cursors.SCHEMA_TYPED_PAIR)
                ? Cursors.decodeTyped(cursor).idBytes()
                : Cursors.decode(cursor).idBytes();
            where.append(" AND ").append(dialect.quoteIdentifier(shape.idField())).append(" > ?");
            params.add(new BoundValue(idDef, bytesToIdValue(idDef, idBytes)));
            return;
        }
        String orderedCol = stripDir(spec.fields().get(orderedIdx));
        EncodeShape.FieldDef orderedDef = fieldDef(shape, orderedCol);
        String orderedExpr = (spec.caseInsensitivePrefix() && orderedIdx == 0)
            ? "LOWER(" + dialect.quoteIdentifier(orderedCol) + ")"
            : dialect.quoteIdentifier(orderedCol);
        boolean desc = spec.fields().get(orderedIdx).startsWith("-");
        Object orderedVal;
        byte[] idBytes;
        if (schema == Cursors.SCHEMA_TYPED_PAIR) {
            Cursors.DecodedTyped d = Cursors.decodeTyped(cursor);
            orderedVal = decodeTypedOrderedValue(d.typeTag(), d.orderedBytes(), orderedDef);
            idBytes = d.idBytes();
        } else {
            Cursors.Decoded d = Cursors.decode(cursor);
            orderedVal = decodeNumericOrderedValue(d.orderedKey(), orderedDef);
            idBytes = d.idBytes();
        }
        Object idValue = bytesToIdValue(idDef, idBytes);
        String cmp = desc ? "<" : ">";
        where.append(" AND (")
             .append(orderedExpr).append(' ').append(cmp).append(" ?")
             .append(" OR (").append(orderedExpr).append(" = ? AND ")
             .append(dialect.quoteIdentifier(shape.idField())).append(" > ?))");
        params.add(new BoundValue(orderedDef, orderedVal));
        params.add(new BoundValue(orderedDef, orderedVal));
        params.add(new BoundValue(idDef, idValue));
    }

    // ---- helpers ----

    private static int orderedColumnIndex(@NotNull IndexSpec spec, int equalityColumnIndex) {
        int next = equalityColumnIndex + 1;
        return next < spec.fields().size() ? next : -1;
    }

    private static @NotNull String stripDir(@NotNull String f) {
        return f.startsWith("-") ? f.substring(1) : f;
    }

    private static @NotNull EncodeShape.FieldDef fieldDef(@NotNull EncodeShape shape, @NotNull String name) {
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (f.name().equals(name)) return f;
        }
        throw new IllegalStateException("shape has no field " + name);
    }

    private static int fieldIndex(@NotNull EncodeShape shape, @NotNull String name) {
        for (int i = 0; i < shape.fields().size(); i++) {
            if (shape.fields().get(i).name().equals(name)) return i;
        }
        throw new IllegalStateException("shape has no field " + name);
    }

    private static @NotNull EncodeShape.FieldDef stringFieldDef() {
        // Synthetic FieldDef for prefix-like binding (String literal we
        // build, not a record column). Reuses the codec's FieldDef shape
        // since SqlBindings.bind only consults javaType + nullable.
        return new EncodeShape.FieldDef("__prefix__", String.class,
            ac.grim.grimac.api.storage.codec.FieldKind.VALUE, false, null, 0, 0);
    }

    private static @NotNull IndexSpec requireIndex(@NotNull Entity<?, ?, ?> kind, @NotNull String name) {
        for (IndexSpec spec : kind.secondaryIndexes()) {
            if (spec.name().equals(name)) return spec;
        }
        throw new IllegalArgumentException(
            "no secondary index named '" + name + "' on Entity " + kind.name());
    }

    private static byte @NotNull [] uuidToBytes(@NotNull java.util.UUID u) {
        byte[] out = new byte[16];
        java.nio.ByteBuffer.wrap(out).putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits());
        return out;
    }

    private static byte @NotNull [] idToBytes(@Nullable Object idValue) {
        if (idValue == null) return new byte[0];
        if (idValue instanceof java.util.UUID u) return uuidToBytes(u);
        if (idValue instanceof byte[] b) return b;
        if (idValue instanceof Number n) {
            byte[] out = new byte[8];
            java.nio.ByteBuffer.wrap(out).putLong(n.longValue());
            return out;
        }
        // String / other → UTF-8 bytes for deterministic cursor stability.
        return idValue.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static @NotNull Object bytesToIdValue(@NotNull EncodeShape.FieldDef idDef, byte @NotNull [] idBytes) {
        Class<?> t = idDef.javaType();
        if (t == java.util.UUID.class) {
            if (idBytes.length != 16) {
                throw new IllegalArgumentException("UUID id cursor bytes must be 16, got " + idBytes.length);
            }
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(idBytes);
            return new java.util.UUID(buf.getLong(), buf.getLong());
        }
        if (t == String.class) {
            return new String(idBytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        if (t == long.class || t == Long.class) {
            if (idBytes.length != 8) {
                throw new IllegalArgumentException("long id cursor bytes must be 8, got " + idBytes.length);
            }
            return java.nio.ByteBuffer.wrap(idBytes).getLong();
        }
        if (t == byte[].class) return idBytes;
        throw new IllegalArgumentException("cannot decode id of type " + t.getName() + " from cursor bytes");
    }

    private static byte @NotNull [] packBinary(byte subtype, byte @NotNull [] data) {
        byte[] out = new byte[1 + data.length];
        out[0] = subtype;
        System.arraycopy(data, 0, out, 1, data.length);
        return out;
    }

    private static @NotNull Object decodeTypedOrderedValue(byte typeTag, byte @NotNull [] bytes,
                                                           @NotNull EncodeShape.FieldDef def) {
        switch (typeTag) {
            case Cursors.TYPE_STRING -> {
                return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            }
            case Cursors.TYPE_DOUBLE -> {
                if (bytes.length != 8) throw new IllegalArgumentException("DOUBLE typed cursor must be 8 bytes");
                return java.nio.ByteBuffer.wrap(bytes).getDouble();
            }
            case Cursors.TYPE_BINARY -> {
                // Payload is [1 byte subtype][N bytes]. For SQL we drop
                // the subtype byte; the typed value is the raw bytes
                // (Postgres UUID or BYTEA depending on column type).
                if (bytes.length < 1) throw new IllegalArgumentException("BINARY typed cursor missing subtype");
                byte[] data = new byte[bytes.length - 1];
                System.arraycopy(bytes, 1, data, 0, data.length);
                if (def.javaType() == java.util.UUID.class) {
                    if (data.length != 16) {
                        throw new IllegalArgumentException("UUID column expects 16-byte cursor payload");
                    }
                    java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data);
                    return new java.util.UUID(buf.getLong(), buf.getLong());
                }
                return data;
            }
            case Cursors.TYPE_DATE -> {
                if (bytes.length != 8) throw new IllegalArgumentException("DATE typed cursor must be 8 bytes");
                return new java.util.Date(java.nio.ByteBuffer.wrap(bytes).getLong());
            }
            default -> throw new IllegalArgumentException("unknown typed-cursor tag 0x"
                + Integer.toHexString(typeTag & 0xff));
        }
    }

    private static @NotNull Object decodeNumericOrderedValue(long key, @NotNull EncodeShape.FieldDef def) {
        Class<?> t = def.javaType();
        if (t == long.class || t == Long.class)   return key;
        if (t == int.class || t == Integer.class) return (int) key;
        if (t == boolean.class || t == Boolean.class) return key != 0;
        if (t.isEnum()) {
            @SuppressWarnings({"rawtypes", "unchecked"})
            Object[] constants = ((Class<Enum>) t).getEnumConstants();
            int ord = (int) key;
            return constants[ord];
        }
        // Doubles / Floats encoded numerically in the legacy path
        // shouldn't happen post-Phase 3b.3 (they take the typed
        // double path). Defensive fallback.
        if (t == double.class || t == Double.class) return (double) key;
        if (t == float.class || t == Float.class)   return (float) key;
        return key;
    }

    private static @NotNull String escapeLike(@NotNull String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' || c == '_' || c == '!') sb.append('!');
            sb.append(c);
        }
        return sb.toString();
    }

    private record BoundValue(@NotNull EncodeShape.FieldDef def, @Nullable Object value) {}

    @Override
    public @NotNull List<Migration<Entity<?, ?, ?>>> migrations(@NotNull Entity<?, ?, ?> kind) {
        return List.of();
    }
}
