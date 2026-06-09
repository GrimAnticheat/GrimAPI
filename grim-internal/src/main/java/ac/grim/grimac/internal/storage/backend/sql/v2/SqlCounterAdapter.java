package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.Counter;
import ac.grim.grimac.api.storage.kind.CounterEvent;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.CounterOps;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.jetbrains.annotations.Nullable;

/**
 * SQL adapter for {@link Counter} stores. Each counter row is one row
 * of shape {@code (id <key_type> PRIMARY KEY, value BIGINT NOT NULL
 * DEFAULT 0)} — dense single-table layout, atomic per-key increment
 * via Postgres {@code INSERT ... ON CONFLICT ... RETURNING} (no
 * read-modify-write).
 *
 * <p>Mirrors the Mongo {@code MongoCounterAdapter} operation set:
 * GetOp, GetManyOp, IncrementByOp, SetIfHigherOp.
 *
 * <p>The ring-buffer writeHandler ingests {@link CounterEvent} deltas
 * as fire-and-forget atomic increments.
 */
@ApiStatus.Internal
public final class SqlCounterAdapter implements KindAdapter<Counter<?>> {

    private final @NotNull DataSource ds;
    private final @NotNull SqlDialect dialect;
    private final @NotNull Logger logger;

    public SqlCounterAdapter(@NotNull DataSource ds, @NotNull SqlDialect dialect,
                             @NotNull Logger logger) {
        this.ds = ds;
        this.dialect = dialect;
        this.logger = logger;
    }

    @SuppressWarnings("unchecked")
    @Override public @NotNull Class<Counter<?>> kindType() {
        return (Class<Counter<?>>) (Class<?>) Counter.class;
    }

    @Override public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_COUNTER, Capability.ATOMIC_UPSERT);
    }

    @Override
    public void ensureStore(@NotNull StoreId id, @NotNull Counter<?> kind) throws BackendException {
        String table = dialect.quoteIdentifier(id.name());
        String keyCol = keyColumnType(kind.keyType());
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " ("
            + "id " + keyCol + " PRIMARY KEY, "
            + "value BIGINT NOT NULL DEFAULT 0)";
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate(ddl);
        } catch (SQLException e) {
            throw new BackendException("failed to ensure counter table " + id, e);
        }
    }

    @Override
    public void dropStore(@NotNull StoreId id, @NotNull Counter<?> kind) throws BackendException {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("DROP TABLE IF EXISTS " + dialect.quoteIdentifier(id.name()));
        } catch (SQLException e) {
            throw new BackendException("failed to drop counter table " + id, e);
        }
    }

    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id, @NotNull Counter<?> kind, @NotNull Category<E> category) {
        String table = dialect.quoteIdentifier(id.name());
        String upsertSql = dialect.counterIncrementSql(table);
        if (dialect.usesLegacySqliteUpsert()) {
            return new LegacyCounterWriteHandler<>(table);
        }
        return new CounterWriteHandler<>(upsertSql);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull Counter<?> kind,
                         @NotNull Operation<R> op) throws BackendException {
        try {
            if (op instanceof CounterOps.GetOp g)         return (R) Long.valueOf(get(id, g));
            if (op instanceof CounterOps.GetManyOp g)     return (R) getMany(id, g);
            if (op instanceof CounterOps.IncrementByOp i) return (R) Long.valueOf(incrementBy(id, i));
            if (op instanceof CounterOps.SetIfHigherOp s) return (R) Long.valueOf(setIfHigher(id, s));
            throw new UnsupportedOperationException(
                "SqlCounterAdapter does not handle " + op.getClass().getName());
        } catch (SQLException e) {
            throw new BackendException("sql counter execute failed for " + op.getClass().getSimpleName(), e);
        }
    }

    @Override
    public @NotNull List<Migration<Counter<?>>> migrations(@NotNull Counter<?> kind) {
        return List.of();
    }

    // ============================== writeHandler ==============================

    private final class CounterWriteHandler<E> implements StorageEventHandler<E> {
        private final String upsertSql;
        CounterWriteHandler(String upsertSql) { this.upsertSql = upsertSql; }

        @Override
        public void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            CounterEvent<?> ce = (CounterEvent<?>) event;
            if (ce.key == null) return;
            if (ce.delta == 0L) return;
            try (Connection c = ds.getConnection();
                 PreparedStatement ps = c.prepareStatement(upsertSql)) {
                bindKey(ps, 1, ce.key);
                ps.setLong(2, ce.delta);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new BackendException("counter write failed", e);
            }
        }
    }

    private final class LegacyCounterWriteHandler<E> implements StorageEventHandler<E> {
        private final String table;

        LegacyCounterWriteHandler(String table) {
            this.table = table;
        }

        @Override
        public void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            CounterEvent<?> ce = (CounterEvent<?>) event;
            if (ce.key == null) return;
            if (ce.delta == 0L) return;
            try (Connection c = ds.getConnection()) {
                boolean prior = c.getAutoCommit();
                c.setAutoCommit(false);
                try {
                    legacyInsertCounter(c, table, ce.key, 0L);
                    legacyAddCounter(c, table, ce.key, ce.delta);
                    c.commit();
                } catch (Exception e) {
                    try { c.rollback(); } catch (SQLException ignored) {}
                    throw e;
                } finally {
                    c.setAutoCommit(prior);
                }
            } catch (SQLException e) {
                throw new BackendException("counter write failed", e);
            }
        }
    }

    // ============================== execute dispatch ==============================

    private long get(@NotNull StoreId id, @NotNull CounterOps.GetOp<?> op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT value FROM " + table + " WHERE id = ?")) {
            bindKey(ps, 1, op.key());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong("value") : 0L;
            }
        }
    }

    private <K> Map<K, Long> getMany(@NotNull StoreId id, @NotNull CounterOps.GetManyOp<K> op) throws SQLException {
        Collection<K> keys = op.keys();
        if (keys.isEmpty()) return Map.of();
        Map<K, Long> out = new LinkedHashMap<>(keys.size() * 4 / 3 + 1);
        for (K k : keys) out.put(k, 0L);

        String table = dialect.quoteIdentifier(id.name());
        String placeholders = String.join(", ", keys.stream().map(k -> "?").toList());
        String sql = "SELECT id, value FROM " + table + " WHERE id IN (" + placeholders + ")";
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            int idx = 1;
            for (K k : keys) bindKey(ps, idx++, k);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    @SuppressWarnings("unchecked")
                    K key = (K) extractKey(rs, "id", keys.iterator().next().getClass());
                    if (key != null) out.put(key, rs.getLong("value"));
                }
            }
        }
        return out;
    }

    private long incrementBy(@NotNull StoreId id, @NotNull CounterOps.IncrementByOp<?> op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        if (dialect.usesLegacySqliteUpsert()) {
            return legacyIncrementBy(table, op.key(), op.delta());
        }
        String upsert = dialect.counterIncrementSql(table);
        if (dialect.supportsReturning()) {
            String sql = upsert + " RETURNING value";
            try (Connection c = ds.getConnection();
                 PreparedStatement ps = c.prepareStatement(sql)) {
                bindKey(ps, 1, op.key());
                ps.setLong(2, op.delta());
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getLong("value") : op.delta();
                }
            }
        }
        // No RETURNING (MySQL): upsert then SELECT in a transaction.
        try (Connection c = ds.getConnection()) {
            boolean prior = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                try (PreparedStatement ps = c.prepareStatement(upsert)) {
                    bindKey(ps, 1, op.key());
                    ps.setLong(2, op.delta());
                    ps.executeUpdate();
                }
                try (PreparedStatement sel = c.prepareStatement(
                        "SELECT value FROM " + table + " WHERE id = ?")) {
                    bindKey(sel, 1, op.key());
                    try (ResultSet rs = sel.executeQuery()) {
                        c.commit();
                        return rs.next() ? rs.getLong("value") : op.delta();
                    }
                }
            } catch (Exception e) {
                try { c.rollback(); } catch (SQLException ignored) {}
                throw e;
            } finally {
                c.setAutoCommit(prior);
            }
        }
    }

    private long setIfHigher(@NotNull StoreId id, @NotNull CounterOps.SetIfHigherOp<?> op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        if (dialect.usesLegacySqliteUpsert()) {
            return legacySetIfHigher(table, op.key(), op.value());
        }
        String valCol = dialect.quoteIdentifier("value");
        String greatest = dialect.greatestFn(table + "." + valCol, dialect.excludedRef(valCol));
        String upsert = dialect.counterIncrementSql(table)
            .replace(table + ".value + " + dialect.excludedRef(valCol), greatest);
        // The replace above adapts the increment upsert into a
        // set-if-higher upsert by swapping the UPDATE SET expression.
        // This is fragile but avoids adding yet another dialect method.
        // TODO: add a dedicated dialect.counterSetIfHigherSql() if this
        // breaks across vendors.

        // Actually, let's build it properly:
        String upsertFixed;
        if ("mysql".equals(dialect.name())) {
            upsertFixed = "INSERT INTO " + table + " (`id`, `value`) VALUES (?, ?) "
                + "ON DUPLICATE KEY UPDATE `value` = GREATEST(`value`, VALUES(`value`))";
        } else {
            String excl = dialect.excludedRef(valCol);
            upsertFixed = "INSERT INTO " + table + " (" + dialect.quoteIdentifier("id") + ", " + valCol + ") VALUES (?, ?) "
                + "ON CONFLICT (id) DO UPDATE SET " + valCol + " = " + dialect.greatestFn(table + "." + valCol, excl);
        }

        if (dialect.supportsReturning()) {
            String sql = upsertFixed + " RETURNING value";
            try (Connection c = ds.getConnection();
                 PreparedStatement ps = c.prepareStatement(sql)) {
                bindKey(ps, 1, op.key());
                ps.setLong(2, op.value());
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getLong("value") : op.value();
                }
            }
        }
        try (Connection c = ds.getConnection()) {
            boolean prior = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                try (PreparedStatement ps = c.prepareStatement(upsertFixed)) {
                    bindKey(ps, 1, op.key());
                    ps.setLong(2, op.value());
                    ps.executeUpdate();
                }
                try (PreparedStatement sel = c.prepareStatement(
                        "SELECT value FROM " + table + " WHERE id = ?")) {
                    bindKey(sel, 1, op.key());
                    try (ResultSet rs = sel.executeQuery()) {
                        c.commit();
                        return rs.next() ? rs.getLong("value") : op.value();
                    }
                }
            } catch (Exception e) {
                try { c.rollback(); } catch (SQLException ignored) {}
                throw e;
            } finally {
                c.setAutoCommit(prior);
            }
        }
    }

    private long legacyIncrementBy(@NotNull String table, @NotNull Object key, long delta) throws SQLException {
        try (Connection c = ds.getConnection()) {
            boolean prior = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                legacyInsertCounter(c, table, key, 0L);
                legacyAddCounter(c, table, key, delta);
                long value = selectCounterValue(c, table, key, delta);
                c.commit();
                return value;
            } catch (Exception e) {
                try { c.rollback(); } catch (SQLException ignored) {}
                throw e;
            } finally {
                c.setAutoCommit(prior);
            }
        }
    }

    private long legacySetIfHigher(@NotNull String table, @NotNull Object key, long value) throws SQLException {
        try (Connection c = ds.getConnection()) {
            boolean prior = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                legacyInsertCounter(c, table, key, value);
                legacyMaxCounter(c, table, key, value);
                long current = selectCounterValue(c, table, key, value);
                c.commit();
                return current;
            } catch (Exception e) {
                try { c.rollback(); } catch (SQLException ignored) {}
                throw e;
            } finally {
                c.setAutoCommit(prior);
            }
        }
    }

    private void legacyInsertCounter(
            @NotNull Connection c,
            @NotNull String table,
            @NotNull Object key,
            long value) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT OR IGNORE INTO " + table
                + " (" + dialect.quoteIdentifier("id") + ", " + dialect.quoteIdentifier("value") + ") VALUES (?, ?)")) {
            bindKey(ps, 1, key);
            ps.setLong(2, value);
            ps.executeUpdate();
        }
    }

    private void legacyAddCounter(
            @NotNull Connection c,
            @NotNull String table,
            @NotNull Object key,
            long delta) throws SQLException {
        String value = dialect.quoteIdentifier("value");
        try (PreparedStatement ps = c.prepareStatement("UPDATE " + table
                + " SET " + value + " = " + value + " + ? WHERE " + dialect.quoteIdentifier("id") + " = ?")) {
            ps.setLong(1, delta);
            bindKey(ps, 2, key);
            ps.executeUpdate();
        }
    }

    private void legacyMaxCounter(
            @NotNull Connection c,
            @NotNull String table,
            @NotNull Object key,
            long incoming) throws SQLException {
        String value = dialect.quoteIdentifier("value");
        try (PreparedStatement ps = c.prepareStatement("UPDATE " + table
                + " SET " + value + " = MAX(" + value + ", ?) WHERE " + dialect.quoteIdentifier("id") + " = ?")) {
            ps.setLong(1, incoming);
            bindKey(ps, 2, key);
            ps.executeUpdate();
        }
    }

    private long selectCounterValue(
            @NotNull Connection c,
            @NotNull String table,
            @NotNull Object key,
            long fallback) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT value FROM " + table
                + " WHERE " + dialect.quoteIdentifier("id") + " = ?")) {
            bindKey(ps, 1, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong("value") : fallback;
            }
        }
    }

    // ============================== key bind/extract ==============================

    private static void bindKey(@NotNull PreparedStatement ps, int idx,
                                @NotNull Object key) throws SQLException {
        if (key instanceof UUID u) { ps.setObject(idx, u); return; }
        if (key instanceof String s) { ps.setString(idx, s); return; }
        if (key instanceof Long l) { ps.setLong(idx, l); return; }
        if (key instanceof Integer i) { ps.setInt(idx, i); return; }
        if (key instanceof byte[] b) { ps.setBytes(idx, b); return; }
        if (key instanceof Enum<?> e) { ps.setInt(idx, e.ordinal()); return; }
        throw new SQLException("unsupported counter key type: " + key.getClass().getName());
    }

    private static @Nullable Object extractKey(@NotNull ResultSet rs, @NotNull String col,
                                               @NotNull Class<?> keyType) throws SQLException {
        if (keyType == UUID.class) return rs.getObject(col, UUID.class);
        if (keyType == String.class) return rs.getString(col);
        if (keyType == Long.class || keyType == long.class) { long v = rs.getLong(col); return rs.wasNull() ? null : v; }
        if (keyType == Integer.class || keyType == int.class) { int v = rs.getInt(col); return rs.wasNull() ? null : v; }
        if (keyType == byte[].class) return rs.getBytes(col);
        return rs.getObject(col);
    }

    private @NotNull String keyColumnType(@NotNull Class<?> keyType) {
        if (keyType == UUID.class) return dialect.name().equals("postgres") ? "UUID" : "BINARY(16)";
        if (keyType == String.class) return "VARCHAR(255)";
        if (keyType == Long.class || keyType == long.class) return "BIGINT";
        if (keyType == Integer.class || keyType == int.class) return "INTEGER";
        if (keyType == byte[].class) return dialect.name().equals("postgres") ? "BYTEA" : "BLOB";
        if (keyType.isEnum()) return "INTEGER";
        throw new IllegalArgumentException("unsupported counter key type: " + keyType.getName());
    }
}
