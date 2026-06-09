package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.KeyValueEvent;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.KeyValueScopedOps;
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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * SQL adapter for {@link KeyValueScoped} stores. Normalized relational
 * layout: one row per {@code (scope, scope_key, key)} triple, with the
 * value stored as a binary column ({@code BLOB} on MySQL / SQLite,
 * {@code BYTEA} on Postgres). Mirrors the Mongo v7 per-tenant envelope
 * semantics but uses a flat table instead of a nested sub-document.
 *
 * <p>Table shape: {@code (scope VARCHAR, scope_key VARCHAR, key VARCHAR,
 * value <binary>, updated_at BIGINT, PRIMARY KEY (scope, scope_key, key))}.
 * Values round-trip arbitrary {@code byte[]} payloads opaquely — KV
 * adapters never invoke the kind's {@code valueCodec} on this path, the
 * caller (or the {@code translateV2Result} bridge) does its own
 * encoding.
 */
@ApiStatus.Internal
public final class SqlKeyValueScopedAdapter implements KindAdapter<KeyValueScoped<?, ?>> {

    private final @NotNull DataSource ds;
    private final @NotNull SqlDialect dialect;
    private final @NotNull Logger logger;

    public SqlKeyValueScopedAdapter(@NotNull DataSource ds, @NotNull SqlDialect dialect,
                                    @NotNull Logger logger) {
        this.ds = ds;
        this.dialect = dialect;
        this.logger = logger;
    }

    @SuppressWarnings("unchecked")
    @Override public @NotNull Class<KeyValueScoped<?, ?>> kindType() {
        return (Class<KeyValueScoped<?, ?>>) (Class<?>) KeyValueScoped.class;
    }

    @Override public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_KV_SCOPED, Capability.ATOMIC_UPSERT);
    }

    @Override
    public void ensureStore(@NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind) throws BackendException {
        String table = dialect.quoteIdentifier(id.name());
        String scope = dialect.quoteIdentifier("scope");
        String scopeKey = dialect.quoteIdentifier("scope_key");
        String key = dialect.quoteIdentifier("key");
        String value = dialect.quoteIdentifier("value");
        String updatedAt = dialect.quoteIdentifier("updated_at");
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " ("
            + scope + " VARCHAR(64) NOT NULL, "
            + scopeKey + " VARCHAR(255) NOT NULL, "
            + key + " VARCHAR(255) NOT NULL, "
            + value + " " + dialect.kvValueColumnType() + ", "
            + updatedAt + " BIGINT NOT NULL DEFAULT 0, "
            + "PRIMARY KEY (" + scope + ", " + scopeKey + ", " + key + "))";
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate(ddl);
        } catch (SQLException e) {
            throw new BackendException("failed to ensure KV table " + id, e);
        }
    }

    @Override
    public void dropStore(@NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind) throws BackendException {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("DROP TABLE IF EXISTS " + dialect.quoteIdentifier(id.name()));
        } catch (SQLException e) {
            throw new BackendException("failed to drop KV table " + id, e);
        }
    }

    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind, @NotNull Category<E> category) {
        String table = dialect.quoteIdentifier(id.name());
        String upsertSql = dialect.kvUpsertSql(table);
        String deleteSql = "DELETE FROM " + table
            + " WHERE " + dialect.quoteIdentifier("scope") + " = ?"
            + " AND " + dialect.quoteIdentifier("scope_key") + " = ?"
            + " AND " + dialect.quoteIdentifier("key") + " = ?";
        if (dialect.usesLegacySqliteUpsert()) {
            return new LegacyKVWriteHandler<>(legacyKvInsertSql(table), legacyKvUpdateSql(table), deleteSql);
        }
        return new KVWriteHandler<>(upsertSql, deleteSql);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind,
                         @NotNull Operation<R> op) throws BackendException {
        try {
            if (op instanceof KeyValueScopedOps.GetOp<?, ?> g)       return (R) get(id, g);
            if (op instanceof KeyValueScopedOps.GetAllOp<?, ?> g)    return (R) getAll(id, g);
            if (op instanceof KeyValueScopedOps.PutOp<?, ?> p)       { put(id, p); return null; }
            if (op instanceof KeyValueScopedOps.PutAllOp<?, ?> p)    { putAll(id, p); return null; }
            if (op instanceof KeyValueScopedOps.RemoveOp<?> r)       { remove(id, r); return null; }
            if (op instanceof KeyValueScopedOps.RemoveAllOp<?> r)    { removeAll(id, r); return null; }
            if (op instanceof KeyValueScopedOps.CountOp<?> c)        return (R) Long.valueOf(count(id, c));
            throw new UnsupportedOperationException(
                "SqlKeyValueScopedAdapter does not handle " + op.getClass().getName());
        } catch (SQLException e) {
            throw new BackendException("sql kv execute failed for " + op.getClass().getSimpleName(), e);
        }
    }

    @Override
    public @NotNull List<Migration<KeyValueScoped<?, ?>>> migrations(@NotNull KeyValueScoped<?, ?> kind) {
        return List.of();
    }

    // ============================== writeHandler ==============================

    private final class KVWriteHandler<E> implements StorageEventHandler<E> {
        private final String upsertSql;
        private final String deleteSql;
        KVWriteHandler(String upsertSql, String deleteSql) {
            this.upsertSql = upsertSql;
            this.deleteSql = deleteSql;
        }

        @Override
        public void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            KeyValueEvent<?, ?> kve = (KeyValueEvent<?, ?>) event;
            if (kve.scope == null || kve.scopeKey == null || kve.key == null) return;
            try (Connection c = ds.getConnection()) {
                if (kve.remove) {
                    try (PreparedStatement ps = c.prepareStatement(deleteSql)) {
                        ps.setString(1, String.valueOf(kve.scope));
                        ps.setString(2, String.valueOf(kve.scopeKey));
                        ps.setString(3, kve.key);
                        ps.executeUpdate();
                    }
                } else {
                    if (kve.value == null) {
                        throw new BackendException("KV PutOp value must be non-null; use remove flag for deletion");
                    }
                    try (PreparedStatement ps = c.prepareStatement(upsertSql)) {
                        ps.setString(1, String.valueOf(kve.scope));
                        ps.setString(2, String.valueOf(kve.scopeKey));
                        ps.setString(3, kve.key);
                        ps.setBytes(4, valueAsBytes(kve.value));
                        ps.setLong(5, kve.updatedEpochMs);
                        ps.executeUpdate();
                    }
                }
            } catch (SQLException e) {
                throw new BackendException("kv write failed", e);
            }
        }
    }

    private final class LegacyKVWriteHandler<E> implements StorageEventHandler<E> {
        private final String insertSql;
        private final String updateSql;
        private final String deleteSql;

        LegacyKVWriteHandler(String insertSql, String updateSql, String deleteSql) {
            this.insertSql = insertSql;
            this.updateSql = updateSql;
            this.deleteSql = deleteSql;
        }

        @Override
        public void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            KeyValueEvent<?, ?> kve = (KeyValueEvent<?, ?>) event;
            if (kve.scope == null || kve.scopeKey == null || kve.key == null) return;
            try (Connection c = ds.getConnection()) {
                if (kve.remove) {
                    try (PreparedStatement ps = c.prepareStatement(deleteSql)) {
                        ps.setString(1, String.valueOf(kve.scope));
                        ps.setString(2, String.valueOf(kve.scopeKey));
                        ps.setString(3, kve.key);
                        ps.executeUpdate();
                    }
                    return;
                }
                if (kve.value == null) {
                    throw new BackendException("KV PutOp value must be non-null; use remove flag for deletion");
                }
                boolean priorAutoCommit = c.getAutoCommit();
                c.setAutoCommit(false);
                try {
                    legacyKvUpsert(c, insertSql, updateSql, String.valueOf(kve.scope),
                        String.valueOf(kve.scopeKey), kve.key, valueAsBytes(kve.value), kve.updatedEpochMs);
                    c.commit();
                } catch (Exception e) {
                    try { c.rollback(); } catch (SQLException ignored) {}
                    throw e;
                } finally {
                    c.setAutoCommit(priorAutoCommit);
                }
            } catch (SQLException e) {
                throw new BackendException("kv write failed", e);
            }
        }
    }

    /**
     * Coerce a KV slot's value to bytes for {@code setBytes}. byte[]
     * passes through; String falls back to UTF-8 so legacy callers
     * (extensions that still pass Strings) keep working. Other types
     * are rejected loudly — KV is binary-by-contract; a quietly-coerced
     * {@code toString()} would round-trip as opaque bytes the reader
     * can't reinterpret.
     */
    private static byte @NotNull [] valueAsBytes(@NotNull Object value) throws BackendException {
        if (value instanceof byte[] b) return b;
        if (value instanceof String s)
            return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        throw new BackendException("KV value must be byte[] or String, got " + value.getClass().getName());
    }

    private @NotNull String legacyKvInsertSql(@NotNull String table) {
        String scope = dialect.quoteIdentifier("scope");
        String scopeKey = dialect.quoteIdentifier("scope_key");
        String key = dialect.quoteIdentifier("key");
        String value = dialect.quoteIdentifier("value");
        String updatedAt = dialect.quoteIdentifier("updated_at");
        return "INSERT OR IGNORE INTO " + table + " (" + scope + ", " + scopeKey + ", " + key
            + ", " + value + ", " + updatedAt + ") VALUES (?, ?, ?, ?, ?)";
    }

    private @NotNull String legacyKvUpdateSql(@NotNull String table) {
        String scope = dialect.quoteIdentifier("scope");
        String scopeKey = dialect.quoteIdentifier("scope_key");
        String key = dialect.quoteIdentifier("key");
        String value = dialect.quoteIdentifier("value");
        String updatedAt = dialect.quoteIdentifier("updated_at");
        return "UPDATE " + table + " SET " + value + " = ?, " + updatedAt + " = ?"
            + " WHERE " + scope + " = ? AND " + scopeKey + " = ? AND " + key + " = ?";
    }

    private void legacyKvUpsert(
            @NotNull Connection c,
            @NotNull String insertSql,
            @NotNull String updateSql,
            @NotNull String scope,
            @NotNull String scopeKey,
            @NotNull String key,
            byte @NotNull [] value,
            long updatedAt) throws SQLException {
        try (PreparedStatement insert = c.prepareStatement(insertSql)) {
            insert.setString(1, scope);
            insert.setString(2, scopeKey);
            insert.setString(3, key);
            insert.setBytes(4, value);
            insert.setLong(5, updatedAt);
            insert.executeUpdate();
        }
        try (PreparedStatement update = c.prepareStatement(updateSql)) {
            update.setBytes(1, value);
            update.setLong(2, updatedAt);
            update.setString(3, scope);
            update.setString(4, scopeKey);
            update.setString(5, key);
            update.executeUpdate();
        }
    }

    // ============================== execute dispatch ==============================

    private Optional<Object> get(@NotNull StoreId id, @NotNull KeyValueScopedOps.GetOp<?, ?> op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT " + dialect.quoteIdentifier("value") + " FROM " + table
                     + " WHERE " + dialect.quoteIdentifier("scope") + " = ?"
                     + " AND " + dialect.quoteIdentifier("scope_key") + " = ?"
                     + " AND " + dialect.quoteIdentifier("key") + " = ?")) {
            ps.setString(1, String.valueOf(op.scope()));
            ps.setString(2, String.valueOf(op.scopeKey()));
            ps.setString(3, op.key());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                byte[] v = rs.getBytes(1);
                return v == null ? Optional.empty() : Optional.of(v);
            }
        }
    }

    private @NotNull Map<String, Object> getAll(@NotNull StoreId id,
                                                @NotNull KeyValueScopedOps.GetAllOp<?, ?> op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        Map<String, Object> out = new LinkedHashMap<>();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT " + dialect.quoteIdentifier("key") + ", " + dialect.quoteIdentifier("value")
                     + " FROM " + table
                     + " WHERE " + dialect.quoteIdentifier("scope") + " = ?"
                     + " AND " + dialect.quoteIdentifier("scope_key") + " = ?")) {
            ps.setString(1, String.valueOf(op.scope()));
            ps.setString(2, String.valueOf(op.scopeKey()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    byte[] v = rs.getBytes(2);
                    if (v != null) out.put(rs.getString(1), v);
                }
            }
        }
        return out;
    }

    private void put(@NotNull StoreId id, @NotNull KeyValueScopedOps.PutOp<?, ?> op) throws SQLException, BackendException {
        if (op.value() == null) {
            throw new IllegalArgumentException("KV PutOp value must be non-null; use RemoveOp for deletion");
        }
        String table = dialect.quoteIdentifier(id.name());
        if (dialect.usesLegacySqliteUpsert()) {
            try (Connection c = ds.getConnection()) {
                boolean prior = c.getAutoCommit();
                c.setAutoCommit(false);
                try {
                    legacyKvUpsert(c, legacyKvInsertSql(table), legacyKvUpdateSql(table),
                        String.valueOf(op.scope()), op.scopeKey(), op.key(),
                        valueAsBytes(op.value()), System.currentTimeMillis());
                    c.commit();
                } catch (Exception e) {
                    try { c.rollback(); } catch (SQLException ignored) {}
                    throw e;
                } finally {
                    c.setAutoCommit(prior);
                }
            }
            return;
        }
        String sql = dialect.kvUpsertSql(table);
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, String.valueOf(op.scope()));
            ps.setString(2, op.scopeKey());
            ps.setString(3, op.key());
            ps.setBytes(4, valueAsBytes(op.value()));
            ps.setLong(5, System.currentTimeMillis());
            ps.executeUpdate();
        }
    }

    private void putAll(@NotNull StoreId id, @NotNull KeyValueScopedOps.PutAllOp<?, ?> op) throws SQLException, BackendException {
        String table = dialect.quoteIdentifier(id.name());
        String sql = dialect.kvUpsertSql(table);
        String legacyInsert = legacyKvInsertSql(table);
        String legacyUpdate = legacyKvUpdateSql(table);
        long now = System.currentTimeMillis();
        try (Connection c = ds.getConnection()) {
            boolean priorAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                if (dialect.usesLegacySqliteUpsert()) {
                    for (Map.Entry<String, ?> e : op.values().entrySet()) {
                        if (e.getValue() == null) {
                            throw new IllegalArgumentException("KV PutAllOp value for key '" + e.getKey() + "' must be non-null");
                        }
                        legacyKvUpsert(c, legacyInsert, legacyUpdate, String.valueOf(op.scope()),
                            op.scopeKey(), e.getKey(), valueAsBytes(e.getValue()), now);
                    }
                } else {
                    try (PreparedStatement ps = c.prepareStatement(sql)) {
                        for (Map.Entry<String, ?> e : op.values().entrySet()) {
                            if (e.getValue() == null) {
                                throw new IllegalArgumentException("KV PutAllOp value for key '" + e.getKey() + "' must be non-null");
                            }
                            ps.setString(1, String.valueOf(op.scope()));
                            ps.setString(2, op.scopeKey());
                            ps.setString(3, e.getKey());
                            ps.setBytes(4, valueAsBytes(e.getValue()));
                            ps.setLong(5, now);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }
                }
                c.commit();
            } catch (Exception e) {
                try { c.rollback(); } catch (SQLException ignored) {}
                throw e;
            } finally {
                c.setAutoCommit(priorAutoCommit);
            }
        }
    }

    private void remove(@NotNull StoreId id, @NotNull KeyValueScopedOps.RemoveOp<?> op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "DELETE FROM " + table
                     + " WHERE " + dialect.quoteIdentifier("scope") + " = ?"
                     + " AND " + dialect.quoteIdentifier("scope_key") + " = ?"
                     + " AND " + dialect.quoteIdentifier("key") + " = ?")) {
            ps.setString(1, String.valueOf(op.scope()));
            ps.setString(2, String.valueOf(op.scopeKey()));
            ps.setString(3, op.key());
            ps.executeUpdate();
        }
    }

    private void removeAll(@NotNull StoreId id, @NotNull KeyValueScopedOps.RemoveAllOp<?> op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "DELETE FROM " + table
                     + " WHERE " + dialect.quoteIdentifier("scope") + " = ?"
                     + " AND " + dialect.quoteIdentifier("scope_key") + " = ?")) {
            ps.setString(1, String.valueOf(op.scope()));
            ps.setString(2, String.valueOf(op.scopeKey()));
            ps.executeUpdate();
        }
    }

    private long count(@NotNull StoreId id, @NotNull KeyValueScopedOps.CountOp<?> op) throws SQLException {
        String table = dialect.quoteIdentifier(id.name());
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT COUNT(*) FROM " + table
                     + " WHERE " + dialect.quoteIdentifier("scope") + " = ?"
                     + " AND " + dialect.quoteIdentifier("scope_key") + " = ?")) {
            ps.setString(1, String.valueOf(op.scope()));
            ps.setString(2, String.valueOf(op.scopeKey()));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }
}
