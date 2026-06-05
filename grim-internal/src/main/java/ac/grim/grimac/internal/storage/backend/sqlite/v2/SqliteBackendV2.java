package ac.grim.grimac.internal.storage.backend.sqlite.v2;

import ac.grim.grimac.api.storage.backend.*;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.kind.Counter;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;
import ac.grim.grimac.internal.storage.backend.sql.v2.SqlEntityAdapter;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqliteDialect;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * SQLite v2 backend. Single-file database; no connection pool needed
 * (SQLite is single-writer). Uses the shared SQL v2 adapters with
 * {@link SqliteDialect}.
 *
 * <p>Currently wires Entity only — Counter/KV/EventStream adapters have
 * Postgres-specific SQL (RETURNING, EXCLUDED) and need dialect methods
 * before SQLite can use them. SQLite 3.35+ supports RETURNING; SQLite
 * uses {@code excluded.} (lowercase) which matches the SQLite upsert
 * clause. These are follow-up dialect refinements.
 */
@ApiStatus.Internal
public final class SqliteBackendV2 implements BackendV2 {

    private final @NotNull SqliteBackendConfig config;
    private final @NotNull SqliteDialect dialect = new SqliteDialect();
    private Logger logger;
    private Connection connection;
    private javax.sql.DataSource singleConnDs;
    private SqlEntityAdapter entityAdapter;
    private ac.grim.grimac.internal.storage.backend.sql.v2.SqlEventStreamAdapter eventStreamAdapter;
    private ac.grim.grimac.internal.storage.backend.sql.v2.SqlKeyValueScopedAdapter kvScopedAdapter;
    private ac.grim.grimac.internal.storage.backend.sql.v2.SqlCounterAdapter counterAdapter;

    public SqliteBackendV2(@NotNull SqliteBackendConfig config) {
        this.config = config;
    }

    @Override public @NotNull String id() { return "sqlite-v2"; }
    @Override public @NotNull ApiVersion apiVersion() { return ApiVersion.CURRENT; }
    @Override
    public int writerThreads(@NotNull ac.grim.grimac.api.storage.category.Category<?> category) {
        return 1; // SQLite is single-writer — not configurable
    }

    @Override public @NotNull EnumSet<Capability> capabilities() {
        return EnumSet.of(Capability.KIND_ENTITY, Capability.KIND_EVENT_STREAM,
            Capability.KIND_KV_SCOPED, Capability.KIND_COUNTER);
    }

    @Override
    public void init(@NotNull BackendContext ctx) throws BackendException {
        this.logger = ctx.logger();
        Path dbFile = ctx.dataDirectory().resolve(config.path());
        String jdbcUrl = "jdbc:sqlite:" + dbFile.toAbsolutePath();
        try {
            this.connection = DriverManager.getConnection(jdbcUrl);
            try (Statement s = connection.createStatement()) {
                s.execute("PRAGMA journal_mode=WAL");
            }
            // High busy_timeout so the v2 connection waits for the
            // legacy backend's connection to release the write lock
            // instead of failing immediately with SQLITE_BUSY. WAL
            // mode allows concurrent reads, so the contention is only
            // on writes — and both backends should never write
            // simultaneously once Layer 1 routes v2-category writes
            // exclusively through the v2 writeHandler.
            try (Statement s = connection.createStatement()) {
                s.execute("PRAGMA busy_timeout=30000");
            }
        } catch (SQLException e) {
            throw new BackendException("failed to open SQLite database " + dbFile, e);
        }
        this.singleConnDs = new SingleConnectionDataSource(connection);
        this.entityAdapter = new SqlEntityAdapter(singleConnDs, dialect, logger);
        this.eventStreamAdapter = new ac.grim.grimac.internal.storage.backend.sql.v2.SqlEventStreamAdapter(singleConnDs, dialect, logger);
        this.kvScopedAdapter = new ac.grim.grimac.internal.storage.backend.sql.v2.SqlKeyValueScopedAdapter(singleConnDs, dialect, logger);
        this.counterAdapter = new ac.grim.grimac.internal.storage.backend.sql.v2.SqlCounterAdapter(singleConnDs, dialect, logger);
    }

    @Override public void flush() {}
    @Override public void close() throws BackendException {
        if (connection != null) {
            try { connection.close(); }
            catch (SQLException e) { throw new BackendException("SQLite close failed", e); }
        }
    }

    @Override @SuppressWarnings({"unchecked", "rawtypes"})
    public <K extends DataKind<?, ?>> @NotNull Optional<KindAdapter<K>> adapterFor(@NotNull K kind) {
        if (kind instanceof Entity<?, ?, ?>)         return Optional.of((KindAdapter) entityAdapter);
        if (kind instanceof EventStream<?, ?>)       return Optional.of((KindAdapter) eventStreamAdapter);
        if (kind instanceof KeyValueScoped<?, ?>)    return Optional.of((KindAdapter) kvScopedAdapter);
        if (kind instanceof Counter<?>)              return Optional.of((KindAdapter) counterAdapter);
        return Optional.empty();
    }
    @Override @SuppressWarnings("unchecked")
    public <X> @NotNull Optional<X> unwrap(@NotNull Class<X> type) {
        if (type.isAssignableFrom(javax.sql.DataSource.class) && singleConnDs != null) return Optional.of((X) singleConnDs);
        return Optional.empty();
    }

    /**
     * Wraps a single persistent Connection as a DataSource. SQLite is
     * single-writer — concurrency is handled by the RingRegistry's
     * single-writer executor (writerThreads == 1), so only ONE thread
     * ever calls getConnection at a time. No locks needed.
     */
    private record SingleConnectionDataSource(Connection conn) implements javax.sql.DataSource {
        @Override public Connection getConnection() { return new NonClosingConnection(conn); }
        @Override public Connection getConnection(String u, String p) { return new NonClosingConnection(conn); }
        @Override public java.io.PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(java.io.PrintWriter out) {}
        @Override public void setLoginTimeout(int seconds) {}
        @Override public int getLoginTimeout() { return 0; }
        @Override public java.util.logging.Logger getParentLogger() { return java.util.logging.Logger.getGlobal(); }
        @Override public <T> T unwrap(Class<T> iface) { return null; }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }

    /**
     * Delegates everything to the wrapped connection. close() is a
     * no-op so try-with-resources in adapters doesn't close the shared
     * SQLite connection. Concurrency is handled externally by the
     * RingRegistry's single-writer executor — this wrapper doesn't
     * need any synchronization.
     */
    private static final class NonClosingConnection implements Connection {
        private final Connection delegate;
        NonClosingConnection(Connection delegate) { this.delegate = delegate; }
        @Override public void close() { /* no-op */ }
        @Override public Statement createStatement() throws SQLException { return delegate.createStatement(); }
        @Override public PreparedStatement prepareStatement(String sql) throws SQLException { return delegate.prepareStatement(sql); }
        @Override public boolean getAutoCommit() throws SQLException { return delegate.getAutoCommit(); }
        @Override public void setAutoCommit(boolean a) throws SQLException { delegate.setAutoCommit(a); }
        @Override public void commit() throws SQLException { delegate.commit(); }
        @Override public void rollback() throws SQLException { delegate.rollback(); }
        @Override public boolean isClosed() throws SQLException { return delegate.isClosed(); }
        @Override public boolean isValid(int timeout) throws SQLException { return delegate.isValid(timeout); }
        // Remaining Connection methods delegate through. For brevity,
        // only the methods actually called by the SQL adapters are
        // explicitly listed above. If an adapter calls an unlisted
        // method, it will hit AbstractMethodError at runtime — add it
        // here when that happens. A full delegation proxy (e.g. via
        // java.lang.reflect.Proxy or a code generator) is overkill for
        // a single-user SQLite backend.
        @Override public java.sql.DatabaseMetaData getMetaData() throws SQLException { return delegate.getMetaData(); }
        @Override public void setReadOnly(boolean r) throws SQLException { delegate.setReadOnly(r); }
        @Override public boolean isReadOnly() throws SQLException { return delegate.isReadOnly(); }
        @Override public void setCatalog(String c) throws SQLException { delegate.setCatalog(c); }
        @Override public String getCatalog() throws SQLException { return delegate.getCatalog(); }
        @Override public void setTransactionIsolation(int l) throws SQLException { delegate.setTransactionIsolation(l); }
        @Override public int getTransactionIsolation() throws SQLException { return delegate.getTransactionIsolation(); }
        @Override public java.sql.SQLWarning getWarnings() throws SQLException { return delegate.getWarnings(); }
        @Override public void clearWarnings() throws SQLException { delegate.clearWarnings(); }
        @Override public Statement createStatement(int t, int c) throws SQLException { return delegate.createStatement(t, c); }
        @Override public PreparedStatement prepareStatement(String s, int t, int c) throws SQLException { return delegate.prepareStatement(s, t, c); }
        @Override public java.sql.CallableStatement prepareCall(String s) throws SQLException { return delegate.prepareCall(s); }
        @Override public java.sql.CallableStatement prepareCall(String s, int t, int c) throws SQLException { return delegate.prepareCall(s, t, c); }
        @Override public String nativeSQL(String s) throws SQLException { return delegate.nativeSQL(s); }
        @Override public java.util.Map<String, Class<?>> getTypeMap() throws SQLException { return delegate.getTypeMap(); }
        @Override public void setTypeMap(java.util.Map<String, Class<?>> m) throws SQLException { delegate.setTypeMap(m); }
        @Override public void setHoldability(int h) throws SQLException { delegate.setHoldability(h); }
        @Override public int getHoldability() throws SQLException { return delegate.getHoldability(); }
        @Override public java.sql.Savepoint setSavepoint() throws SQLException { return delegate.setSavepoint(); }
        @Override public java.sql.Savepoint setSavepoint(String n) throws SQLException { return delegate.setSavepoint(n); }
        @Override public void rollback(java.sql.Savepoint s) throws SQLException { delegate.rollback(s); }
        @Override public void releaseSavepoint(java.sql.Savepoint s) throws SQLException { delegate.releaseSavepoint(s); }
        @Override public Statement createStatement(int t, int c, int h) throws SQLException { return delegate.createStatement(t, c, h); }
        @Override public PreparedStatement prepareStatement(String s, int t, int c, int h) throws SQLException { return delegate.prepareStatement(s, t, c, h); }
        @Override public java.sql.CallableStatement prepareCall(String s, int t, int c, int h) throws SQLException { return delegate.prepareCall(s, t, c, h); }
        @Override public PreparedStatement prepareStatement(String s, int f) throws SQLException { return delegate.prepareStatement(s, f); }
        @Override public PreparedStatement prepareStatement(String s, int[] i) throws SQLException { return delegate.prepareStatement(s, i); }
        @Override public PreparedStatement prepareStatement(String s, String[] n) throws SQLException { return delegate.prepareStatement(s, n); }
        @Override public java.sql.Clob createClob() throws SQLException { return delegate.createClob(); }
        @Override public java.sql.Blob createBlob() throws SQLException { return delegate.createBlob(); }
        @Override public java.sql.NClob createNClob() throws SQLException { return delegate.createNClob(); }
        @Override public java.sql.SQLXML createSQLXML() throws SQLException { return delegate.createSQLXML(); }
        @Override public void setClientInfo(String k, String v) throws java.sql.SQLClientInfoException { delegate.setClientInfo(k, v); }
        @Override public void setClientInfo(java.util.Properties p) throws java.sql.SQLClientInfoException { delegate.setClientInfo(p); }
        @Override public String getClientInfo(String n) throws SQLException { return delegate.getClientInfo(n); }
        @Override public java.util.Properties getClientInfo() throws SQLException { return delegate.getClientInfo(); }
        @Override public java.sql.Array createArrayOf(String t, Object[] e) throws SQLException { return delegate.createArrayOf(t, e); }
        @Override public java.sql.Struct createStruct(String t, Object[] a) throws SQLException { return delegate.createStruct(t, a); }
        @Override public void setSchema(String s) throws SQLException { delegate.setSchema(s); }
        @Override public String getSchema() throws SQLException { return delegate.getSchema(); }
        @Override public void abort(java.util.concurrent.Executor e) throws SQLException { delegate.abort(e); }
        @Override public void setNetworkTimeout(java.util.concurrent.Executor e, int ms) throws SQLException { delegate.setNetworkTimeout(e, ms); }
        @Override public int getNetworkTimeout() throws SQLException { return delegate.getNetworkTimeout(); }
        @Override public <T> T unwrap(Class<T> i) throws SQLException { return delegate.unwrap(i); }
        @Override public boolean isWrapperFor(Class<?> i) throws SQLException { return delegate.isWrapperFor(i); }
    }
}
