package ac.grim.grimac.internal.storage.backend.mysql.v2;

import ac.grim.grimac.api.storage.backend.*;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.kind.Counter;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendConfig;
import ac.grim.grimac.internal.storage.backend.sql.v2.SqlEntityAdapter;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.MysqlDialect;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * MySQL v2 backend. Uses HikariCP + {@link MysqlDialect} + the shared
 * SQL v2 adapters. Currently wires Entity only — Counter/KV/EventStream
 * adapters have Postgres-specific SQL (RETURNING, EXCLUDED) and need
 * dialect methods before MySQL can use them.
 */
@ApiStatus.Internal
public final class MysqlBackendV2 implements BackendV2 {

    private static final int DEFAULT_MAX_POOL_SIZE = 10;
    private final @NotNull MysqlBackendConfig config;
    private final @NotNull MysqlDialect dialect = new MysqlDialect();
    private Logger logger;
    private HikariDataSource ds;
    private SqlEntityAdapter entityAdapter;
    private ac.grim.grimac.internal.storage.backend.sql.v2.SqlEventStreamAdapter eventStreamAdapter;
    private ac.grim.grimac.internal.storage.backend.sql.v2.SqlKeyValueScopedAdapter kvScopedAdapter;
    private ac.grim.grimac.internal.storage.backend.sql.v2.SqlCounterAdapter counterAdapter;

    public MysqlBackendV2(@NotNull MysqlBackendConfig config) {
        this.config = config;
    }

    @Override public @NotNull String id() { return "mysql-v2"; }
    @Override public @NotNull ApiVersion apiVersion() { return ApiVersion.CURRENT; }
    @Override public int writerThreads(@NotNull ac.grim.grimac.api.storage.category.Category<?> category) {
        return config.writerThreadsFor(category.id());
    }
    @Override public @NotNull EnumSet<Capability> capabilities() {
        return EnumSet.of(Capability.KIND_ENTITY, Capability.KIND_EVENT_STREAM,
            Capability.KIND_KV_SCOPED, Capability.KIND_COUNTER, Capability.ATOMIC_UPSERT);
    }

    @Override
    public void init(@NotNull BackendContext ctx) throws BackendException {
        this.logger = ctx.logger();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Class.forName("com.zaxxer.hikari.HikariDataSource");
        } catch (ClassNotFoundException cnf) {
            throw new BackendException("MySQL JDBC driver or HikariCP missing: " + cnf.getMessage(), cnf);
        }
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(config.jdbcUrl());
        hc.setUsername(config.user());
        if (config.password() != null) hc.setPassword(config.password());
        hc.setMaximumPoolSize(DEFAULT_MAX_POOL_SIZE);
        hc.setPoolName("grim-mysql-v2");
        hc.setConnectionTimeout(5_000L);
        try {
            this.ds = new HikariDataSource(hc);
        } catch (RuntimeException e) {
            throw new BackendException("failed to init MySQL pool", e);
        }
        try (Connection probe = ds.getConnection()) {
            if (!probe.isValid(5)) throw new BackendException("MySQL probe invalid", null);
        } catch (SQLException e) {
            ds.close();
            throw new BackendException("MySQL probe failed", e);
        }
        this.entityAdapter = new SqlEntityAdapter(ds, dialect, logger);
        this.eventStreamAdapter = new ac.grim.grimac.internal.storage.backend.sql.v2.SqlEventStreamAdapter(ds, dialect, logger);
        this.kvScopedAdapter = new ac.grim.grimac.internal.storage.backend.sql.v2.SqlKeyValueScopedAdapter(ds, dialect, logger);
        this.counterAdapter = new ac.grim.grimac.internal.storage.backend.sql.v2.SqlCounterAdapter(ds, dialect, logger);
    }

    @Override public void flush() {}
    @Override public void close() throws BackendException {
        if (ds != null) { try { ds.close(); } catch (RuntimeException e) { throw new BackendException("MySQL close failed", e); } }
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
        if (type.isAssignableFrom(javax.sql.DataSource.class) && ds != null) return Optional.of((X) ds);
        return Optional.empty();
    }
}
