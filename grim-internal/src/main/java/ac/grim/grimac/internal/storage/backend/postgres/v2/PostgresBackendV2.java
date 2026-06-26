package ac.grim.grimac.internal.storage.backend.postgres.v2;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.AdminAdapter;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.SearchAdapter;
import ac.grim.grimac.api.storage.backend.TxAdapter;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.kind.Counter;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.internal.storage.backend.postgres.PostgresBackendConfig;
import ac.grim.grimac.internal.storage.backend.sql.v2.SqlEntityAdapter;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.PostgresDialect;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Narrow Postgres {@link BackendV2}: owns the JDBC DataSource lifecycle
 * + capability advertisement; per-Kind work delegates to the v2
 * {@link KindAdapter}s. Coexists with the legacy {@code PostgresBackend}
 * until the v2 wiring (Phase 1.4c) flips DataStore over.
 *
 * <p>Connection pooling: backed by HikariCP. The pool is sized small
 * by default (5 max connections) since the v2 adapter SPI's per-event
 * acquire pattern releases connections quickly back to the pool; a
 * busy disruptor handler thread typically holds at most one. Operators
 * who want a larger pool can override via JDBC params or a future
 * config knob (the connection-count limits live on the Postgres
 * server anyway).
 *
 * <p>Currently wires:
 * <ul>
 *   <li>{@link SqlEntityAdapter} with the {@link PostgresDialect}.</li>
 *   <li>EventStream / KeyValueScoped / Counter / Blob — empty
 *       {@link Optional}, indicating the kind isn't supported yet.
 *       Each sub-phase adds them.</li>
 * </ul>
 *
 * <p>The legacy {@code PostgresBackend} stays in place; this is purely
 * additive until {@code DataStoreLifecycle} (Phase 1.4c) chooses
 * between the two based on the user-facing config.
 */
@ApiStatus.Internal
public final class PostgresBackendV2 implements BackendV2 {

    /** Default pool size; conservative — Postgres' connection limit is small. */
    private static final int DEFAULT_MAX_POOL_SIZE = 5;
    /** Pool name prefix surfaced in HikariCP's MBean + logs. */
    private static final String POOL_NAME = "grim-postgres-v2";

    private final @NotNull PostgresBackendConfig config;
    private final @NotNull SqlDialect dialect = new PostgresDialect();

    private Logger logger;
    private HikariDataSource ds;
    private SqlEntityAdapter entityAdapter;
    private ac.grim.grimac.internal.storage.backend.sql.v2.SqlServerOwnershipAdapter ownershipAdapter;
    private ac.grim.grimac.internal.storage.backend.sql.v2.SqlEventStreamAdapter eventStreamAdapter;
    private ac.grim.grimac.internal.storage.backend.sql.v2.SqlKeyValueScopedAdapter kvScopedAdapter;
    private ac.grim.grimac.internal.storage.backend.sql.v2.SqlCounterAdapter counterAdapter;

    public PostgresBackendV2(@NotNull PostgresBackendConfig config) {
        this.config = config;
    }

    @Override public @NotNull String id() { return "postgres-v2"; }
    @Override public @NotNull ApiVersion apiVersion() { return ApiVersion.CURRENT; }

    @Override
    public int writerThreads(@NotNull ac.grim.grimac.api.storage.category.Category<?> category) {
        return config.writerThreadsFor(category.id());
    }

    @Override
    public @NotNull EnumSet<Capability> capabilities() {
        return EnumSet.of(
            Capability.KIND_ENTITY,
            Capability.KIND_EVENT_STREAM,
            Capability.KIND_KV_SCOPED,
            Capability.KIND_COUNTER,
            Capability.ATOMIC_UPSERT,
            Capability.MULTI_WRITER);
    }

    @Override
    public void init(@NotNull BackendContext ctx) throws BackendException {
        this.logger = ctx.logger();
        try {
            // Force-load both classes so a missing dependency fails at
            // boot with a clear message rather than mid-write with a
            // NoClassDefFoundError surfacing through the disruptor.
            Class.forName("org.postgresql.Driver");
            Class.forName("com.zaxxer.hikari.HikariDataSource");
        } catch (ClassNotFoundException cnf) {
            throw new BackendException(
                "postgresql JDBC driver or HikariCP not on the classpath — shade them "
                    + "into the plugin jar or drop into server/plugins (missing: "
                    + cnf.getMessage() + ")", cnf);
        }
        // 5-second connectTimeout bounds the TCP connect/handshake at
        // boot so a blackholed host fails fast instead of hanging on
        // PgJDBC's 60s default. The user's jdbcUrl() already includes
        // their extraJdbcParams; only append connectTimeout if it
        // isn't already specified so an operator override wins.
        String jdbcUrl = withConnectTimeout(config.jdbcUrl(), 5);

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(jdbcUrl);
        hc.setUsername(config.user());
        if (config.password() != null) hc.setPassword(config.password());
        hc.setMaximumPoolSize(DEFAULT_MAX_POOL_SIZE);
        hc.setPoolName(POOL_NAME);
        // Mirror connectTimeout at the Hikari level so connection
        // ACQUISITION (not just initial TCP connect) is bounded.
        hc.setConnectionTimeout(5_000L); // ms — Hikari minimum is 250
        // Verify connections quickly on borrow; PgJDBC's default
        // isValid implementation is a cheap SELECT 1.
        hc.setValidationTimeout(3_000L);
        // Don't keep idle connections forever — close after 10 minutes
        // so server-side connection caches don't pile up across slow
        // periods.
        hc.setIdleTimeout(600_000L);

        // new HikariDataSource(hc) eagerly starts the pool and can
        // throw PoolInitializationException on bad config (DB
        // unreachable, wrong creds, etc.) BEFORE the smoke-test
        // try/catch below would run. Wrap construction explicitly so
        // every failure surfaces as a BackendException with the
        // underlying cause preserved.
        HikariDataSource pool;
        try {
            pool = new HikariDataSource(hc);
        } catch (RuntimeException e) {
            throw new BackendException("failed to initialise Hikari pool for postgres", e);
        }
        this.ds = pool;

        // Smoke-test on first acquire — pool is constructed by now, so
        // any failure here is per-connection (driver-level), not
        // pool-init. Release the (now-empty) pool on failure so we
        // don't leak the Hikari housekeeping thread.
        try (Connection probe = ds.getConnection()) {
            if (!probe.isValid(5)) {
                throw new BackendException("postgres connection probe returned invalid", null);
            }
        } catch (SQLException e) {
            ds.close();
            throw new BackendException("postgres connection probe failed", e);
        }
        this.entityAdapter = new SqlEntityAdapter(ds, dialect, logger);
        this.ownershipAdapter = new ac.grim.grimac.internal.storage.backend.sql.v2.SqlServerOwnershipAdapter(ds, dialect, logger);
        this.eventStreamAdapter = new ac.grim.grimac.internal.storage.backend.sql.v2.SqlEventStreamAdapter(ds, dialect, logger);
        this.kvScopedAdapter = new ac.grim.grimac.internal.storage.backend.sql.v2.SqlKeyValueScopedAdapter(ds, dialect, logger);
        this.counterAdapter = new ac.grim.grimac.internal.storage.backend.sql.v2.SqlCounterAdapter(ds, dialect, logger);
    }

    @Override
    public void flush() {
        // Nothing buffered at the backend level; per-adapter handlers
        // flush via their own batch hooks (Phase 5b).
    }

    @Override
    public void close() throws BackendException {
        if (ds != null) {
            try {
                ds.close();
            } catch (RuntimeException e) {
                throw new BackendException("failed to close Hikari pool", e);
            }
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <K extends DataKind<?, ?>> @NotNull Optional<KindAdapter<K>> adapterFor(@NotNull K kind) {
        if (kind instanceof Entity<?, ?, ?>)            return Optional.of((KindAdapter) entityAdapter);
        if (kind instanceof EventStream<?, ?>)          return Optional.of((KindAdapter) eventStreamAdapter);
        if (kind instanceof KeyValueScoped<?, ?>)       return Optional.of((KindAdapter) kvScopedAdapter);
        if (kind instanceof Counter<?>)                 return Optional.of((KindAdapter) counterAdapter);
        return Optional.empty();
    }

    @Override public @NotNull Optional<SearchAdapter> searchAdapter() { return Optional.empty(); }
    @Override public @NotNull Optional<TxAdapter> txAdapter() { return Optional.empty(); }
    @Override public @NotNull Optional<AdminAdapter> adminAdapter() { return Optional.empty(); }
    @Override public @NotNull Optional<ac.grim.grimac.api.storage.instance.ServerOwnershipAdapter> ownershipAdapter() {
        return Optional.ofNullable(ownershipAdapter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <X> @NotNull Optional<X> unwrap(@NotNull Class<X> type) {
        if (type.isInstance(ds)) return Optional.of((X) ds);
        return Optional.empty();
    }

    /**
     * Append {@code connectTimeout=<seconds>} to a JDBC URL if the
     * URL doesn't already specify one. Operator-supplied overrides
     * in {@code config.extraJdbcParams} take precedence (their
     * {@code connectTimeout} appears in the URL before this one and
     * Postgres uses the first occurrence).
     */
    static @NotNull String withConnectTimeout(@NotNull String url, int seconds) {
        if (url.contains("connectTimeout=")) return url;
        char sep = url.indexOf('?') >= 0 ? '&' : '?';
        return url + sep + "connectTimeout=" + seconds;
    }
}
