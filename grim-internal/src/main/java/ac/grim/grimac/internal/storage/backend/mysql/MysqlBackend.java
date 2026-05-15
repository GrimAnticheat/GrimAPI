package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.check.CheckCatalogPersistence;
import ac.grim.grimac.api.storage.check.CheckCatalogRepairResult;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.api.storage.event.PlayerIdentityEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.event.SettingEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Deletes;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.api.storage.query.Query;
import ac.grim.grimac.internal.storage.checks.JdbcCheckCatalogPersistence;
import ac.grim.grimac.internal.storage.checks.JdbcCheckCatalogRepair;
import ac.grim.grimac.internal.storage.util.UuidCodec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * MySQL-family backend. Same ring/handler model as the SQLite reference
 * backend — each category's {@link StorageEventHandler} owns a dedicated write
 * connection in {@code autoCommit=false}, reads open a fresh connection per
 * call. No connection pool is bundled; operators who need one can front the
 * JDBC URL with a server-side proxy (ProxySQL) or drop in HikariCP alongside
 * the plugin.
 * <p>
 * Unsupported features gracefully throw {@link UnsupportedOperationException}
 * where the {@link Backend} contract allows it. The schema is the post-v5
 * baseline (sessions with {@code client_version_pvn}, violations with
 * {@code verbose_format} as INTEGER and a {@code metadata} column); there are
 * no pre-v5 MySQL databases to migrate from, so the linear applyVN pattern
 * used by SQLite isn't mirrored here.
 * <p>
 * Server flavor (genuine MySQL vs MariaDB) is detected at {@link #init} by
 * probing {@code SELECT VERSION()}; the dialect that owns the flavor-specific
 * SQL (functional index vs STORED generated column for {@code current_name},
 * aliased-row vs legacy {@code VALUES()} upsert) is held in {@link #dialect}.
 */
@ApiStatus.Internal
public final class MysqlBackend implements Backend {

    public static final String ID = "mysql";
    private static final long CONNECTION_VALIDATE_AFTER_IDLE_MS = TimeUnit.SECONDS.toMillis(30);
    private static final int CONNECTION_VALIDATION_TIMEOUT_SECONDS = 2;

    private final MysqlBackendConfig config;
    private final Object writeMutex = new Object();
    private final List<BatchingHandler<?>> handlers = new ArrayList<>();
    private final String insertViolations;
    // Populated in init() once the dialect is selected from the live engine.
    // The handlers and direct-write paths read these via final-field-after-init
    // semantics — init() runs before eventHandlerFor() / writeXxx() is invoked.
    private String upsertSessions;
    private String upsertIdentities;
    private String upsertSettings;
    private MysqlDialect dialect;
    private Connection writeConn;
    // Cached at init() if minecraft-mysql-jdbc holder is present. When non-null,
    // every openConnection() call routes through the holder's child-first
    // classloader instead of DriverManager. See SqliteBackend for the same
    // pattern with deeper rationale.
    private java.lang.reflect.Method holderConnectMethod;

    public MysqlBackend(MysqlBackendConfig config) {
        this.config = config;
        TableNames t = config.tableNames();
        // Flavor-agnostic — plain INSERT, no ON DUPLICATE KEY clause.
        this.insertViolations =
                "INSERT INTO " + t.violations() + "(id, session_id, player_uuid, check_id, vl, occurred_at, verbose, verbose_format) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override public @NotNull String id() { return ID; }

    @Override public @NotNull ApiVersion getApiVersion() { return ApiVersion.CURRENT; }

    @Override
    public @NotNull EnumSet<Capability> capabilities() {
        return EnumSet.of(
                Capability.INDEXED_KV,
                Capability.TIMESERIES_APPEND,
                Capability.TRANSACTIONS,
                Capability.HISTORY,
                Capability.SETTINGS,
                Capability.PLAYER_IDENTITY);
    }

    @Override
    public @NotNull Set<Category<?>> supportedCategories() {
        return Set.of(
                Categories.VIOLATION,
                Categories.SESSION,
                Categories.PLAYER_IDENTITY,
                Categories.SETTING);
    }

    @Override
    public void init(@NotNull BackendContext ctx) throws BackendException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException cnf) {
            throw new BackendException("mysql-connector-j not on the classpath — either shade it into the plugin jar or drop it into server/plugins", cnf);
        }
        // If minecraft-mysql-jdbc holder is on the classloader graph (Grim
        // softdepends on it), route through its child-first classloader.
        // Unlike SQLite, MySQL doesn't have a sharply-different feature gap
        // between bundled and current connector-j — operators install the
        // holder when they specifically want its version (typically on
        // Fabric/NeoForge or to upgrade past a stale fork's bundled driver).
        this.holderConnectMethod = pickHolderIfInstalled(ctx.logger());
        synchronized (writeMutex) {
            try {
                this.writeConn = openConnection();
                this.dialect = selectDialect(writeConn, ctx.logger());
                TableNames t = config.tableNames();
                this.upsertSessions = dialect.upsertSessions(t);
                this.upsertIdentities = dialect.upsertIdentities(t);
                this.upsertSettings = dialect.upsertSettings(t);
                MysqlSchema.ensureInitialized(writeConn, "phase1", t, dialect);
            } catch (BackendException be) {
                throw be;
            } catch (SQLException e) {
                throw new BackendException("failed to initialise MySQL backend", e);
            }
        }
    }

    @Override
    public @NotNull CheckCatalogPersistence checkCatalog() {
        return new JdbcCheckCatalogPersistence(this::openConnection, config.tableNames().checks());
    }

    @Override
    public @NotNull CheckCatalogRepairResult repairCheckCatalog(
            @NotNull Map<Integer, Integer> legacyToCatalogCheckIds,
            String introducedVersionReplacement) throws BackendException {
        try {
            TableNames t = config.tableNames();
            return JdbcCheckCatalogRepair.run(
                    this::openConnection,
                    t.checks(),
                    t.violations(),
                    legacyToCatalogCheckIds,
                    introducedVersionReplacement);
        } catch (SQLException e) {
            throw new BackendException("check catalog repair failed", e);
        }
    }

    /**
     * Probe {@code SELECT VERSION()} and pick the matching dialect. MariaDB
     * 10.6+ goes through {@link MariaDbDialect}; everything else routes through
     * {@link MysqlEightDialect}. MariaDB older than 10.6 is rejected — the
     * STORED-generated-column DDL needs 10.2.1+ at the absolute minimum, and
     * 10.6 is the oldest LTS still in upstream support.
     * <p>
     * Mirrors {@link ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackend}'s
     * dialect-selector pattern; see that class for the fuller rationale on
     * version-driven dialect splits over compile-time forks.
     */
    static MysqlDialect selectDialect(Connection c, java.util.logging.Logger log) throws SQLException, BackendException {
        String version;
        try (java.sql.Statement s = c.createStatement();
             java.sql.ResultSet rs = s.executeQuery("SELECT VERSION()")) {
            version = rs.next() ? rs.getString(1) : "";
        }
        boolean mariaDb = version.contains("MariaDB");
        if (!mariaDb) {
            log.info("[grim-datastore] MySQL engine " + version + " — using MySQL 8 dialect");
            return new MysqlEightDialect();
        }
        // MariaDB version strings:
        //   "10.11.16-MariaDB-ubu2204"           — modern
        //   "5.5.5-10.11.16-MariaDB-…"           — legacy compat prefix the
        //                                          server prepends to fool old
        //                                          MySQL client libs (see
        //                                          MDEV-9788). Strip it.
        String triplePart = version.startsWith("5.5.5-") ? version.substring("5.5.5-".length()) : version;
        int dash = triplePart.indexOf('-');
        if (dash > 0) triplePart = triplePart.substring(0, dash);
        int[] triple = parseTriple(triplePart);
        int major = triple[0], minor = triple[1];
        if (major < 10 || (major == 10 && minor < 6)) {
            throw new BackendException("MariaDB " + version + " is too old; minimum supported is 10.6 "
                    + "(STORED generated columns require 10.2.1, 10.6 is the oldest LTS still in upstream support). "
                    + "Upgrade MariaDB or point Grim at a different storage backend.");
        }
        log.info("[grim-datastore] MariaDB engine " + version + " — using MariaDB dialect");
        return new MariaDbDialect();
    }

    private static int[] parseTriple(String version) {
        int[] triple = {0, 0, 0};
        int idx = 0, i = 0;
        while (i < version.length() && idx < 3) {
            int start = i;
            while (i < version.length() && Character.isDigit(version.charAt(i))) i++;
            if (i == start) break;
            triple[idx++] = Integer.parseInt(version.substring(start, i));
            if (i < version.length() && version.charAt(i) == '.') i++;
            else break;
        }
        return triple;
    }

    /**
     * Probe for {@code dev.axionize.mysql_jdbc.MinecraftMysqlJdbc} on the
     * classpath. Return its {@code connect(String, Properties)} method if
     * present, {@code null} otherwise. See SqliteBackend.pickHolderIfNewer
     * for the deeper reasoning on why the API class lets us bypass parent-
     * first delegation.
     */
    private java.lang.reflect.Method pickHolderIfInstalled(java.util.logging.Logger log) {
        try {
            Class<?> api = Class.forName(
                    "dev.axionize.mysql_jdbc.MinecraftMysqlJdbc",
                    true, getClass().getClassLoader());
            String holderVersion = (String) api.getMethod("driverVersion").invoke(null);
            java.lang.reflect.Method connect = api.getMethod(
                    "connect", String.class, java.util.Properties.class);
            log.info("[grim-datastore] minecraft-mysql-jdbc holder detected (driver "
                    + holderVersion + "); routing JDBC through holder's child-first classloader");
            return connect;
        } catch (ClassNotFoundException notInstalled) {
            return null;
        } catch (Throwable t) {
            log.warning("[grim-datastore] mysql holder probe failed (" + t + "); using bundled driver");
            return null;
        }
    }

    private Connection openConnection() throws SQLException {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", config.user());
        props.setProperty("password", config.password() == null ? "" : config.password());
        if (holderConnectMethod != null) {
            try {
                return (Connection) holderConnectMethod.invoke(null, config.jdbcUrl(), props);
            } catch (java.lang.reflect.InvocationTargetException ite) {
                Throwable cause = ite.getCause();
                if (cause instanceof SQLException) throw (SQLException) cause;
                throw new SQLException("mysql holder connect failed", cause != null ? cause : ite);
            } catch (Exception e) {
                throw new SQLException("mysql holder connect failed", e);
            }
        }
        return DriverManager.getConnection(config.jdbcUrl(), config.user(),
                config.password() == null ? "" : config.password());
    }

    private Connection ensureWriteConnectionLocked() throws SQLException {
        if (isConnectionUsable(writeConn, 0L)) return writeConn;
        closeQuietly(writeConn);
        writeConn = openConnection();
        return writeConn;
    }

    private static boolean isConnectionUsable(Connection connection, long lastUseMs) {
        if (connection == null) return false;
        try {
            if (connection.isClosed()) return false;
            long now = System.currentTimeMillis();
            if (lastUseMs > 0L && now - lastUseMs < CONNECTION_VALIDATE_AFTER_IDLE_MS) return true;
            return connection.isValid(CONNECTION_VALIDATION_TIMEOUT_SECONDS);
        } catch (SQLException e) {
            return false;
        }
    }

    private static boolean isConnectionFailure(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql) {
                String state = sql.getSQLState();
                if (state != null && state.startsWith("08")) return true;
            }
            String className = t.getClass().getName();
            if (className.contains("ConnectionIsClosed") || className.contains("CJCommunicationsException")) {
                return true;
            }
        }
        return false;
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) return;
        try { closeable.close(); } catch (Exception ignored) {}
    }

    @Override public void flush() {}

    @Override
    public void close() throws BackendException {
        synchronized (handlers) {
            for (BatchingHandler<?> h : handlers) h.shutDown();
            handlers.clear();
        }
        synchronized (writeMutex) {
            try {
                if (writeConn != null) writeConn.close();
            } catch (SQLException e) {
                throw new BackendException("close failed", e);
            } finally {
                writeConn = null;
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <E> StorageEventHandler<E> eventHandlerFor(@NotNull Category<E> cat) throws BackendException {
        BatchingHandler<?> h;
        if (cat == Categories.VIOLATION) h = new ViolationHandler();
        else if (cat == Categories.SESSION) h = new SessionHandler();
        else if (cat == Categories.PLAYER_IDENTITY) h = new IdentityHandler();
        else if (cat == Categories.SETTING) h = new SettingHandler();
        else throw new IllegalArgumentException("unsupported category: " + cat.id());
        h.start();
        synchronized (handlers) { handlers.add(h); }
        return (StorageEventHandler<E>) h;
    }

    private abstract class BatchingHandler<E> implements StorageEventHandler<E> {
        private Connection conn;
        private PreparedStatement stmt;
        private final List<PendingWrite> pending = new ArrayList<>();
        private long lastConnectionUseMs;

        void start() throws BackendException {
            try {
                reconnectLocked();
            } catch (SQLException e) {
                throw new BackendException("init failed for " + categoryId(), e);
            }
        }

        @Override
        public synchronized void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            try {
                pending.add(snapshot(event));
                if (endOfBatch || pending.size() >= config.batchFlushCap()) flushLocked();
            } catch (SQLException e) {
                pending.clear();
                throw new BackendException(categoryId() + " write failed", e);
            }
        }

        private void flushLocked() throws SQLException {
            if (pending.isEmpty()) return;
            try {
                executePendingLocked();
                pending.clear();
            } catch (SQLException first) {
                abortLocked(first);
                if (!isConnectionFailure(first)) {
                    pending.clear();
                    throw first;
                }
                reconnectLocked();
                try {
                    executePendingLocked();
                    pending.clear();
                } catch (SQLException second) {
                    abortLocked(second);
                    pending.clear();
                    throw second;
                }
            }
        }

        private void executePendingLocked() throws SQLException {
            ensureConnectionLocked();
            if (stmt == null) stmt = conn.prepareStatement(sql());
            for (PendingWrite write : pending) {
                write.bind(stmt);
                stmt.addBatch();
            }
            stmt.executeBatch();
            conn.commit();
            lastConnectionUseMs = System.currentTimeMillis();
        }

        private void abortLocked(SQLException cause) {
            try { if (conn != null) conn.rollback(); } catch (SQLException ignore) {}
            closeQuietly(stmt);
            stmt = null;
            if (isConnectionFailure(cause)) {
                closeQuietly(conn);
                conn = null;
                lastConnectionUseMs = 0L;
            }
        }

        private void ensureConnectionLocked() throws SQLException {
            if (isConnectionUsable(conn, lastConnectionUseMs)) return;
            reconnectLocked();
        }

        private void reconnectLocked() throws SQLException {
            closeQuietly(stmt);
            closeQuietly(conn);
            stmt = null;
            conn = openConnection();
            conn.setAutoCommit(false);
            lastConnectionUseMs = System.currentTimeMillis();
        }

        synchronized void shutDown() {
            try { flushLocked(); }
            catch (SQLException ignore) {}
            closeQuietly(stmt);
            closeQuietly(conn);
            stmt = null;
            conn = null;
            pending.clear();
            lastConnectionUseMs = 0L;
        }

        protected abstract String sql();
        protected abstract String categoryId();
        protected abstract PendingWrite snapshot(E event);
    }

    @FunctionalInterface
    private interface PendingWrite {
        void bind(PreparedStatement ps) throws SQLException;
    }

    private final class ViolationHandler extends BatchingHandler<ViolationEvent> {
        @Override protected String sql() { return insertViolations; }
        @Override protected String categoryId() { return "violation"; }
        @Override
        protected PendingWrite snapshot(ViolationEvent v) {
            UUID id = v.id();
            UUID sessionId = v.sessionId();
            UUID playerUuid = v.playerUuid();
            int checkId = v.checkId();
            double vl = v.vl();
            long occurredEpochMs = v.occurredEpochMs();
            String verbose = v.verbose();
            int verboseFormat = v.verboseFormat().code();
            return ps -> {
                ps.setBytes(1, UuidCodec.toBytes(id));
                ps.setBytes(2, UuidCodec.toBytes(sessionId));
                ps.setBytes(3, UuidCodec.toBytes(playerUuid));
                ps.setInt(4, checkId);
                ps.setDouble(5, vl);
                ps.setLong(6, occurredEpochMs);
                ps.setString(7, verbose);
                ps.setInt(8, verboseFormat);
            };
        }
    }

    private final class SessionHandler extends BatchingHandler<SessionEvent> {
        @Override protected String sql() { return upsertSessions; }
        @Override protected String categoryId() { return "session"; }
        @Override
        protected PendingWrite snapshot(SessionEvent s) {
            UUID sessionId = s.sessionId();
            UUID playerUuid = s.playerUuid();
            String serverName = s.serverName();
            long startedEpochMs = s.startedEpochMs();
            long lastActivityEpochMs = s.lastActivityEpochMs();
            Long closedAtEpochMs = s.closedAtEpochMs();
            String grimVersion = s.grimVersion();
            String clientBrand = s.clientBrand();
            int clientVersion = s.clientVersion();
            String serverVersionString = s.serverVersionString();
            boolean hasSessionBlobs = !s.sessionBlobs().isEmpty();
            return ps -> {
                ps.setBytes(1, UuidCodec.toBytes(sessionId));
                ps.setBytes(2, UuidCodec.toBytes(playerUuid));
                ps.setString(3, serverName);
                ps.setLong(4, startedEpochMs);
                ps.setLong(5, lastActivityEpochMs);
                if (closedAtEpochMs == null) ps.setNull(6, java.sql.Types.BIGINT);
                else ps.setLong(6, closedAtEpochMs);
                ps.setString(7, grimVersion);
                ps.setString(8, clientBrand);
                ps.setInt(9, clientVersion);
                ps.setString(10, serverVersionString);
                ps.setString(11, hasSessionBlobs ? serializeSessionBlobsShim() : "[]");
            };
        }
    }

    private final class IdentityHandler extends BatchingHandler<PlayerIdentityEvent> {
        @Override protected String sql() { return upsertIdentities; }
        @Override protected String categoryId() { return "player-identity"; }
        @Override
        protected PendingWrite snapshot(PlayerIdentityEvent e) {
            UUID uuid = e.uuid();
            String currentName = e.currentName();
            long firstSeenEpochMs = e.firstSeenEpochMs();
            long lastSeenEpochMs = e.lastSeenEpochMs();
            return ps -> {
                ps.setBytes(1, UuidCodec.toBytes(uuid));
                ps.setString(2, currentName);
                ps.setLong(3, firstSeenEpochMs);
                ps.setLong(4, lastSeenEpochMs);
            };
        }
    }

    private final class SettingHandler extends BatchingHandler<SettingEvent> {
        @Override protected String sql() { return upsertSettings; }
        @Override protected String categoryId() { return "setting"; }
        @Override
        protected PendingWrite snapshot(SettingEvent s) {
            String scope = s.scope().name();
            String scopeKey = s.scopeKey();
            String key = s.key();
            byte[] value = s.value().clone();
            long updatedEpochMs = s.updatedEpochMs();
            return ps -> {
                ps.setString(1, scope);
                ps.setString(2, scopeKey);
                ps.setString(3, key);
                ps.setBytes(4, value);
                ps.setLong(5, updatedEpochMs);
            };
        }
    }

    private static String serializeSessionBlobsShim() {
        throw new UnsupportedOperationException(
                "session-blob serialisation isn't implemented by this backend; "
                        + "sessions with non-empty sessionBlobs cannot be stored");
    }

    // --- bulk-load path (cross-backend copy target) -------------------------

    @Override
    public <R> void bulkImport(@NotNull Category<?> cat, @NotNull List<R> records) throws BackendException {
        if (records.isEmpty()) return;
        synchronized (writeMutex) {
            try {
                ensureWriteConnectionLocked();
                writeConn.setAutoCommit(false);
                if (cat == Categories.VIOLATION) writeViolationRecords((List<ViolationRecord>) records);
                else if (cat == Categories.SESSION) writeSessionRecords((List<SessionRecord>) records);
                else if (cat == Categories.PLAYER_IDENTITY) writeIdentityRecords((List<PlayerIdentity>) records);
                else if (cat == Categories.SETTING) writeSettingRecords((List<SettingRecord>) records);
                else throw new BackendException("unsupported category: " + cat.id());
                writeConn.commit();
            } catch (SQLException e) {
                try { if (writeConn != null) writeConn.rollback(); } catch (SQLException ignore) {}
                if (isConnectionFailure(e)) {
                    closeQuietly(writeConn);
                    writeConn = null;
                }
                throw new BackendException("bulkImport failed for " + cat.id(), e);
            } finally {
                try { if (writeConn != null) writeConn.setAutoCommit(true); } catch (SQLException ignore) {}
            }
        }
    }

    private void writeViolationRecords(List<ViolationRecord> rows) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(insertViolations)) {
            for (ViolationRecord v : rows) {
                ps.setBytes(1, UuidCodec.toBytes(v.id()));
                ps.setBytes(2, UuidCodec.toBytes(v.sessionId()));
                ps.setBytes(3, UuidCodec.toBytes(v.playerUuid()));
                ps.setInt(4, v.checkId());
                ps.setDouble(5, v.vl());
                ps.setLong(6, v.occurredEpochMs());
                ps.setString(7, v.verbose());
                ps.setInt(8, v.verboseFormat().code());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void writeSessionRecords(List<SessionRecord> rows) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(upsertSessions)) {
            for (SessionRecord s : rows) {
                ps.setBytes(1, UuidCodec.toBytes(s.sessionId()));
                ps.setBytes(2, UuidCodec.toBytes(s.playerUuid()));
                ps.setString(3, s.serverName());
                ps.setLong(4, s.startedEpochMs());
                ps.setLong(5, s.lastActivityEpochMs());
                if (s.closedAtEpochMs() == null) ps.setNull(6, java.sql.Types.BIGINT);
                else ps.setLong(6, s.closedAtEpochMs());
                ps.setString(7, s.grimVersion());
                ps.setString(8, s.clientBrand());
                ps.setInt(9, s.clientVersion());
                ps.setString(10, s.serverVersionString());
                ps.setString(11, s.sessionBlobs().isEmpty() ? "[]" : serializeSessionBlobsShim());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void writeIdentityRecords(List<PlayerIdentity> rows) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(upsertIdentities)) {
            for (PlayerIdentity id : rows) {
                ps.setBytes(1, UuidCodec.toBytes(id.uuid()));
                ps.setString(2, id.currentName());
                ps.setLong(3, id.firstSeenEpochMs());
                ps.setLong(4, id.lastSeenEpochMs());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void writeSettingRecords(List<SettingRecord> rows) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(upsertSettings)) {
            for (SettingRecord s : rows) {
                ps.setString(1, s.scope().name());
                ps.setString(2, s.scopeKey());
                ps.setString(3, s.key());
                ps.setBytes(4, s.value());
                ps.setLong(5, s.updatedEpochMs());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    // --- read path ----------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <R> Page<R> read(@NotNull Category<?> cat, @NotNull Query<R> query) throws BackendException {
        try (Connection c = openConnection()) {
            if (query instanceof Queries.ListSessionsByPlayer q) return (Page<R>) listSessionsByPlayer(c, q);
            if (query instanceof Queries.GetSessionById q) return (Page<R>) getSessionById(c, q);
            if (query instanceof Queries.ListViolationsInSession q) return (Page<R>) listViolationsInSession(c, q);
            if (query instanceof Queries.GetPlayerIdentity q) return (Page<R>) getPlayerIdentity(c, q);
            if (query instanceof Queries.GetPlayerIdentityByName q) return (Page<R>) getPlayerIdentityByName(c, q);
            if (query instanceof Queries.ListPlayersByNamePrefix q) return (Page<R>) listPlayersByNamePrefix(c, q);
            if (query instanceof Queries.GetSetting q) return (Page<R>) getSetting(c, q);
            throw new BackendException("unsupported query: " + query.getClass().getSimpleName());
        } catch (SQLException e) {
            throw new BackendException("read failed", e);
        }
    }

    private Page<SessionRecord> listSessionsByPlayer(Connection c, Queries.ListSessionsByPlayer q) throws SQLException {
        long cursorStarted = decodeStartedCursor(q.cursor(), Long.MAX_VALUE);
        byte[] cursorSessionId = decodeSessionIdCursor(q.cursor());
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT session_id, player_uuid, server_name, started_at, last_activity, closed_at, "
                        + "grim_version, client_brand, client_version_pvn, server_version, replay_clips_json "
                        + "FROM " + config.tableNames().sessions() + " "
                        + "WHERE player_uuid = ? AND (started_at < ? OR (started_at = ? AND session_id < ?)) "
                        + "ORDER BY started_at DESC, session_id DESC "
                        + "LIMIT ?")) {
            ps.setBytes(1, UuidCodec.toBytes(q.player()));
            ps.setLong(2, cursorStarted);
            ps.setLong(3, cursorStarted);
            ps.setBytes(4, cursorSessionId);
            ps.setInt(5, q.pageSize() + 1);
            try (ResultSet rs = ps.executeQuery()) {
                List<SessionRecord> out = new ArrayList<>();
                boolean hasMore = false;
                while (rs.next()) {
                    if (out.size() >= q.pageSize()) { hasMore = true; break; }
                    out.add(mapSession(rs));
                }
                Cursor next = null;
                if (hasMore && !out.isEmpty()) {
                    SessionRecord last = out.get(out.size() - 1);
                    next = encodeStartedCursor(last.startedEpochMs(), last.sessionId());
                }
                return new Page<>(out, next);
            }
        }
    }

    private Page<SessionRecord> getSessionById(Connection c, Queries.GetSessionById q) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT session_id, player_uuid, server_name, started_at, last_activity, closed_at, "
                        + "grim_version, client_brand, client_version_pvn, server_version, replay_clips_json "
                        + "FROM " + config.tableNames().sessions() + " WHERE session_id = ?")) {
            ps.setBytes(1, UuidCodec.toBytes(q.sessionId()));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return new Page<>(List.of(mapSession(rs)), null);
                return Page.empty();
            }
        }
    }

    private Page<ViolationRecord> listViolationsInSession(Connection c, Queries.ListViolationsInSession q) throws SQLException {
        // Order by (occurred_at, id) — id is wall-clock at mint time and can drift
        // from event time when callers pre-set event.id(), when ring slots sit
        // before the sink runs, or when bulkImport carries through original ids.
        // Id stays as the tiebreaker for same-ms bursts.
        long lastOccurred = decodeViolationOccurredCursor(q.cursor(), Long.MIN_VALUE);
        byte[] lastId = decodeViolationIdCursor(q.cursor());
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT id, session_id, player_uuid, check_id, vl, occurred_at, verbose, verbose_format "
                        + "FROM " + config.tableNames().violations() + " "
                        + "WHERE session_id = ? AND (occurred_at > ? OR (occurred_at = ? AND id > ?)) "
                        + "ORDER BY occurred_at ASC, id ASC "
                        + "LIMIT ?")) {
            ps.setBytes(1, UuidCodec.toBytes(q.sessionId()));
            ps.setLong(2, lastOccurred);
            ps.setLong(3, lastOccurred);
            ps.setBytes(4, lastId);
            ps.setInt(5, q.pageSize() + 1);
            try (ResultSet rs = ps.executeQuery()) {
                List<ViolationRecord> out = new ArrayList<>();
                boolean hasMore = false;
                while (rs.next()) {
                    if (out.size() >= q.pageSize()) { hasMore = true; break; }
                    out.add(mapViolation(rs));
                }
                Cursor next = null;
                if (hasMore && !out.isEmpty()) {
                    ViolationRecord last = out.get(out.size() - 1);
                    next = encodeViolationCursor(last.occurredEpochMs(), last.id());
                }
                return new Page<>(out, next);
            }
        }
    }

    private Page<PlayerIdentity> getPlayerIdentity(Connection c, Queries.GetPlayerIdentity q) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT uuid, current_name, first_seen, last_seen FROM " + config.tableNames().players() + " WHERE uuid = ?")) {
            ps.setBytes(1, UuidCodec.toBytes(q.uuid()));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return new Page<>(List.of(mapIdentity(rs)), null);
                return Page.empty();
            }
        }
    }

    private Page<PlayerIdentity> getPlayerIdentityByName(Connection c, Queries.GetPlayerIdentityByName q) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(dialect.selectIdentityByName(config.tableNames()))) {
            ps.setString(1, q.name().toLowerCase(Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return new Page<>(List.of(mapIdentity(rs)), null);
                return Page.empty();
            }
        }
    }

    private Page<PlayerIdentity> listPlayersByNamePrefix(Connection c, Queries.ListPlayersByNamePrefix q) throws SQLException {
        String prefix = q.lowerPrefix();
        if (prefix == null || prefix.isEmpty() || q.limit() <= 0) return Page.empty();
        String escaped = escapeLike(prefix);
        try (PreparedStatement ps = c.prepareStatement(dialect.selectIdentitiesByNamePrefix(config.tableNames()))) {
            ps.setString(1, escaped + "%");
            ps.setInt(2, q.limit());
            try (ResultSet rs = ps.executeQuery()) {
                java.util.List<PlayerIdentity> out = new java.util.ArrayList<>();
                while (rs.next()) out.add(mapIdentity(rs));
                return new Page<>(out, null);
            }
        }
    }

    private static String escapeLike(String s) {
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '%' || c == '_') out.append('\\');
            out.append(c);
        }
        return out.toString();
    }

    private Page<SettingRecord> getSetting(Connection c, Queries.GetSetting q) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT scope, scope_key, `key`, value, updated_at FROM " + config.tableNames().settings() + " "
                        + "WHERE scope = ? AND scope_key = ? AND `key` = ?")) {
            ps.setString(1, q.scope().name());
            ps.setString(2, q.scopeKey());
            ps.setString(3, q.key());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return new Page<>(List.of(mapSetting(rs)), null);
                return Page.empty();
            }
        }
    }

    private static SessionRecord mapSession(ResultSet rs) throws SQLException {
        long closedAt = rs.getLong("closed_at");
        return new SessionRecord(
                UuidCodec.fromBytes(rs.getBytes("session_id")),
                UuidCodec.fromBytes(rs.getBytes("player_uuid")),
                rs.getString("server_name"),
                rs.getLong("started_at"),
                rs.getLong("last_activity"),
                rs.wasNull() ? null : closedAt,
                rs.getString("grim_version"),
                rs.getString("client_brand"),
                rs.getInt("client_version_pvn"),
                rs.getString("server_version"),
                List.of());
    }

    private static ViolationRecord mapViolation(ResultSet rs) throws SQLException {
        return new ViolationRecord(
                UuidCodec.fromBytes(rs.getBytes("id")),
                UuidCodec.fromBytes(rs.getBytes("session_id")),
                UuidCodec.fromBytes(rs.getBytes("player_uuid")),
                rs.getInt("check_id"),
                rs.getDouble("vl"),
                rs.getLong("occurred_at"),
                rs.getString("verbose"),
                VerboseFormat.fromCode(rs.getInt("verbose_format")));
    }

    private static PlayerIdentity mapIdentity(ResultSet rs) throws SQLException {
        return new PlayerIdentity(
                UuidCodec.fromBytes(rs.getBytes("uuid")),
                rs.getString("current_name"),
                rs.getLong("first_seen"),
                rs.getLong("last_seen"));
    }

    private static SettingRecord mapSetting(ResultSet rs) throws SQLException {
        return new SettingRecord(
                ac.grim.grimac.api.storage.model.SettingScope.valueOf(rs.getString("scope")),
                rs.getString("scope_key"),
                rs.getString("key"),
                rs.getBytes("value"),
                rs.getLong("updated_at"));
    }

    @Override
    public <E> void delete(@NotNull Category<E> cat, @NotNull DeleteCriteria criteria) throws BackendException {
        synchronized (writeMutex) {
            try {
                ensureWriteConnectionLocked();
                writeConn.setAutoCommit(false);
                TableNames t = config.tableNames();
                if (criteria instanceof Deletes.ByPlayer d) {
                    byte[] uuid = UuidCodec.toBytes(d.uuid());
                    if (cat == Categories.VIOLATION) execDelete("DELETE FROM " + t.violations() + " WHERE player_uuid = ?", uuid);
                    else if (cat == Categories.SESSION) {
                        execDelete("DELETE FROM " + t.violations() + " WHERE player_uuid = ?", uuid);
                        execDelete("DELETE FROM " + t.sessions() + " WHERE player_uuid = ?", uuid);
                    } else if (cat == Categories.PLAYER_IDENTITY) {
                        execDelete("DELETE FROM " + t.players() + " WHERE uuid = ?", uuid);
                    } else if (cat == Categories.SETTING) {
                        try (PreparedStatement ps = writeConn.prepareStatement(
                                "DELETE FROM " + t.settings() + " WHERE scope = 'PLAYER' AND scope_key = ?")) {
                            ps.setString(1, d.uuid().toString());
                            ps.executeUpdate();
                        }
                    } else {
                        throw new BackendException("unsupported category for delete: " + cat.id());
                    }
                } else if (criteria instanceof Deletes.OlderThan d) {
                    long cutoff = System.currentTimeMillis() - d.maxAgeMs();
                    if (cat == Categories.SESSION) {
                        try (PreparedStatement ps = writeConn.prepareStatement(
                                "DELETE v FROM " + t.violations() + " v "
                                        + "JOIN " + t.sessions() + " s ON s.session_id = v.session_id "
                                        + "WHERE s.started_at < ?")) {
                            ps.setLong(1, cutoff);
                            ps.executeUpdate();
                        }
                        try (PreparedStatement ps = writeConn.prepareStatement(
                                "DELETE FROM " + t.sessions() + " WHERE started_at < ?")) {
                            ps.setLong(1, cutoff);
                            ps.executeUpdate();
                        }
                    } else if (cat == Categories.VIOLATION) {
                        try (PreparedStatement ps = writeConn.prepareStatement(
                                "DELETE FROM " + t.violations() + " WHERE occurred_at < ?")) {
                            ps.setLong(1, cutoff);
                            ps.executeUpdate();
                        }
                    } else {
                        throw new BackendException("unsupported category for retention: " + cat.id());
                    }
                } else {
                    throw new BackendException("unknown DeleteCriteria: " + criteria.getClass().getSimpleName());
                }
                writeConn.commit();
            } catch (SQLException e) {
                try { if (writeConn != null) writeConn.rollback(); } catch (SQLException ignore) {}
                if (isConnectionFailure(e)) {
                    closeQuietly(writeConn);
                    writeConn = null;
                }
                throw new BackendException("delete failed", e);
            } finally {
                try { if (writeConn != null) writeConn.setAutoCommit(true); } catch (SQLException ignore) {}
            }
        }
    }

    private void execDelete(String sql, byte[] uuid) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(sql)) {
            ps.setBytes(1, uuid);
            ps.executeUpdate();
        }
    }

    @Override
    public long countViolationsInSession(@NotNull UUID sessionId) throws BackendException {
        return scalarLong("SELECT COUNT(*) FROM " + config.tableNames().violations() + " WHERE session_id = ?",
                UuidCodec.toBytes(sessionId), "countViolationsInSession");
    }

    @Override
    public long countUniqueChecksInSession(@NotNull UUID sessionId) throws BackendException {
        return scalarLong("SELECT COUNT(DISTINCT check_id) FROM " + config.tableNames().violations() + " WHERE session_id = ?",
                UuidCodec.toBytes(sessionId), "countUniqueChecksInSession");
    }

    @Override
    public long countSessionsByPlayer(@NotNull UUID player) throws BackendException {
        return scalarLong("SELECT COUNT(*) FROM " + config.tableNames().sessions() + " WHERE player_uuid = ?",
                UuidCodec.toBytes(player), "countSessionsByPlayer");
    }

    @Override
    public long markCrashedSessions() throws BackendException {
        try (Connection c = openConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE " + config.tableNames().sessions()
                             + " SET closed_at = last_activity WHERE closed_at IS NULL")) {
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new BackendException("markCrashedSessions failed", e);
        }
    }

    private long scalarLong(String sql, byte[] bind, String op) throws BackendException {
        try (Connection c = openConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setBytes(1, bind);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (SQLException e) {
            throw new BackendException(op + " failed", e);
        }
    }

    private static Cursor encodeStartedCursor(long started, UUID sessionId) {
        return new Cursor(started + ":" + sessionId.toString().replace("-", ""));
    }

    private static long decodeStartedCursor(Cursor c, long defaultVal) {
        if (c == null) return defaultVal;
        String t = c.token();
        int colon = t.indexOf(':');
        if (colon <= 0) return defaultVal;
        try { return Long.parseLong(t.substring(0, colon)); }
        catch (NumberFormatException e) { return defaultVal; }
    }

    private static byte[] decodeSessionIdCursor(Cursor c) {
        if (c == null) return new byte[16];
        String t = c.token();
        int colon = t.indexOf(':');
        if (colon <= 0 || colon == t.length() - 1) return new byte[16];
        String hex = t.substring(colon + 1);
        if (hex.length() != 32) return new byte[16];
        byte[] out = new byte[16];
        for (int i = 0; i < 16; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    private static Cursor encodeViolationCursor(long occurredAt, UUID id) {
        return new Cursor("v:" + occurredAt + ":" + id);
    }

    private static long decodeViolationOccurredCursor(Cursor c, long defaultVal) {
        if (c == null) return defaultVal;
        String[] parts = c.token().split(":", 3);
        if (parts.length < 3 || !"v".equals(parts[0])) return defaultVal;
        try { return Long.parseLong(parts[1]); }
        catch (NumberFormatException e) { return defaultVal; }
    }

    private static byte[] decodeViolationIdCursor(Cursor c) {
        if (c == null) return new byte[16];
        String[] parts = c.token().split(":", 3);
        if (parts.length < 3 || !"v".equals(parts[0])) return new byte[16];
        try { return UuidCodec.toBytes(UUID.fromString(parts[2])); }
        catch (IllegalArgumentException e) { return new byte[16]; }
    }

    @ApiStatus.Internal
    public TableNames tableNames() {
        return config.tableNames();
    }

    /** Test-only accessor for asserting the version probe picked the right dialect. */
    @ApiStatus.Internal
    MysqlDialect dialectForTest() {
        return dialect;
    }
}
