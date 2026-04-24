package ac.grim.grimac.internal.storage.backend.sqlite;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
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
import ac.grim.grimac.internal.storage.backend.sqlite.writers.IdentityUpserter;
import ac.grim.grimac.internal.storage.backend.sqlite.writers.SessionUpserter;
import ac.grim.grimac.internal.storage.backend.sqlite.writers.SettingsUpserter;
import ac.grim.grimac.internal.storage.backend.sqlite.writers.UpserterFactory;
import ac.grim.grimac.internal.storage.util.UuidCodec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * SQLite reference backend. Each {@link StorageEventHandler} owns its own
 * write connection in {@code autoCommit=false} mode, so cross-category
 * commits never tread on each other's in-flight transactions. SQLite's
 * file-level locking serialises the actual writes; the per-handler isolation
 * just keeps transaction boundaries clean. Reads open a fresh connection per
 * call — WAL mode permits concurrent readers alongside the writers.
 * <p>
 * {@link #writeRecordsDirect(Category, List)} is a record-taking bulk-load
 * escape hatch for one-shot importers (e.g. {@code LegacyMigrator}) that want
 * to bypass the ring; it uses the backend's shared {@code writeConn} under
 * {@code writeMutex}.
 */
@ApiStatus.Internal
public final class SqliteBackend implements Backend {

    public static final String ID = "sqlite";

    private final SqliteBackendConfig config;
    private final Object writeMutex = new Object();
    private final List<BatchingHandler<?>> handlers = new ArrayList<>();
    private final String insertViolations;
    // Picked once at init(). Defaults to MODERN here; the version-probe branch
    // in init() may swap to a legacy factory for pre-3.24 SQLite engines.
    private UpserterFactory upserterFactory = UpserterFactory.MODERN;
    private String jdbcUrl;
    private Connection writeConn;

    public SqliteBackend(SqliteBackendConfig config) {
        this.config = config;
        ac.grim.grimac.api.storage.config.TableNames t = config.tableNames();
        this.insertViolations =
                "INSERT INTO " + t.violations() + "(session_id, player_uuid, check_id, vl, occurred_at, verbose, verbose_format) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)";
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
        Path dataDir = ctx.dataDirectory();
        Path dbFile = Path.of(config.path()).isAbsolute()
                ? Path.of(config.path())
                : dataDir.resolve(config.path());
        try {
            java.nio.file.Files.createDirectories(dbFile.getParent());
        } catch (java.io.IOException e) {
            throw new BackendException("failed to create SQLite data dir: " + dbFile.getParent(), e);
        }
        this.jdbcUrl = "jdbc:sqlite:" + dbFile.toAbsolutePath();
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException cnf) {
            throw new BackendException("sqlite-jdbc not on the classpath", cnf);
        }
        synchronized (writeMutex) {
            try {
                this.writeConn = openConnection();
                try (Statement s = writeConn.createStatement()) {
                    s.execute("PRAGMA journal_mode=" + config.journalMode());
                    s.execute("PRAGMA synchronous=" + config.synchronousMode());
                    s.execute("PRAGMA busy_timeout=" + config.busyTimeoutMs());
                    s.execute("PRAGMA cache_size=" + config.cachePages());
                }
                this.upserterFactory = selectDialect(writeConn, ctx.logger());
                SqliteSchema.ensureInitialized(writeConn, "phase1", config.tableNames());
                // writeConn stays in autoCommit=true; write ops that batch wrap their own
                // setAutoCommit(false) / commit cycle.
            } catch (SQLException e) {
                throw new BackendException("failed to initialise SQLite backend", e);
            }
        }
    }

    /**
     * Picks a modern or legacy upsert dialect based on the live engine version.
     * The modern path uses {@code ON CONFLICT DO UPDATE} (SQLite 3.24+, 2018,
     * = CraftBukkit 1.13.2+ bundled). Older engines fall through to the legacy
     * two-step {@code INSERT OR IGNORE} + {@code UPDATE} path.
     */
    private static UpserterFactory selectDialect(Connection c, java.util.logging.Logger log) throws SQLException {
        String version;
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT sqlite_version()")) {
            if (!rs.next()) {
                log.warning("[grim-datastore] sqlite_version() returned no row; assuming modern dialect");
                return UpserterFactory.MODERN;
            }
            version = rs.getString(1);
        }
        boolean legacy = compareVersionTriple(version, 3, 24, 0) < 0;
        log.info("[grim-datastore] SQLite engine " + version + " — using "
                + (legacy ? "legacy (pre-3.24, no UPSERT) dialect" : "modern dialect"));
        return legacy ? UpserterFactory.LEGACY : UpserterFactory.MODERN;
    }

    private static int compareVersionTriple(String version, int majorFloor, int minorFloor, int patchFloor) {
        int[] triple = {0, 0, 0};
        int idx = 0;
        int i = 0;
        while (i < version.length() && idx < 3) {
            int start = i;
            while (i < version.length() && Character.isDigit(version.charAt(i))) i++;
            if (i == start) break;
            triple[idx++] = Integer.parseInt(version.substring(start, i));
            if (i < version.length() && version.charAt(i) == '.') i++;
            else break;
        }
        if (triple[0] != majorFloor) return Integer.compare(triple[0], majorFloor);
        if (triple[1] != minorFloor) return Integer.compare(triple[1], minorFloor);
        return Integer.compare(triple[2], patchFloor);
    }

    private Connection openConnection() throws SQLException {
        Connection c = DriverManager.getConnection(jdbcUrl);
        try (Statement s = c.createStatement()) {
            s.execute("PRAGMA busy_timeout=" + config.busyTimeoutMs());
        }
        return c;
    }

    @Override
    public void flush() throws BackendException {
        // Handlers commit on endOfBatch; nothing to flush explicitly here. Shutdown
        // drains the rings, which forces each handler's last batch through before
        // close() is called.
    }

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

    /**
     * Per-category handler that owns a dedicated write connection. The connection
     * stays in autoCommit=false mode for the handler's lifetime; {@code flushLocked}
     * commits and resets the pending count. Per-handler connections keep each
     * category's transaction timeline independent of sibling categories.
     * <p>
     * Each concrete handler owns its own statements (a plain PreparedStatement
     * for single-shot inserts, or an {@link UpserterFactory}-produced upserter
     * that may hide one or two PreparedStatements depending on dialect). The
     * abstract class knows only the lifecycle, never the statement shape.
     */
    private abstract class BatchingHandler<E> implements StorageEventHandler<E> {
        protected Connection conn;
        private int pending;

        void start() throws BackendException {
            try {
                this.conn = openConnection();
                conn.setAutoCommit(false);
            } catch (SQLException e) {
                throw new BackendException("init failed for " + categoryId(), e);
            }
        }

        @Override
        public synchronized void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            if (conn == null) return; // closed
            try {
                bindOne(event);
                pending++;
                if (endOfBatch || pending >= config.batchFlushCap()) flushLocked();
            } catch (SQLException e) {
                abortLocked();
                throw new BackendException(categoryId() + " write failed", e);
            }
        }

        private void flushLocked() throws SQLException {
            if (pending == 0) return;
            executeBatch();
            conn.commit();
            pending = 0;
        }

        private void abortLocked() {
            try { if (conn != null) conn.rollback(); } catch (SQLException ignore) {}
            try { closeStmts(); } catch (SQLException ignore) {}
            pending = 0;
        }

        synchronized void shutDown() {
            try { if (pending > 0) { executeBatch(); conn.commit(); } } catch (SQLException ignore) {}
            try { closeStmts(); } catch (SQLException ignore) {}
            try { if (conn != null) conn.close(); } catch (SQLException ignore) {}
            conn = null;
            pending = 0;
        }

        protected abstract String categoryId();

        /** Bind + addBatch one event onto this handler's statements. */
        protected abstract void bindOne(E event) throws SQLException;

        /** Execute all accumulated batches. Called inside {@code flushLocked()}. */
        protected abstract void executeBatch() throws SQLException;

        /** Close all owned statements. Called on shutdown and abort. */
        protected abstract void closeStmts() throws SQLException;
    }

    private final class ViolationHandler extends BatchingHandler<ViolationEvent> {
        private PreparedStatement stmt;

        @Override protected String categoryId() { return "violation"; }

        @Override
        protected void bindOne(ViolationEvent v) throws SQLException {
            if (stmt == null) stmt = conn.prepareStatement(insertViolations);
            stmt.setBytes(1, UuidCodec.toBytes(v.sessionId()));
            stmt.setBytes(2, UuidCodec.toBytes(v.playerUuid()));
            stmt.setInt(3, v.checkId());
            stmt.setDouble(4, v.vl());
            stmt.setLong(5, v.occurredEpochMs());
            stmt.setString(6, v.verbose());
            stmt.setInt(7, v.verboseFormat().code());
            stmt.addBatch();
        }

        @Override
        protected void executeBatch() throws SQLException {
            if (stmt != null) stmt.executeBatch();
        }

        @Override
        protected void closeStmts() throws SQLException {
            if (stmt != null) { stmt.close(); stmt = null; }
        }
    }

    private final class SessionHandler extends BatchingHandler<SessionEvent> {
        private SessionUpserter upserter;

        @Override protected String categoryId() { return "session"; }

        @Override
        protected void bindOne(SessionEvent s) throws SQLException {
            if (upserter == null) upserter = upserterFactory.newSessionUpserter(conn, config.tableNames());
            upserter.addBatch(
                    s.sessionId(), s.playerUuid(), s.serverName(),
                    s.startedEpochMs(), s.lastActivityEpochMs(),
                    s.grimVersion(), s.clientBrand(), s.clientVersion(),
                    s.serverVersionString(),
                    s.replayClips().isEmpty() ? "[]" : serializeReplayClipsShim());
        }

        @Override
        protected void executeBatch() throws SQLException {
            if (upserter != null) upserter.executeBatch();
        }

        @Override
        protected void closeStmts() throws SQLException {
            if (upserter != null) { upserter.close(); upserter = null; }
        }
    }

    private final class IdentityHandler extends BatchingHandler<PlayerIdentityEvent> {
        private IdentityUpserter upserter;

        @Override protected String categoryId() { return "player-identity"; }

        @Override
        protected void bindOne(PlayerIdentityEvent e) throws SQLException {
            if (upserter == null) upserter = upserterFactory.newIdentityUpserter(conn, config.tableNames());
            upserter.addBatch(e.uuid(), e.currentName(), e.firstSeenEpochMs(), e.lastSeenEpochMs());
        }

        @Override
        protected void executeBatch() throws SQLException {
            if (upserter != null) upserter.executeBatch();
        }

        @Override
        protected void closeStmts() throws SQLException {
            if (upserter != null) { upserter.close(); upserter = null; }
        }
    }

    private final class SettingHandler extends BatchingHandler<SettingEvent> {
        private SettingsUpserter upserter;

        @Override protected String categoryId() { return "setting"; }

        @Override
        protected void bindOne(SettingEvent s) throws SQLException {
            if (upserter == null) upserter = upserterFactory.newSettingsUpserter(conn, config.tableNames());
            upserter.addBatch(s.scope().name(), s.scopeKey(), s.key(), s.value(), s.updatedEpochMs());
        }

        @Override
        protected void executeBatch() throws SQLException {
            if (upserter != null) upserter.executeBatch();
        }

        @Override
        protected void closeStmts() throws SQLException {
            if (upserter != null) { upserter.close(); upserter = null; }
        }
    }

    private static String serializeReplayClipsShim() {
        throw new UnsupportedOperationException(
                "replay-clip serialisation isn't implemented by this backend; "
                        + "sessions with non-empty replayClips cannot be stored");
    }

    // --- migration / bulk-load path (bypasses rings, shared writeConn) ------

    @Override
    public <R> void bulkImport(@NotNull Category<?> cat, @NotNull List<R> records) throws BackendException {
        writeRecordsDirect(cat, records);
    }

    /**
     * Record-taking direct write for {@link ac.grim.grimac.internal.storage.migrate.LegacyMigrator}
     * and similar one-shot importers. Uses the backend's shared {@code writeConn}
     * (not a handler conn) so callers can interleave with {@link #writeConnection()}
     * metadata updates under the same mutex.
     */
    public void writeRecordsDirect(Category<?> cat, List<?> records) throws BackendException {
        if (records.isEmpty()) return;
        synchronized (writeMutex) {
            if (writeConn == null) throw new BackendException("backend not initialised");
            try {
                writeConn.setAutoCommit(false);
                if (cat == Categories.VIOLATION) writeViolationRecords((List<ViolationRecord>) records);
                else if (cat == Categories.SESSION) writeSessionRecords((List<SessionRecord>) records);
                else if (cat == Categories.PLAYER_IDENTITY) writeIdentityRecords((List<PlayerIdentity>) records);
                else if (cat == Categories.SETTING) writeSettingRecords((List<SettingRecord>) records);
                else throw new BackendException("unsupported category: " + cat.id());
                writeConn.commit();
            } catch (SQLException e) {
                try { writeConn.rollback(); } catch (SQLException ignore) {}
                throw new BackendException("writeRecordsDirect failed for " + cat.id(), e);
            } finally {
                try { writeConn.setAutoCommit(true); } catch (SQLException ignore) {}
            }
        }
    }

    private void writeViolationRecords(List<ViolationRecord> rows) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(insertViolations)) {
            for (ViolationRecord v : rows) {
                ps.setBytes(1, UuidCodec.toBytes(v.sessionId()));
                ps.setBytes(2, UuidCodec.toBytes(v.playerUuid()));
                ps.setInt(3, v.checkId());
                ps.setDouble(4, v.vl());
                ps.setLong(5, v.occurredEpochMs());
                ps.setString(6, v.verbose());
                ps.setInt(7, v.verboseFormat().code());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void writeSessionRecords(List<SessionRecord> rows) throws SQLException {
        try (SessionUpserter u = upserterFactory.newSessionUpserter(writeConn, config.tableNames())) {
            for (SessionRecord s : rows) {
                u.addBatch(
                        s.sessionId(), s.playerUuid(), s.serverName(),
                        s.startedEpochMs(), s.lastActivityEpochMs(),
                        s.grimVersion(), s.clientBrand(), s.clientVersion(),
                        s.serverVersionString(),
                        s.replayClips().isEmpty() ? "[]" : serializeReplayClipsShim());
            }
            u.executeBatch();
        }
    }

    private void writeIdentityRecords(List<PlayerIdentity> rows) throws SQLException {
        try (IdentityUpserter u = upserterFactory.newIdentityUpserter(writeConn, config.tableNames())) {
            for (PlayerIdentity id : rows) {
                u.addBatch(id.uuid(), id.currentName(), id.firstSeenEpochMs(), id.lastSeenEpochMs());
            }
            u.executeBatch();
        }
    }

    private void writeSettingRecords(List<SettingRecord> rows) throws SQLException {
        try (SettingsUpserter u = upserterFactory.newSettingsUpserter(writeConn, config.tableNames())) {
            for (SettingRecord s : rows) {
                u.addBatch(s.scope().name(), s.scopeKey(), s.key(), s.value(), s.updatedEpochMs());
            }
            u.executeBatch();
        }
    }

    // --- read path ----------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <R> Page<R> read(@NotNull Category<?> cat, @NotNull Query<R> query) throws BackendException {
        try (Connection c = openConnection()) {
            if (query instanceof Queries.ListSessionsByPlayer q) {
                return (Page<R>) listSessionsByPlayer(c, q);
            }
            if (query instanceof Queries.GetSessionById q) {
                return (Page<R>) getSessionById(c, q);
            }
            if (query instanceof Queries.ListViolationsInSession q) {
                return (Page<R>) listViolationsInSession(c, q);
            }
            if (query instanceof Queries.GetPlayerIdentity q) {
                return (Page<R>) getPlayerIdentity(c, q);
            }
            if (query instanceof Queries.GetPlayerIdentityByName q) {
                return (Page<R>) getPlayerIdentityByName(c, q);
            }
            if (query instanceof Queries.ListPlayersByNamePrefix q) {
                return (Page<R>) listPlayersByNamePrefix(c, q);
            }
            if (query instanceof Queries.GetSetting q) {
                return (Page<R>) getSetting(c, q);
            }
            throw new BackendException("unsupported query: " + query.getClass().getSimpleName());
        } catch (SQLException e) {
            throw new BackendException("read failed", e);
        }
    }

    private Page<SessionRecord> listSessionsByPlayer(Connection c, Queries.ListSessionsByPlayer q) throws SQLException {
        long cursorStarted = decodeStartedCursor(q.cursor(), Long.MAX_VALUE);
        byte[] cursorSessionId = decodeSessionIdCursor(q.cursor());
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT session_id, player_uuid, server_name, started_at, last_activity, "
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
                "SELECT session_id, player_uuid, server_name, started_at, last_activity, "
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
        long lastOccurred = decodeViolationOccurredCursor(q.cursor(), Long.MIN_VALUE);
        long lastId = decodeViolationIdCursor(q.cursor());
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT id, session_id, player_uuid, check_id, vl, occurred_at, verbose, verbose_format "
                        + "FROM " + config.tableNames().violations() + " "
                        + "WHERE session_id = ? AND (occurred_at > ? OR (occurred_at = ? AND id > ?)) "
                        + "ORDER BY occurred_at ASC, id ASC "
                        + "LIMIT ?")) {
            ps.setBytes(1, UuidCodec.toBytes(q.sessionId()));
            ps.setLong(2, lastOccurred);
            ps.setLong(3, lastOccurred);
            ps.setLong(4, lastId);
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
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT uuid, current_name, first_seen, last_seen FROM " + config.tableNames().players() + " "
                        + "WHERE current_name_lower = ? ORDER BY last_seen DESC LIMIT 1")) {
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
        // Prefix match only — the index idx_<players>_name_lower is a B-tree
        // on current_name_lower so LIKE 'x%' stays sargable. Wildcards embedded
        // in the user-supplied prefix are escaped so '%' and '_' behave as
        // literals in the match.
        String escaped = escapeLike(prefix);
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT uuid, current_name, first_seen, last_seen FROM " + config.tableNames().players() + " "
                        + "WHERE current_name_lower LIKE ? ESCAPE '\\' "
                        + "ORDER BY last_seen DESC LIMIT ?")) {
            ps.setString(1, escaped + "%");
            ps.setInt(2, q.limit());
            try (ResultSet rs = ps.executeQuery()) {
                List<PlayerIdentity> out = new ArrayList<>();
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
                "SELECT scope, scope_key, key, value, updated_at FROM " + config.tableNames().settings() + " "
                        + "WHERE scope = ? AND scope_key = ? AND key = ?")) {
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
        return new SessionRecord(
                UuidCodec.fromBytes(rs.getBytes("session_id")),
                UuidCodec.fromBytes(rs.getBytes("player_uuid")),
                rs.getString("server_name"),
                rs.getLong("started_at"),
                rs.getLong("last_activity"),
                rs.getString("grim_version"),
                rs.getString("client_brand"),
                rs.getInt("client_version_pvn"),
                rs.getString("server_version"),
                List.of());
    }

    private static ViolationRecord mapViolation(ResultSet rs) throws SQLException {
        return new ViolationRecord(
                rs.getLong("id"),
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
            if (writeConn == null) throw new BackendException("backend not initialised");
            try {
                writeConn.setAutoCommit(false);
                ac.grim.grimac.api.storage.config.TableNames t = config.tableNames();
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
                                "DELETE FROM " + t.violations() + " WHERE session_id IN "
                                        + "(SELECT session_id FROM " + t.sessions() + " WHERE started_at < ?)")) {
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
                try { writeConn.rollback(); } catch (SQLException ignore) {}
                throw new BackendException("delete failed", e);
            } finally {
                try { writeConn.setAutoCommit(true); } catch (SQLException ignore) {}
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
        try (Connection c = openConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT COUNT(*) FROM " + config.tableNames().violations() + " WHERE session_id = ?")) {
            ps.setBytes(1, UuidCodec.toBytes(sessionId));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
                return 0L;
            }
        } catch (SQLException e) {
            throw new BackendException("countViolationsInSession failed", e);
        }
    }

    @Override
    public long countUniqueChecksInSession(@NotNull UUID sessionId) throws BackendException {
        try (Connection c = openConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT COUNT(DISTINCT check_id) FROM " + config.tableNames().violations() + " WHERE session_id = ?")) {
            ps.setBytes(1, UuidCodec.toBytes(sessionId));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
                return 0L;
            }
        } catch (SQLException e) {
            throw new BackendException("countUniqueChecksInSession failed", e);
        }
    }

    @Override
    public long countSessionsByPlayer(@NotNull UUID player) throws BackendException {
        try (Connection c = openConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT COUNT(*) FROM " + config.tableNames().sessions() + " WHERE player_uuid = ?")) {
            ps.setBytes(1, UuidCodec.toBytes(player));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
                return 0L;
            }
        } catch (SQLException e) {
            throw new BackendException("countSessionsByPlayer failed", e);
        }
    }

    // --- cursor helpers ----------------------------------------------------

    private static Cursor encodeStartedCursor(long started, UUID sessionId) {
        return new Cursor(started + ":" + sessionId.toString().replace("-", ""));
    }

    private static long decodeStartedCursor(Cursor c, long defaultVal) {
        if (c == null) return defaultVal;
        String t = c.token();
        int colon = t.indexOf(':');
        if (colon <= 0) return defaultVal;
        try {
            return Long.parseLong(t.substring(0, colon));
        } catch (NumberFormatException e) {
            return defaultVal;
        }
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

    private static Cursor encodeViolationCursor(long occurred, long id) {
        return new Cursor("v:" + occurred + ":" + id);
    }

    private static long decodeViolationOccurredCursor(Cursor c, long defaultVal) {
        if (c == null) return defaultVal;
        String[] parts = c.token().split(":");
        if (parts.length < 3) return defaultVal;
        try {
            return Long.parseLong(parts[1]);
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    private static long decodeViolationIdCursor(Cursor c) {
        if (c == null) return Long.MIN_VALUE;
        String[] parts = c.token().split(":");
        if (parts.length < 3) return Long.MIN_VALUE;
        try {
            return Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            return Long.MIN_VALUE;
        }
    }

    @ApiStatus.Internal
    public Connection writeConnection() {
        return writeConn;
    }

    @ApiStatus.Internal
    public String jdbcUrl() {
        return jdbcUrl;
    }

    /** Exposes the backend's effective table names for cross-cutting consumers
     * (copier, check persistence) that can't go through the ring. */
    @ApiStatus.Internal
    public ac.grim.grimac.api.storage.config.TableNames tableNames() {
        return config.tableNames();
    }

    /**
     * Copier-only hatch: shares the backend's write mutex so cross-cutting
     * bulk operations (delete-all, drop tables) don't race handler commits.
     * Do not hold while the backend is serving live ring traffic — callers
     * should quiesce rings first.
     */
    @ApiStatus.Internal
    public Object writeMutexForCopier() {
        return writeMutex;
    }
}
