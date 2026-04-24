package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
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
import java.util.Set;
import java.util.UUID;

/**
 * MySQL 8.x backend. Same ring/handler model as the SQLite reference backend —
 * each category's {@link StorageEventHandler} owns a dedicated write connection
 * in {@code autoCommit=false}, reads open a fresh connection per call. No
 * connection pool is bundled; operators who need one can front the JDBC URL
 * with a server-side proxy (ProxySQL) or drop in HikariCP alongside the plugin.
 * <p>
 * Unsupported features gracefully throw {@link UnsupportedOperationException}
 * where the {@link Backend} contract allows it. The schema is the post-v5
 * baseline (sessions with {@code client_version_pvn}, violations with
 * {@code verbose_format} as INTEGER and a {@code metadata} column); there are
 * no pre-v5 MySQL databases to migrate from, so the linear applyVN pattern
 * used by SQLite isn't mirrored here.
 */
@ApiStatus.Internal
public final class MysqlBackend implements Backend {

    public static final String ID = "mysql";

    private final MysqlBackendConfig config;
    private final Object writeMutex = new Object();
    private final List<BatchingHandler<?>> handlers = new ArrayList<>();
    private final String insertViolations;
    private final String upsertSessions;
    private final String upsertIdentities;
    private final String upsertSettings;
    private Connection writeConn;

    public MysqlBackend(MysqlBackendConfig config) {
        this.config = config;
        TableNames t = config.tableNames();
        // MySQL 8.0+ aliased-row upsert: `... AS new ON DUPLICATE KEY UPDATE col = new.col`.
        // Avoids the deprecated VALUES(col) reference.
        this.insertViolations =
                "INSERT INTO " + t.violations() + "(session_id, player_uuid, check_id, vl, occurred_at, verbose, verbose_format) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)";
        this.upsertSessions =
                "INSERT INTO " + t.sessions() + "(session_id, player_uuid, server_name, started_at, last_activity, "
                        + "grim_version, client_brand, client_version_pvn, server_version, replay_clips_json) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS new "
                        + "ON DUPLICATE KEY UPDATE "
                        + "server_name=new.server_name, "
                        + "last_activity=new.last_activity, "
                        + "grim_version=new.grim_version, "
                        + "client_brand=new.client_brand, "
                        + "client_version_pvn=new.client_version_pvn, "
                        + "server_version=new.server_version, "
                        + "replay_clips_json=new.replay_clips_json";
        this.upsertIdentities =
                "INSERT INTO " + t.players() + "(uuid, current_name, first_seen, last_seen) "
                        + "VALUES (?, ?, ?, ?) AS new "
                        + "ON DUPLICATE KEY UPDATE "
                        + "current_name = new.current_name, "
                        + "first_seen = LEAST(" + t.players() + ".first_seen, new.first_seen), "
                        + "last_seen = GREATEST(" + t.players() + ".last_seen, new.last_seen)";
        this.upsertSettings =
                "INSERT INTO " + t.settings() + "(scope, scope_key, `key`, value, updated_at) "
                        + "VALUES (?, ?, ?, ?, ?) AS new "
                        + "ON DUPLICATE KEY UPDATE "
                        + "value = new.value, "
                        + "updated_at = new.updated_at";
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
        synchronized (writeMutex) {
            try {
                this.writeConn = openConnection();
                MysqlSchema.ensureInitialized(writeConn, "phase1", config.tableNames());
            } catch (SQLException e) {
                throw new BackendException("failed to initialise MySQL backend", e);
            }
        }
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(config.jdbcUrl(), config.user(),
                config.password() == null ? "" : config.password());
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
            if (conn == null) return;
            try {
                if (stmt == null) stmt = conn.prepareStatement(sql());
                bind(stmt, event);
                stmt.addBatch();
                pending++;
                if (endOfBatch || pending >= config.batchFlushCap()) flushLocked();
            } catch (SQLException e) {
                abortLocked();
                throw new BackendException(categoryId() + " write failed", e);
            }
        }

        private void flushLocked() throws SQLException {
            if (pending == 0) return;
            stmt.executeBatch();
            conn.commit();
            pending = 0;
        }

        private void abortLocked() {
            try { if (conn != null) conn.rollback(); } catch (SQLException ignore) {}
            try { if (stmt != null) { stmt.close(); stmt = null; } } catch (SQLException ignore) {}
            pending = 0;
        }

        synchronized void shutDown() {
            try { if (pending > 0 && stmt != null) { stmt.executeBatch(); conn.commit(); } }
            catch (SQLException ignore) {}
            try { if (stmt != null) stmt.close(); } catch (SQLException ignore) {}
            try { if (conn != null) conn.close(); } catch (SQLException ignore) {}
            stmt = null;
            conn = null;
            pending = 0;
        }

        protected abstract String sql();
        protected abstract String categoryId();
        protected abstract void bind(PreparedStatement ps, E event) throws SQLException;
    }

    private final class ViolationHandler extends BatchingHandler<ViolationEvent> {
        @Override protected String sql() { return insertViolations; }
        @Override protected String categoryId() { return "violation"; }
        @Override
        protected void bind(PreparedStatement ps, ViolationEvent v) throws SQLException {
            ps.setBytes(1, UuidCodec.toBytes(v.sessionId()));
            ps.setBytes(2, UuidCodec.toBytes(v.playerUuid()));
            ps.setInt(3, v.checkId());
            ps.setDouble(4, v.vl());
            ps.setLong(5, v.occurredEpochMs());
            ps.setString(6, v.verbose());
            ps.setInt(7, v.verboseFormat().code());
        }
    }

    private final class SessionHandler extends BatchingHandler<SessionEvent> {
        @Override protected String sql() { return upsertSessions; }
        @Override protected String categoryId() { return "session"; }
        @Override
        protected void bind(PreparedStatement ps, SessionEvent s) throws SQLException {
            ps.setBytes(1, UuidCodec.toBytes(s.sessionId()));
            ps.setBytes(2, UuidCodec.toBytes(s.playerUuid()));
            ps.setString(3, s.serverName());
            ps.setLong(4, s.startedEpochMs());
            ps.setLong(5, s.lastActivityEpochMs());
            ps.setString(6, s.grimVersion());
            ps.setString(7, s.clientBrand());
            ps.setInt(8, s.clientVersion());
            ps.setString(9, s.serverVersionString());
            ps.setString(10, s.replayClips().isEmpty() ? "[]" : serializeReplayClipsShim());
        }
    }

    private final class IdentityHandler extends BatchingHandler<PlayerIdentityEvent> {
        @Override protected String sql() { return upsertIdentities; }
        @Override protected String categoryId() { return "player-identity"; }
        @Override
        protected void bind(PreparedStatement ps, PlayerIdentityEvent e) throws SQLException {
            ps.setBytes(1, UuidCodec.toBytes(e.uuid()));
            ps.setString(2, e.currentName());
            ps.setLong(3, e.firstSeenEpochMs());
            ps.setLong(4, e.lastSeenEpochMs());
        }
    }

    private final class SettingHandler extends BatchingHandler<SettingEvent> {
        @Override protected String sql() { return upsertSettings; }
        @Override protected String categoryId() { return "setting"; }
        @Override
        protected void bind(PreparedStatement ps, SettingEvent s) throws SQLException {
            ps.setString(1, s.scope().name());
            ps.setString(2, s.scopeKey());
            ps.setString(3, s.key());
            ps.setBytes(4, s.value());
            ps.setLong(5, s.updatedEpochMs());
        }
    }

    private static String serializeReplayClipsShim() {
        throw new UnsupportedOperationException(
                "replay-clip serialisation isn't implemented by this backend; "
                        + "sessions with non-empty replayClips cannot be stored");
    }

    // --- bulk-load path (cross-backend copy target) -------------------------

    @Override
    public <R> void bulkImport(@NotNull Category<?> cat, @NotNull List<R> records) throws BackendException {
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
                throw new BackendException("bulkImport failed for " + cat.id(), e);
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
        try (PreparedStatement ps = writeConn.prepareStatement(upsertSessions)) {
            for (SessionRecord s : rows) {
                ps.setBytes(1, UuidCodec.toBytes(s.sessionId()));
                ps.setBytes(2, UuidCodec.toBytes(s.playerUuid()));
                ps.setString(3, s.serverName());
                ps.setLong(4, s.startedEpochMs());
                ps.setLong(5, s.lastActivityEpochMs());
                ps.setString(6, s.grimVersion());
                ps.setString(7, s.clientBrand());
                ps.setInt(8, s.clientVersion());
                ps.setString(9, s.serverVersionString());
                ps.setString(10, s.replayClips().isEmpty() ? "[]" : serializeReplayClipsShim());
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
                        + "WHERE LOWER(current_name) = ? ORDER BY last_seen DESC LIMIT 1")) {
            ps.setString(1, q.name().toLowerCase(Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return new Page<>(List.of(mapIdentity(rs)), null);
                return Page.empty();
            }
        }
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

    private static Cursor encodeViolationCursor(long occurred, long id) {
        return new Cursor("v:" + occurred + ":" + id);
    }

    private static long decodeViolationOccurredCursor(Cursor c, long defaultVal) {
        if (c == null) return defaultVal;
        String[] parts = c.token().split(":");
        if (parts.length < 3) return defaultVal;
        try { return Long.parseLong(parts[1]); }
        catch (NumberFormatException e) { return defaultVal; }
    }

    private static long decodeViolationIdCursor(Cursor c) {
        if (c == null) return Long.MIN_VALUE;
        String[] parts = c.token().split(":");
        if (parts.length < 3) return Long.MIN_VALUE;
        try { return Long.parseLong(parts[2]); }
        catch (NumberFormatException e) { return Long.MIN_VALUE; }
    }

    @ApiStatus.Internal
    public TableNames tableNames() {
        return config.tableNames();
    }
}
