package ac.grim.grimac.internal.storage.backend.sqlite;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
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
import org.jetbrains.annotations.ApiStatus;

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
 * Phase-1 SQLite reference backend. Single writer connection (owned by the WriterLoop
 * thread that calls writeBatch / delete). Reads open a fresh connection per call —
 * WAL mode permits concurrent readers alongside the writer.
 */
@ApiStatus.Internal
public final class SqliteBackend implements Backend {

    public static final String ID = "sqlite";

    private final SqliteBackendConfig config;
    private String jdbcUrl;
    private Connection writeConn;

    public SqliteBackend(SqliteBackendConfig config) {
        this.config = config;
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public ApiVersion getApiVersion() {
        return ApiVersion.CURRENT;
    }

    @Override
    public EnumSet<Capability> capabilities() {
        return EnumSet.of(
                Capability.INDEXED_KV,
                Capability.TIMESERIES_APPEND,
                Capability.TRANSACTIONS,
                Capability.HISTORY,
                Capability.SETTINGS,
                Capability.PLAYER_IDENTITY);
    }

    @Override
    public Set<Category<?>> supportedCategories() {
        return Set.of(
                Categories.VIOLATION,
                Categories.SESSION,
                Categories.PLAYER_IDENTITY,
                Categories.SETTING);
    }

    @Override
    public synchronized void init(BackendContext ctx) throws BackendException {
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
            // Load driver explicitly — shaded deployments sometimes need this.
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException cnf) {
            throw new BackendException("sqlite-jdbc not on the classpath", cnf);
        }
        try {
            this.writeConn = openConnection();
            try (Statement s = writeConn.createStatement()) {
                s.execute("PRAGMA journal_mode=" + config.journalMode());
                s.execute("PRAGMA synchronous=" + config.synchronousMode());
                s.execute("PRAGMA busy_timeout=" + config.busyTimeoutMs());
                s.execute("PRAGMA cache_size=" + config.cachePages());
            }
            SqliteSchema.ensureInitialized(writeConn, "phase1");
        } catch (SQLException e) {
            throw new BackendException("failed to initialise SQLite backend", e);
        }
    }

    private Connection openConnection() throws SQLException {
        Connection c = DriverManager.getConnection(jdbcUrl);
        try (Statement s = c.createStatement()) {
            s.execute("PRAGMA busy_timeout=" + config.busyTimeoutMs());
        }
        return c;
    }

    @Override
    public synchronized void flush() {
        // WAL mode commits on each transaction; nothing extra to do here for phase 1.
    }

    @Override
    public synchronized void close() throws BackendException {
        try {
            if (writeConn != null) writeConn.close();
        } catch (SQLException e) {
            throw new BackendException("close failed", e);
        } finally {
            writeConn = null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <R> void writeBatch(Category<R> cat, List<R> records) throws BackendException {
        if (records.isEmpty()) return;
        if (writeConn == null) throw new BackendException("backend not initialised");
        try {
            writeConn.setAutoCommit(false);
            if (cat == Categories.VIOLATION) writeViolations((List<ViolationRecord>) records);
            else if (cat == Categories.SESSION) writeSessions((List<SessionRecord>) records);
            else if (cat == Categories.PLAYER_IDENTITY) writeIdentities((List<PlayerIdentity>) records);
            else if (cat == Categories.SETTING) writeSettings((List<SettingRecord>) records);
            else throw new BackendException("unsupported category: " + cat.id());
            writeConn.commit();
        } catch (SQLException e) {
            try { writeConn.rollback(); } catch (SQLException ignore) {}
            throw new BackendException("writeBatch failed for " + cat.id(), e);
        } finally {
            try { writeConn.setAutoCommit(true); } catch (SQLException ignore) {}
        }
    }

    private void writeViolations(List<ViolationRecord> rows) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(
                "INSERT INTO grim_violations(session_id, player_uuid, check_id, vl, occurred_at, verbose, verbose_format) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (ViolationRecord v : rows) {
                ps.setBytes(1, UuidCodec.toBytes(v.sessionId()));
                ps.setBytes(2, UuidCodec.toBytes(v.playerUuid()));
                ps.setInt(3, v.checkId());
                ps.setDouble(4, v.vl());
                ps.setLong(5, v.occurredEpochMs());
                ps.setString(6, v.verbose());
                ps.setString(7, v.verboseFormat().name());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void writeSessions(List<SessionRecord> rows) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(
                "INSERT INTO grim_sessions(session_id, player_uuid, server_name, started_at, last_activity, "
                        + "grim_version, client_brand, client_version, server_version, replay_clips_json) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        + "ON CONFLICT(session_id) DO UPDATE SET "
                        + "server_name=excluded.server_name, "
                        + "last_activity=excluded.last_activity, "
                        + "grim_version=excluded.grim_version, "
                        + "client_brand=excluded.client_brand, "
                        + "client_version=excluded.client_version, "
                        + "server_version=excluded.server_version, "
                        + "replay_clips_json=excluded.replay_clips_json")) {
            for (SessionRecord s : rows) {
                ps.setBytes(1, UuidCodec.toBytes(s.sessionId()));
                ps.setBytes(2, UuidCodec.toBytes(s.playerUuid()));
                ps.setString(3, s.serverName());
                ps.setLong(4, s.startedEpochMs());
                ps.setLong(5, s.lastActivityEpochMs());
                ps.setString(6, s.grimVersion());
                ps.setString(7, s.clientBrand());
                ps.setString(8, s.clientVersionString());
                ps.setString(9, s.serverVersionString());
                // Phase 1 stores replay clips as an empty JSON array. Phase 3 replay work
                // will introduce a proper codec; for now just record the shape.
                ps.setString(10, s.replayClips().isEmpty() ? "[]" : serializeReplayClipsShim(s));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static String serializeReplayClipsShim(SessionRecord s) {
        // Phase 1 never writes clips; this exists only so future non-empty lists hit a
        // clear "not implemented yet" failure rather than silently losing data.
        throw new UnsupportedOperationException(
                "replay clip serialization is phase-3 scope; phase 1 sessions must have empty replayClips");
    }

    private void writeIdentities(List<PlayerIdentity> rows) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(
                "INSERT INTO grim_players(uuid, current_name, first_seen, last_seen) "
                        + "VALUES (?, ?, ?, ?) "
                        + "ON CONFLICT(uuid) DO UPDATE SET "
                        + "current_name = excluded.current_name, "
                        + "first_seen = min(first_seen, excluded.first_seen), "
                        + "last_seen = max(last_seen, excluded.last_seen)")) {
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

    private void writeSettings(List<SettingRecord> rows) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(
                "INSERT INTO grim_settings(scope, scope_key, key, value, updated_at) "
                        + "VALUES (?, ?, ?, ?, ?) "
                        + "ON CONFLICT(scope, scope_key, key) DO UPDATE SET "
                        + "value = excluded.value, "
                        + "updated_at = excluded.updated_at")) {
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

    @Override
    @SuppressWarnings("unchecked")
    public <R> Page<R> read(Category<R> cat, Query<R> query) throws BackendException {
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
                        + "grim_version, client_brand, client_version, server_version, replay_clips_json "
                        + "FROM grim_sessions "
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
                while (rs.next() && out.size() < q.pageSize()) {
                    out.add(mapSession(rs));
                }
                Cursor next = null;
                if (rs.next()) {
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
                        + "grim_version, client_brand, client_version, server_version, replay_clips_json "
                        + "FROM grim_sessions WHERE session_id = ?")) {
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
                        + "FROM grim_violations "
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
                while (rs.next() && out.size() < q.pageSize()) {
                    out.add(mapViolation(rs));
                }
                Cursor next = null;
                if (rs.next()) {
                    ViolationRecord last = out.get(out.size() - 1);
                    next = encodeViolationCursor(last.occurredEpochMs(), last.id());
                }
                return new Page<>(out, next);
            }
        }
    }

    private Page<PlayerIdentity> getPlayerIdentity(Connection c, Queries.GetPlayerIdentity q) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT uuid, current_name, first_seen, last_seen FROM grim_players WHERE uuid = ?")) {
            ps.setBytes(1, UuidCodec.toBytes(q.uuid()));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return new Page<>(List.of(mapIdentity(rs)), null);
                return Page.empty();
            }
        }
    }

    private Page<PlayerIdentity> getPlayerIdentityByName(Connection c, Queries.GetPlayerIdentityByName q) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT uuid, current_name, first_seen, last_seen FROM grim_players "
                        + "WHERE lower(current_name) = ? ORDER BY last_seen DESC LIMIT 1")) {
            ps.setString(1, q.name().toLowerCase(Locale.ROOT));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return new Page<>(List.of(mapIdentity(rs)), null);
                return Page.empty();
            }
        }
    }

    private Page<SettingRecord> getSetting(Connection c, Queries.GetSetting q) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT scope, scope_key, key, value, updated_at FROM grim_settings "
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
                rs.getString("client_version"),
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
                VerboseFormat.valueOf(rs.getString("verbose_format")));
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
    public synchronized <R> void delete(Category<R> cat, DeleteCriteria criteria) throws BackendException {
        if (writeConn == null) throw new BackendException("backend not initialised");
        try {
            writeConn.setAutoCommit(false);
            if (criteria instanceof Deletes.ByPlayer d) {
                byte[] uuid = UuidCodec.toBytes(d.uuid());
                if (cat == Categories.VIOLATION) execDelete("DELETE FROM grim_violations WHERE player_uuid = ?", uuid);
                else if (cat == Categories.SESSION) {
                    // Children first: delete violations belonging to this player's sessions.
                    execDelete("DELETE FROM grim_violations WHERE player_uuid = ?", uuid);
                    execDelete("DELETE FROM grim_sessions WHERE player_uuid = ?", uuid);
                } else if (cat == Categories.PLAYER_IDENTITY) {
                    execDelete("DELETE FROM grim_players WHERE uuid = ?", uuid);
                } else if (cat == Categories.SETTING) {
                    try (PreparedStatement ps = writeConn.prepareStatement(
                            "DELETE FROM grim_settings WHERE scope = 'PLAYER' AND scope_key = ?")) {
                        ps.setString(1, d.uuid().toString());
                        ps.executeUpdate();
                    }
                } else {
                    throw new BackendException("unsupported category for delete: " + cat.id());
                }
            } else if (criteria instanceof Deletes.OlderThan d) {
                long cutoff = System.currentTimeMillis() - d.maxAgeMs();
                if (cat == Categories.SESSION) {
                    // Delete violations first to keep things consistent.
                    try (PreparedStatement ps = writeConn.prepareStatement(
                            "DELETE FROM grim_violations WHERE session_id IN "
                                    + "(SELECT session_id FROM grim_sessions WHERE started_at < ?)")) {
                        ps.setLong(1, cutoff);
                        ps.executeUpdate();
                    }
                    try (PreparedStatement ps = writeConn.prepareStatement(
                            "DELETE FROM grim_sessions WHERE started_at < ?")) {
                        ps.setLong(1, cutoff);
                        ps.executeUpdate();
                    }
                } else if (cat == Categories.VIOLATION) {
                    try (PreparedStatement ps = writeConn.prepareStatement(
                            "DELETE FROM grim_violations WHERE occurred_at < ?")) {
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

    private void execDelete(String sql, byte[] uuid) throws SQLException {
        try (PreparedStatement ps = writeConn.prepareStatement(sql)) {
            ps.setBytes(1, uuid);
            ps.executeUpdate();
        }
    }

    @Override
    public long countViolationsInSession(UUID sessionId) throws BackendException {
        try (Connection c = openConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT COUNT(*) FROM grim_violations WHERE session_id = ?")) {
            ps.setBytes(1, UuidCodec.toBytes(sessionId));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
                return 0L;
            }
        } catch (SQLException e) {
            throw new BackendException("countViolationsInSession failed", e);
        }
    }

    // --- cursor helpers ----------------------------------------------------
    // Session cursor = "<started_at>:<session_id_hex>"
    // Violation cursor = "v:<occurred_at>:<id>"

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

    // Exposed for LegacyMigrator / tests — direct connection access for bulk work
    // that needs to bypass the async queue.
    @ApiStatus.Internal
    public Connection writeConnection() {
        return writeConn;
    }

    @ApiStatus.Internal
    public String jdbcUrl() {
        return jdbcUrl;
    }
}
