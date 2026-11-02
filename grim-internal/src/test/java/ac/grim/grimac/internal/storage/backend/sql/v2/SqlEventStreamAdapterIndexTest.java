package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.IndexSpec;
import ac.grim.grimac.api.storage.kind.ops.EventStreamOps;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqliteDialect;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import ac.grim.grimac.internal.storage.util.UuidV7;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@DisplayName("SQL v2 event stream: secondary index creation")
class SqlEventStreamAdapterIndexTest {

    private static final Map<String, String> EXPECTED = Map.of(
        "idx_grim_violations_session_id", "session_id",
        "idx_grim_violations_player_uuid", "player_uuid",
        "idx_grim_violations_check_id", "check_id",
        "idx_grim_violations_ts", "occurred_at");

    @TempDir Path tempDir;

    private SQLiteDataSource ds;
    private final List<LogRecord> logs = new ArrayList<>();
    private Logger logger;

    @BeforeEach
    void setUp() {
        ds = new SQLiteDataSource();
        ds.setUrl("jdbc:sqlite:" + tempDir.resolve("events.db").toAbsolutePath());
        logger = Logger.getAnonymousLogger();
        logger.setUseParentHandlers(false);
        logger.addHandler(new Handler() {
            @Override public void publish(LogRecord record) { logs.add(record); }
            @Override public void flush() {}
            @Override public void close() {}
        });
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static EventStream violations() {
        return (EventStream) V2BuiltinKinds.violations();
    }

    @SuppressWarnings("unchecked")
    @Test @DisplayName("fresh table gets all partition and timestamp indexes")
    void freshTable() throws Exception {
        SqlEventStreamAdapter adapter = new SqlEventStreamAdapter(ds, new SqliteDialect(), logger);
        adapter.ensureStore(StoreId.grim("grim_violations"), violations());
        assertEquals(new TreeMap<>(EXPECTED), secondaryIndexes("grim_violations"));
        assertTrue(warnings().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test @DisplayName("repeated ensureStore is quiet and keeps rows")
    void repeatedEnsureStore() throws Exception {
        StoreId id = StoreId.grim("grim_violations");
        EventStream kind = violations();
        SqlEventStreamAdapter adapter = new SqlEventStreamAdapter(ds, new SqliteDialect(), logger);
        adapter.ensureStore(id, kind);
        UUID player = UUID.randomUUID();
        UUID session = UUID.randomUUID();
        var handler = adapter.writeHandler(id, kind, Categories.VIOLATION);
        for (int i = 0; i < 3; i++) {
            handler.onEvent(violation(session, player, 1_700_000_000_000L, i), 0, i == 2);
        }
        adapter.ensureStore(id, kind);
        adapter.ensureStore(id, kind);
        assertEquals(new TreeMap<>(EXPECTED), secondaryIndexes("grim_violations"));
        assertEquals(3L, (long) adapter.execute(id, kind,
            new EventStreamOps.CountOp(Categories.VIOLATION, "player_uuid", player)));
        assertTrue(warnings().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test @DisplayName("table with some indexes missing is repaired")
    void partialRepair() throws Exception {
        StoreId id = StoreId.grim("grim_violations");
        SqlEventStreamAdapter adapter = new SqlEventStreamAdapter(ds, new SqliteDialect(), logger);
        adapter.ensureStore(id, violations());
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("DROP INDEX \"idx_grim_violations_check_id\"");
            s.executeUpdate("DROP INDEX \"idx_grim_violations_ts\"");
        }
        adapter.ensureStore(id, violations());
        assertEquals(new TreeMap<>(EXPECTED), secondaryIndexes("grim_violations"));
    }

    @SuppressWarnings("unchecked")
    @Test @DisplayName("a failing index warns with the cause and the rest are still created")
    void failingIndexWarns() throws Exception {
        SqliteDialect dialect = spy(new SqliteDialect());
        doReturn("CREATE INDEX broken syntax here").when(dialect)
            .createIndexSql(anyString(), any(IndexSpec.class), eq("idx_grim_violations_check_id"));
        SqlEventStreamAdapter adapter = new SqlEventStreamAdapter(ds, dialect, logger);
        adapter.ensureStore(StoreId.grim("grim_violations"), violations());

        Map<String, String> expected = new TreeMap<>(EXPECTED);
        expected.remove("idx_grim_violations_check_id");
        assertEquals(expected, secondaryIndexes("grim_violations"));

        List<LogRecord> warnings = warnings();
        assertEquals(1, warnings.size());
        LogRecord w = warnings.get(0);
        assertInstanceOf(SQLException.class, w.getThrown());
        assertTrue(w.getMessage().contains("idx_grim_violations_check_id"), w.getMessage());
        assertTrue(w.getMessage().contains("check_id"), w.getMessage());
        assertTrue(w.getMessage().contains("sqlite"), w.getMessage());
    }

    @SuppressWarnings("unchecked")
    @Test @DisplayName("errors the dialect classifies as duplicate index names are not logged")
    void duplicateIsQuiet() throws Exception {
        SqliteDialect dialect = spy(new SqliteDialect());
        doAnswer(inv -> "CREATE INDEX \"" + inv.getArgument(2) + "\" ON \"" + inv.getArgument(0)
            + "\" (\"" + ((IndexSpec) inv.getArgument(1)).fields().get(0) + "\")")
            .when(dialect).createIndexSql(anyString(), any(IndexSpec.class), anyString());
        doAnswer(inv -> ((SQLException) inv.getArgument(0)).getMessage().contains("already exists"))
            .when(dialect).isDuplicateIndexError(any(SQLException.class));
        SqlEventStreamAdapter adapter = new SqlEventStreamAdapter(ds, dialect, logger);
        StoreId id = StoreId.grim("grim_violations");
        adapter.ensureStore(id, violations());
        adapter.ensureStore(id, violations());
        assertEquals(new TreeMap<>(EXPECTED), secondaryIndexes("grim_violations"));
        assertTrue(warnings().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test @DisplayName("an unclassified error on an existing index still warns")
    void unclassifiedDuplicateWarns() throws Exception {
        SqliteDialect dialect = spy(new SqliteDialect());
        doAnswer(inv -> "CREATE INDEX \"" + inv.getArgument(2) + "\" ON \"" + inv.getArgument(0)
            + "\" (\"" + ((IndexSpec) inv.getArgument(1)).fields().get(0) + "\")")
            .when(dialect).createIndexSql(anyString(), any(IndexSpec.class), anyString());
        SqlEventStreamAdapter adapter = new SqlEventStreamAdapter(ds, dialect, logger);
        StoreId id = StoreId.grim("grim_violations");
        adapter.ensureStore(id, violations());
        assertTrue(warnings().isEmpty());
        adapter.ensureStore(id, violations());
        assertEquals(4, warnings().size());
    }

    @SuppressWarnings("unchecked")
    @Test @DisplayName("custom store names are used for table and index names")
    void customStoreName() throws Exception {
        SqlEventStreamAdapter adapter = new SqlEventStreamAdapter(ds, new SqliteDialect(), logger);
        adapter.ensureStore(StoreId.grim("srv-1_violations"), violations());
        Map<String, String> expected = new TreeMap<>();
        expected.put("idx_srv-1_violations_session_id", "session_id");
        expected.put("idx_srv-1_violations_player_uuid", "player_uuid");
        expected.put("idx_srv-1_violations_check_id", "check_id");
        expected.put("idx_srv-1_violations_ts", "occurred_at");
        assertEquals(expected, secondaryIndexes("srv-1_violations"));
        assertTrue(warnings().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test @DisplayName("connection failures are still fatal")
    void connectionFailureIsFatal() throws Exception {
        DataSource broken = mock(DataSource.class);
        SQLException cause = new SQLSyntaxErrorException("Duplicate key name 'x'", "42000", 1061);
        when(broken.getConnection()).thenThrow(cause);
        SqlEventStreamAdapter adapter = new SqlEventStreamAdapter(broken, new SqliteDialect(), logger);
        BackendException e = assertThrows(BackendException.class,
            () -> adapter.ensureStore(StoreId.grim("grim_violations"), violations()));
        assertSame(cause, e.getCause());
    }

    @SuppressWarnings("unchecked")
    @Test @DisplayName("table creation failures are still fatal")
    void tableFailureIsFatal() throws Exception {
        DataSource failing = mock(DataSource.class);
        Connection c = mock(Connection.class);
        Statement s = mock(Statement.class);
        SQLException cause = new SQLException("CREATE command denied", "42000", 1142);
        when(failing.getConnection()).thenReturn(c);
        when(c.createStatement()).thenReturn(s);
        when(s.executeUpdate(anyString())).thenThrow(cause);
        SqlEventStreamAdapter adapter = new SqlEventStreamAdapter(failing, new SqliteDialect(), logger);
        BackendException e = assertThrows(BackendException.class,
            () -> adapter.ensureStore(StoreId.grim("grim_violations"), violations()));
        assertSame(cause, e.getCause());
        assertTrue(warnings().isEmpty());
    }

    private static ViolationEvent violation(UUID session, UUID player, long ts, int seq) {
        return new ViolationEvent()
            .id(UuidV7.fromTimestampMs(ts, seq))
            .sessionId(session)
            .playerUuid(player)
            .checkId(7)
            .vl(1.0)
            .occurredEpochMs(ts);
    }

    private List<LogRecord> warnings() {
        return logs.stream().filter(r -> r.getLevel() == Level.WARNING).toList();
    }

    private Map<String, String> secondaryIndexes(String table) throws SQLException {
        Map<String, String> out = new TreeMap<>();
        try (Connection c = ds.getConnection();
             ResultSet rs = c.getMetaData().getIndexInfo(null, null, table, false, false)) {
            while (rs.next()) {
                String name = rs.getString("INDEX_NAME");
                if (name == null || name.startsWith("sqlite_autoindex")) continue;
                assertTrue(rs.getBoolean("NON_UNIQUE"), name + " is non-unique");
                assertFalse(out.containsKey(name), name + " is single-column");
                out.put(name, rs.getString("COLUMN_NAME"));
            }
        }
        return out;
    }
}
