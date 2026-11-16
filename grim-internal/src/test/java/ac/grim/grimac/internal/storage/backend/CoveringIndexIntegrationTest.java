package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.mongo.MongoBackendConfig;
import ac.grim.grimac.internal.storage.backend.mongo.v2.MongoBackendV2;
import ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendConfig;
import ac.grim.grimac.internal.storage.backend.mysql.v2.MysqlBackendV2;
import ac.grim.grimac.internal.storage.backend.postgres.PostgresBackendConfig;
import ac.grim.grimac.internal.storage.backend.postgres.v2.PostgresBackendV2;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.MysqlDialect;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.PostgresDialect;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqliteDialect;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;
import ac.grim.grimac.internal.storage.backend.sqlite.v2.SqliteBackendV2;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.Socket;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The report this covers: an operator replaces grim_violations' single-column
 * indexes with composite ones that lead with the same columns, and Grim must not
 * recreate the defaults on the next start. SQLite always runs; the other
 * databases run when reachable (same {@code grim.test.*} properties as
 * {@link V2BackendIntegrationTest}).
 */
@DisplayName("ensureStore skips a default index that an existing index covers")
class CoveringIndexIntegrationTest {

    private static final Logger LOG = Logger.getLogger("CoveringIndexIntegrationTest");

    @Test
    void sqlite(@TempDir Path dir) throws Exception {
        SqliteBackendConfig cfg = SqliteBackendConfig.defaults("data/covering.db");
        SqliteBackendV2 backend = new SqliteBackendV2(cfg);
        try {
            backend.init(ctx(cfg, dir));
            String url = "jdbc:sqlite:" + dir.resolve("data/covering.db");
            violationsScenario(backend, new SqliteDialect(), () -> DriverManager.getConnection(url),
                "covidx_violations", true);
            sessionsScenario(backend, new SqliteDialect(), () -> DriverManager.getConnection(url));
        } finally {
            backend.close();
        }
    }

    @Test
    void postgres() throws Exception {
        String host = System.getProperty("grim.test.postgres.host", "localhost");
        int port = Integer.getInteger("grim.test.postgres.port", 5432);
        assumeReachable(host, port);
        String database = System.getProperty("grim.test.postgres.database", "grim");
        String user = System.getProperty("grim.test.postgres.user", "postgres");
        String password = System.getProperty("grim.test.postgres.password", "grim-test-postgres");
        PostgresBackendConfig cfg = new PostgresBackendConfig(host, port, database, user, password, "", 256,
            TableNames.DEFAULTS);
        PostgresBackendV2 backend = new PostgresBackendV2(cfg);
        try {
            backend.init(ctx(cfg, Path.of("/tmp")));
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
            violationsScenario(backend, new PostgresDialect(), () -> DriverManager.getConnection(url, user, password),
                "covidx_violations_" + Long.toHexString(System.nanoTime()), true);
        } finally {
            backend.close();
        }
    }

    /** An index in another collation or operator class orders text differently, so it covers no default. */
    @Test
    void postgresIndexesWithAnotherCollationOrOperatorClassDoNotCover() throws Exception {
        String host = System.getProperty("grim.test.postgres.host", "localhost");
        int port = Integer.getInteger("grim.test.postgres.port", 5432);
        assumeReachable(host, port);
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + System.getProperty("grim.test.postgres.database", "grim");
        String table = "covidx_collation_" + Long.toHexString(System.nanoTime());
        try (Connection c = DriverManager.getConnection(url, System.getProperty("grim.test.postgres.user", "postgres"),
                System.getProperty("grim.test.postgres.password", "grim-test-postgres"));
             Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE " + table + " (server_name TEXT, n INTEGER)");
            try {
                s.executeUpdate("CREATE INDEX " + table + "_plain ON " + table + " (server_name)");
                s.executeUpdate("CREATE INDEX " + table + "_c ON " + table + " (server_name COLLATE \"C\")");
                s.executeUpdate("CREATE INDEX " + table + "_pattern ON " + table + " (server_name text_pattern_ops)");
                s.executeUpdate("CREATE INDEX " + table + "_mixed ON " + table + " (n, server_name COLLATE \"C\")");
                s.executeUpdate("CREATE INDEX " + table + "_int ON " + table + " (n)");
                Map<String, Boolean> usable = new TreeMap<>();
                for (IndexCoverage.Existing index : new PostgresDialect().existingIndexes(c, table)) {
                    usable.put(index.name(), index.usable());
                }
                assertEquals(new TreeMap<>(Map.of(table + "_plain", true, table + "_c", false, table + "_pattern", false,
                    table + "_mixed", false, table + "_int", true)), usable);
            } finally {
                s.executeUpdate("DROP TABLE " + table);
            }
        }
    }

    @Test
    void mysql() throws Exception {
        String host = System.getProperty("grim.test.mysql.host", "localhost");
        int port = Integer.getInteger("grim.test.mysql.port", 3306);
        assumeReachable(host, port);
        String database = System.getProperty("grim.test.mysql.database", "grim");
        String user = System.getProperty("grim.test.mysql.user", "grim");
        String password = System.getProperty("grim.test.mysql.password", "grim-test-mysql");
        MysqlBackendConfig cfg = new MysqlBackendConfig(host, port, database, user, password, "", 256,
            TableNames.DEFAULTS);
        MysqlBackendV2 backend = new MysqlBackendV2(cfg);
        try {
            backend.init(ctx(cfg, Path.of("/tmp")));
            String url = "jdbc:mysql://" + host + ":" + port + "/" + database;
            // MySQL and MariaDB have no partial indexes.
            violationsScenario(backend, new MysqlDialect(), () -> DriverManager.getConnection(url, user, password),
                "covidx_violations_" + Long.toHexString(System.nanoTime()), false);
        } finally {
            backend.close();
        }
    }

    @Test
    void mongo() throws Exception {
        String uri = System.getProperty("grim.test.mongo.uri",
            "mongodb://root:grim-test-mongo@localhost:27017/?authSource=admin");
        ServerAddress address = new ServerAddress(new ConnectionString(uri).getHosts().get(0));
        assumeReachable(address.getHost(), address.getPort());
        String database = "covering_index_test";
        MongoBackendConfig cfg = new MongoBackendConfig(uri, database, 64, TableNames.DEFAULTS);
        MongoBackendV2 backend = new MongoBackendV2(cfg);
        try (MongoClient client = MongoClients.create(uri)) {
            backend.init(ctx(cfg, Path.of("/tmp")));
            EventStream<ViolationEvent, ViolationRecord> kind = V2BuiltinKinds.violations();
            KindAdapter<EventStream<ViolationEvent, ViolationRecord>> adapter = backend.adapterFor(kind).orElseThrow();
            String name = "covidx_violations_" + Long.toHexString(System.nanoTime());
            StoreId id = StoreId.grim(name);
            MongoCollection<Document> c = client.getDatabase(database).getCollection(name);

            adapter.ensureStore(id, kind);
            assertTrue(mongoIndexes(c).contains("meta.session_id_1"), "defaults are created on a new collection");

            // The (meta.session_id, occurred_at) compound covers the single-field index.
            c.dropIndex("meta.session_id_1");
            // The operator's (meta.player_uuid, occurred_at, id) covers both player defaults.
            c.createIndex(Indexes.ascending("meta.player_uuid", "occurred_at", "id"), new com.mongodb.client.model
                .IndexOptions().name("dash_player"));
            c.dropIndex("meta.player_uuid_1");
            c.dropIndex("meta.player_uuid_1_occurred_at_1");
            adapter.ensureStore(id, kind);
            List<String> after = mongoIndexes(c);
            assertFalse(after.contains("meta.session_id_1"), after.toString());
            assertFalse(after.contains("meta.player_uuid_1"), after.toString());
            assertFalse(after.contains("meta.player_uuid_1_occurred_at_1"), after.toString());

            // A partial index is not a substitute.
            c.createIndex(Indexes.ascending("meta.check_id", "occurred_at"), new com.mongodb.client.model
                .IndexOptions().name("partial_check").partialFilterExpression(new Document("vl", new Document("$gt", 1))));
            c.dropIndex("meta.check_id_1");
            c.dropIndex("meta.check_id_1_occurred_at_1");
            adapter.ensureStore(id, kind);
            after = mongoIndexes(c);
            assertTrue(after.contains("meta.check_id_1"), after.toString());
            assertTrue(after.contains("meta.check_id_1_occurred_at_1"), after.toString());
            c.drop();
        } finally {
            backend.close();
        }
    }

    /** The report's exact indexes, on every SQL dialect. */
    private void violationsScenario(BackendV2 backend, SqlDialect dialect, ConnectionFactory connections,
                                    String table, boolean partialIndexes) throws Exception {
        EventStream<ViolationEvent, ViolationRecord> kind = V2BuiltinKinds.violations();
        KindAdapter<EventStream<ViolationEvent, ViolationRecord>> adapter = backend.adapterFor(kind).orElseThrow();
        StoreId id = StoreId.grim(table);
        List<String> defaults = List.of("idx_" + table + "_session_id", "idx_" + table + "_player_uuid",
            "idx_" + table + "_check_id", "idx_" + table + "_ts");

        adapter.ensureStore(id, kind);
        try (Connection c = connections.open(); Statement s = c.createStatement()) {
            List<String> created = sqlIndexes(dialect, c, table);
            assertTrue(created.containsAll(lower(defaults)), "defaults are created on a new table: " + created);

            create(s, dialect, table, table + "_dash_session", "session_id", "occurred_at", "id");
            create(s, dialect, table, table + "_dash_time", "occurred_at", "id");
            create(s, dialect, table, table + "_dash_check", "check_id", "occurred_at");
            create(s, dialect, table, table + "_dash_player", "player_uuid", "occurred_at");
            for (String index : defaults) drop(s, dialect, table, index);
        }

        adapter.ensureStore(id, kind);
        try (Connection c = connections.open(); Statement s = c.createStatement()) {
            List<String> after = sqlIndexes(dialect, c, table);
            for (String index : defaults) {
                assertFalse(after.contains(index.toLowerCase(Locale.ROOT)), index + " was recreated: " + after);
            }

            // Without its covering index the default comes back.
            drop(s, dialect, table, table + "_dash_check");
            if (partialIndexes) {
                s.executeUpdate("CREATE INDEX " + dialect.quoteIdentifier(table + "_partial_check") + " ON "
                    + dialect.quoteIdentifier(table) + " (" + dialect.quoteIdentifier("check_id") + ", "
                    + dialect.quoteIdentifier("occurred_at") + ") WHERE " + dialect.quoteIdentifier("vl") + " > 1");
            }
        }
        adapter.ensureStore(id, kind);
        try (Connection c = connections.open(); Statement s = c.createStatement()) {
            List<String> after = sqlIndexes(dialect, c, table);
            assertTrue(after.contains(("idx_" + table + "_check_id").toLowerCase(Locale.ROOT)), after.toString());
            assertFalse(after.contains(("idx_" + table + "_session_id").toLowerCase(Locale.ROOT)), after.toString());
            s.executeUpdate("DROP TABLE " + dialect.quoteIdentifier(table));
        }
    }

    /** An Entity default (by_started on -started_at) covered by an ascending composite. */
    private void sessionsScenario(BackendV2 backend, SqlDialect dialect, ConnectionFactory connections) throws Exception {
        Entity<?, ?, ?> kind = V2BuiltinKinds.sessions();
        @SuppressWarnings({"unchecked", "rawtypes"})
        KindAdapter<Entity<?, ?, ?>> adapter = (KindAdapter) backend.adapterFor(kind).orElseThrow();
        String table = "covidx_sessions";
        StoreId id = StoreId.grim(table);
        adapter.ensureStore(id, kind);
        try (Connection c = connections.open(); Statement s = c.createStatement()) {
            assertTrue(sqlIndexes(dialect, c, table).contains(table + "_by_started"));
            create(s, dialect, table, table + "_dash_started", "started_at", "closed_at");
            drop(s, dialect, table, table + "_by_started");
        }
        adapter.ensureStore(id, kind);
        try (Connection c = connections.open()) {
            List<String> after = sqlIndexes(dialect, c, table);
            assertFalse(after.contains(table + "_by_started"), after.toString());
            assertTrue(after.contains(table + "_by_player_started"), after.toString());
        }
    }

    private static void create(Statement s, SqlDialect dialect, String table, String name, String... columns)
            throws SQLException {
        List<String> quoted = new ArrayList<>();
        for (String column : columns) quoted.add(dialect.quoteIdentifier(column));
        s.executeUpdate("CREATE INDEX " + dialect.quoteIdentifier(name) + " ON " + dialect.quoteIdentifier(table)
            + " (" + String.join(", ", quoted) + ")");
    }

    private static void drop(Statement s, SqlDialect dialect, String table, String name) throws SQLException {
        String sql = "DROP INDEX " + dialect.quoteIdentifier(name);
        if (dialect.name().equals("mysql")) sql += " ON " + dialect.quoteIdentifier(table);
        s.executeUpdate(sql);
    }

    private static List<String> sqlIndexes(SqlDialect dialect, Connection c, String table) throws SQLException {
        List<String> names = new ArrayList<>();
        for (IndexCoverage.Existing index : dialect.existingIndexes(c, table)) {
            names.add(index.name().toLowerCase(Locale.ROOT));
        }
        return names;
    }

    private static List<String> mongoIndexes(MongoCollection<Document> c) {
        List<String> names = new ArrayList<>();
        for (Document index : c.listIndexes()) names.add(index.getString("name"));
        return names;
    }

    private static List<String> lower(List<String> names) {
        List<String> out = new ArrayList<>();
        for (String name : names) out.add(name.toLowerCase(Locale.ROOT));
        return out;
    }

    @FunctionalInterface
    private interface ConnectionFactory {
        Connection open() throws SQLException;
    }

    private static BackendContext ctx(BackendConfig cfg, Path dataDirectory) {
        return new BackendContext() {
            @Override public Logger logger() { return LOG; }
            @Override public Path dataDirectory() { return dataDirectory; }
            @Override public BackendConfig config() { return cfg; }
        };
    }

    private static void assumeReachable(String host, int port) {
        try (Socket s = new Socket(host, port)) {
            Assumptions.assumeTrue(s.isConnected());
        } catch (Exception e) {
            Assumptions.assumeTrue(false, host + ":" + port + " unreachable");
        }
    }
}
