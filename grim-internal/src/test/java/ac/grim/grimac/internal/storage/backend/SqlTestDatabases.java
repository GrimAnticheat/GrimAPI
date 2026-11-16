package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.config.TableNames;
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
import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import org.junit.jupiter.api.Assumptions;

import java.net.Socket;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * The SQL databases the integration tests run against: SQLite always, Postgres and
 * MySQL/MariaDB when reachable, through the same {@code grim.test.*} properties as
 * {@link V2BackendIntegrationTest}. Each backend logs into its own {@link Logs}.
 */
public final class SqlTestDatabases {

    private SqlTestDatabases() {}

    /** One initialised backend plus a way to open plain JDBC connections to the same database. */
    public record Target(String label, BackendV2 backend, SqlDialect dialect, ConnectionFactory connections, Logs logs)
            implements AutoCloseable {
        public Connection open() throws SQLException { return connections.open(); }

        /** A second backend on the same database, as a second server sharing it would have. */
        public Target reopen() throws Exception {
            return switch (dialect.name()) {
                case "sqlite" -> throw new UnsupportedOperationException("SQLite is single-server");
                case "postgres" -> postgres();
                default -> mysql();
            };
        }

        @Override public void close() throws Exception { backend.close(); }
    }

    @FunctionalInterface
    public interface ConnectionFactory {
        Connection open() throws SQLException;
    }

    public static Target sqlite(Path dir, String file) throws Exception {
        SqliteBackendConfig cfg = SqliteBackendConfig.defaults("data/" + file);
        SqliteBackendV2 backend = new SqliteBackendV2(cfg);
        Logs logs = new Logs();
        backend.init(ctx(cfg, dir, logs.logger()));
        String url = "jdbc:sqlite:" + dir.resolve("data/" + file);
        return new Target("sqlite", backend, new SqliteDialect(), () -> DriverManager.getConnection(url), logs);
    }

    public static Target postgres() throws Exception {
        String host = System.getProperty("grim.test.postgres.host", "localhost");
        int port = Integer.getInteger("grim.test.postgres.port", 5432);
        assumeReachable(host, port);
        String database = System.getProperty("grim.test.postgres.database", "grim");
        String user = System.getProperty("grim.test.postgres.user", "postgres");
        String password = System.getProperty("grim.test.postgres.password", "grim-test-postgres");
        PostgresBackendConfig cfg = new PostgresBackendConfig(host, port, database, user, password, "", 256,
            TableNames.DEFAULTS);
        PostgresBackendV2 backend = new PostgresBackendV2(cfg);
        Logs logs = new Logs();
        backend.init(ctx(cfg, Path.of(System.getProperty("java.io.tmpdir")), logs.logger()));
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        return new Target("postgres", backend, new PostgresDialect(),
            () -> DriverManager.getConnection(url, user, password), logs);
    }

    public static Target mysql() throws Exception {
        String host = System.getProperty("grim.test.mysql.host", "localhost");
        int port = Integer.getInteger("grim.test.mysql.port", 3306);
        assumeReachable(host, port);
        String database = System.getProperty("grim.test.mysql.database", "grim");
        String user = System.getProperty("grim.test.mysql.user", "grim");
        String password = System.getProperty("grim.test.mysql.password", "grim-test-mysql");
        MysqlBackendConfig cfg = new MysqlBackendConfig(host, port, database, user, password, "", 256,
            TableNames.DEFAULTS);
        MysqlBackendV2 backend = new MysqlBackendV2(cfg);
        Logs logs = new Logs();
        backend.init(ctx(cfg, Path.of(System.getProperty("java.io.tmpdir")), logs.logger()));
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;
        String version;
        try (Connection c = DriverManager.getConnection(url, user, password)) {
            version = c.getMetaData().getDatabaseProductVersion();
        }
        return new Target("mysql " + version, backend, new MysqlDialect(),
            () -> DriverManager.getConnection(url, user, password), logs);
    }

    /** Mongo, when reachable; it has no JDBC side, so it is a bare backend. */
    public static BackendV2 mongo(Logs logs) throws Exception {
        String uri = System.getProperty("grim.test.mongo.uri",
            "mongodb://root:grim-test-mongo@localhost:27017/?authSource=admin");
        ServerAddress address = new ServerAddress(new ConnectionString(uri).getHosts().get(0));
        assumeReachable(address.getHost(), address.getPort());
        MongoBackendConfig cfg = new MongoBackendConfig(uri, "grim_it", 64, TableNames.DEFAULTS);
        MongoBackendV2 backend = new MongoBackendV2(cfg);
        backend.init(ctx(cfg, Path.of(System.getProperty("java.io.tmpdir")), logs.logger()));
        return backend;
    }

    public static String uniqueTable(String prefix) {
        return prefix + "_" + Long.toHexString(System.nanoTime());
    }

    private static BackendContext ctx(BackendConfig cfg, Path dataDirectory, Logger logger) {
        return new BackendContext() {
            @Override public Logger logger() { return logger; }
            @Override public Path dataDirectory() { return dataDirectory; }
            @Override public BackendConfig config() { return cfg; }
        };
    }

    public static void assumeReachable(String host, int port) {
        try (Socket s = new Socket(host, port)) {
            Assumptions.assumeTrue(s.isConnected());
        } catch (Exception e) {
            Assumptions.assumeTrue(false, host + ":" + port + " unreachable");
        }
    }

    /** A logger that keeps what it was given, so tests can assert on warnings. */
    public static final class Logs extends Handler {
        private final Logger logger;
        private final List<LogRecord> records = new ArrayList<>();

        public Logs() {
            logger = Logger.getAnonymousLogger();
            logger.setUseParentHandlers(false);
            logger.setLevel(Level.ALL);
            logger.addHandler(this);
            setLevel(Level.ALL);
        }

        public Logger logger() { return logger; }

        public synchronized List<LogRecord> at(Level level) {
            List<LogRecord> out = new ArrayList<>();
            for (LogRecord r : records) if (r.getLevel().intValue() >= level.intValue()) out.add(r);
            return out;
        }

        public synchronized List<String> messages(Level level) {
            List<String> out = new ArrayList<>();
            for (LogRecord r : at(level)) out.add(r.getMessage());
            return out;
        }

        public synchronized void clear() { records.clear(); }

        @Override public synchronized void publish(LogRecord record) { records.add(record); }
        @Override public void flush() {}
        @Override public void close() {}
    }
}
