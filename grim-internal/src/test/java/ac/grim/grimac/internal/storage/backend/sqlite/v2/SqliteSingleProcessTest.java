package ac.grim.grimac.internal.storage.backend.sqlite.v2;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two servers on one SQLite file race for its write lock, and every race lost past
 * the busy timeout used to drop a batch and log a stack trace. Inside one process all
 * writes share one connection, so the second process is the cause: it must now be
 * refused at startup with a message that says what to do, while a reload inside the
 * same process keeps working.
 */
@DisplayName("A SQLite file is used by one process at a time")
class SqliteSingleProcessTest {

    private static final String FILE = "data/history.v1.db";

    @Test
    void reloadInTheSameProcessIsAllowed(@TempDir Path dir) throws Exception {
        SqliteBackendV2 first = open(dir);
        SqliteBackendV2 second = open(dir);
        first.close();
        second.close();
        open(dir).close();
    }

    @Test
    void anotherProcessIsRefusedUntilItExits(@TempDir Path dir) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process holder = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                SqliteLockHolder.class.getName(), dir.toString(), FILE)
            .redirectErrorStream(true).start();
        try {
            BufferedReader out = new BufferedReader(new InputStreamReader(holder.getInputStream(), StandardCharsets.UTF_8));
            String line;
            while ((line = out.readLine()) != null && !line.equals("ready")) {
                // skip JVM noise
            }
            assertEquals("ready", line, "the other process opened the database");

            BackendException refused = assertThrows(BackendException.class, () -> open(dir));
            assertTrue(refused.getMessage().contains("another process"), refused.getMessage());
            assertTrue(refused.getMessage().contains("one server per file"), refused.getMessage());
        } finally {
            holder.getOutputStream().close();
            if (!holder.waitFor(30, TimeUnit.SECONDS)) holder.destroyForcibly().waitFor();
        }
        open(dir).close();
    }

    private static SqliteBackendV2 open(Path dir) throws BackendException {
        SqliteBackendConfig cfg = SqliteBackendConfig.defaults(FILE);
        SqliteBackendV2 backend = new SqliteBackendV2(cfg);
        backend.init(new BackendContext() {
            @Override public Logger logger() { return Logger.getAnonymousLogger(); }
            @Override public Path dataDirectory() { return dir; }
            @Override public BackendConfig config() { return cfg; }
        });
        return backend;
    }
}
