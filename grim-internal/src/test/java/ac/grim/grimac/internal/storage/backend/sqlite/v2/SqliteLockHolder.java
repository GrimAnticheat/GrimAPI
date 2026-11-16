package ac.grim.grimac.internal.storage.backend.sqlite.v2;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;

import java.nio.file.Path;
import java.util.logging.Logger;

/** A second "server": opens the SQLite backend in its own process and holds it until stdin closes. */
public final class SqliteLockHolder {
    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        SqliteBackendConfig cfg = SqliteBackendConfig.defaults(args[1]);
        SqliteBackendV2 backend = new SqliteBackendV2(cfg);
        backend.init(new BackendContext() {
            @Override public Logger logger() { return Logger.getAnonymousLogger(); }
            @Override public Path dataDirectory() { return dir; }
            @Override public BackendConfig config() { return cfg; }
        });
        System.out.println("ready");
        System.out.flush();
        while (System.in.read() >= 0) {
            // hold the backend open
        }
        backend.close();
    }
}
