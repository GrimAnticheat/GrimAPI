package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.MysqlDialect;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlKeyValueScopedAdapterTest {

    @Test
    @DisplayName("MySQL KV DDL quotes the reserved key column")
    void mysqlDdlQuotesReservedKeyColumn() throws Exception {
        CapturingDataSource ds = new CapturingDataSource();
        SqlKeyValueScopedAdapter adapter = new SqlKeyValueScopedAdapter(
            ds, new MysqlDialect(), Logger.getLogger("sql-kv-test"));

        adapter.ensureStore(StoreId.grim("grim_settings"), V2BuiltinKinds.settings());

        String ddl = ds.sql();
        assertTrue(ddl.contains("`key` VARCHAR(255) NOT NULL"), ddl);
        assertTrue(ddl.contains("PRIMARY KEY (`scope`, `scope_key`, `key`)"), ddl);
        assertFalse(ddl.contains(" key VARCHAR"), ddl);
    }

    private static final class CapturingDataSource implements DataSource {
        private final AtomicReference<String> sql = new AtomicReference<>();

        String sql() {
            return sql.get();
        }

        @Override public Connection getConnection() {
            Statement statement = proxy(Statement.class, (proxy, method, args) -> {
                if (method.getName().equals("executeUpdate") && args != null && args.length == 1) {
                    sql.set((String) args[0]);
                    return 0;
                }
                return defaultValue(method.getReturnType());
            });
            return proxy(Connection.class, (proxy, method, args) -> switch (method.getName()) {
                case "createStatement" -> statement;
                case "isClosed" -> false;
                default -> defaultValue(method.getReturnType());
            });
        }

        @Override public Connection getConnection(String username, String password) {
            return getConnection();
        }

        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) {}
        @Override public void setLoginTimeout(int seconds) {}
        @Override public int getLoginTimeout() { return 0; }
        @Override public java.util.logging.Logger getParentLogger() {
            return java.util.logging.Logger.getGlobal();
        }
        @Override public <T> T unwrap(Class<T> iface) { return null; }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static Object defaultValue(Class<?> type) {
        if (type == void.class) return null;
        if (type == boolean.class) return false;
        if (type == byte.class) return (byte) 0;
        if (type == short.class) return (short) 0;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0F;
        if (type == double.class) return 0D;
        if (type == char.class) return '\0';
        return null;
    }
}
