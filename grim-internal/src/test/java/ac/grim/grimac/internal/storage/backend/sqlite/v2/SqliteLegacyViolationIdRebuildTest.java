package ac.grim.grimac.internal.storage.backend.sqlite.v2;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import ac.grim.grimac.internal.storage.util.UuidCodec;
import ac.grim.grimac.internal.storage.util.UuidV7;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for GrimAnticheat/Grim#2768: SQLite databases whose
 * {@code grim_violations} table predates the UUIDv7 id scheme keep
 * {@code id INTEGER PRIMARY KEY AUTOINCREMENT} — the strict rowid alias —
 * so every v2 violation insert failed with SQLITE_MISMATCH. ensureStore
 * must rebuild the table with a binary id, carry every row (including
 * columns the v2 shape no longer declares), normalize text-backed UUID
 * columns to blobs so partition reads still match, and unblock the
 * write path.
 */
@DisplayName("SQLite v2: legacy INTEGER violation ids rebuilt to UUIDv7 blobs")
class SqliteLegacyViolationIdRebuildTest {

    private static final Logger LOG = Logger.getLogger("SqliteLegacyViolationIdRebuildTest");
    private static final String DB_RELATIVE = "data/violations.sqlite";
    private static final long BASE_TS = 1_700_000_000_000L;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test @DisplayName("ensureStore rebuilds the pre-v4 table and unblocks writes")
    void rebuildsLegacyIntegerIdTable(@TempDir Path tempDir) throws Exception {
        Path db = tempDir.resolve(DB_RELATIVE);
        Files.createDirectories(db.getParent());
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        seedLegacyTable(db, session, player);

        SqliteBackendConfig cfg = SqliteBackendConfig.defaults(DB_RELATIVE);
        SqliteBackendV2 backend = new SqliteBackendV2(cfg);
        try {
            backend.init(ctx(cfg, tempDir));
            EventStream kind = V2BuiltinKinds.violations();
            KindAdapter adapter = backend.adapterFor(kind).orElseThrow(
                () -> new AssertionError("no EventStream adapter"));
            StoreId id = StoreId.grim("grim_violations");

            adapter.ensureStore(id, kind);

            // The write path must no longer raise SQLITE_MISMATCH. The id
            // is a UUIDv7 (as on the real write path) so the ORDER BY id
            // assertion below stays deterministic.
            var handler = adapter.writeHandler(id, kind, Categories.VIOLATION);
            ViolationEvent event = new ViolationEvent()
                .id(UuidV7.fromTimestampMs(BASE_TS + 60_000, 0))
                .sessionId(session)
                .playerUuid(player)
                .checkId(7)
                .vl(5.0)
                .occurredEpochMs(BASE_TS + 60_000);
            handler.onEvent(event, 0, true);
        } finally {
            backend.close();
        }

        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath())) {
            try (ResultSet cols = c.getMetaData().getColumns(null, null, "grim_violations", "id")) {
                assertTrue(cols.next(), "id column present after rebuild");
                assertEquals("BLOB", cols.getString("TYPE_NAME").toUpperCase(java.util.Locale.ROOT));
            }
            List<Double> vlsInIdOrder = new ArrayList<>();
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery(
                     "SELECT id, vl, metadata, typeof(session_id) AS session_type, session_id"
                         + " FROM grim_violations ORDER BY id")) {
                while (rs.next()) {
                    byte[] idBytes = rs.getBytes("id");
                    assertNotNull(idBytes, "rebuilt id is binary");
                    assertEquals(16, idBytes.length, "rebuilt id is a 16-byte UUID");
                    assertEquals("blob", rs.getString("session_type"),
                        "UUID partition columns normalized to blobs");
                    assertEquals(session, UuidCodec.fromBytes(rs.getBytes("session_id")),
                        "partition value survives normalization");
                    vlsInIdOrder.add(rs.getDouble("vl"));
                    if (rs.getDouble("vl") < 5.0) {
                        assertEquals("meta" + (int) (rs.getDouble("vl") - 1), rs.getString("metadata"),
                            "legacy metadata column carried over");
                    }
                }
            }
            // Four legacy rows plus the new write; UUIDv7 byte order keeps
            // the legacy order — same-ms rows by old numeric id, then by ts.
            assertEquals(List.of(1.0, 2.0, 3.0, 4.0, 5.0), vlsInIdOrder);
        }
    }

    private static void seedLegacyTable(Path db, UUID session, UUID player) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE grim_violations ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                + "session_id BLOB NOT NULL, "
                + "player_uuid BLOB NOT NULL, "
                + "check_id INTEGER NOT NULL, "
                + "vl REAL NOT NULL, "
                + "occurred_at INTEGER NOT NULL, "
                + "verbose TEXT, "
                + "verbose_format INTEGER NOT NULL DEFAULT 0, "
                + "metadata TEXT)");
            try (PreparedStatement ins = c.prepareStatement(
                "INSERT INTO grim_violations "
                    + "(session_id, player_uuid, check_id, vl, occurred_at, verbose, verbose_format, metadata) "
                    + "VALUES (?, ?, ?, ?, ?, ?, 0, ?)")) {
                for (int i = 0; i < 4; i++) {
                    // last row stores its UUIDs as text — SQLite BLOB columns
                    // keep whatever storage class they're handed, and some
                    // legacy writers bound strings — to exercise the
                    // rebuild's blob normalization
                    if (i == 3) {
                        ins.setString(1, session.toString());
                        ins.setString(2, player.toString());
                    } else {
                        ins.setBytes(1, UuidCodec.toBytes(session));
                        ins.setBytes(2, UuidCodec.toBytes(player));
                    }
                    ins.setInt(3, 7);
                    ins.setDouble(4, i + 1);
                    // first two rows share a millisecond to exercise the
                    // deterministic same-ms ordering of the minted UUIDv7s
                    ins.setLong(5, i < 2 ? BASE_TS : BASE_TS + 5_000L * (i - 1));
                    ins.setString(6, "row" + i);
                    ins.setString(7, "meta" + i);
                    ins.executeUpdate();
                }
            }
        }
    }

    private static BackendContext ctx(BackendConfig cfg, Path dataDirectory) {
        return new BackendContext() {
            @Override public Logger logger() { return LOG; }
            @Override public Path dataDirectory() { return dataDirectory; }
            @Override public BackendConfig config() { return cfg; }
        };
    }
}
