package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.BackendException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A write that keeps failing logs a few lines, not a stack trace every 10 seconds. */
@DisplayName("Repeated write failures are rate-limited")
class WriteFailureWarningTest {

    private static final long RATE = 10_000L;

    @Test
    void backsOffAndPrintsEachStackTraceOnce() {
        AtomicLong clock = new AtomicLong(1_000_000L);
        AtomicReference<String> failure = new AtomicReference<>("[SQLITE_BUSY] The database file is locked");
        List<LogRecord> logs = new ArrayList<>();
        DisruptorEventHandlerAdapter<Object> adapter = new DisruptorEventHandlerAdapter<>("player-identity",
            (event, sequence, endOfBatch) -> {
                String message = failure.get();
                if (message != null) throw new BackendException("player-identity write failed", new SQLException(message));
            }, RATE, logger(logs), clock::get);

        adapter.onEvent(new Object(), 0, true);
        assertEquals(1, logs.size());
        assertNotNull(logs.get(0).getThrown(), "the first failure carries its stack trace");

        // One failure a second for an hour.
        for (int i = 0; i < 3600; i++) {
            clock.addAndGet(1_000L);
            adapter.onEvent(new Object(), 0, true);
        }
        // Reports at 10, 30, 70, 150, 310 s, then every 300 s: 16 lines instead of 360.
        assertEquals(16, logs.size(), () -> messages(logs));
        for (LogRecord r : logs.subList(1, logs.size())) {
            assertNull(r.getThrown(), () -> messages(logs));
            assertTrue(r.getMessage().contains("SQLITE_BUSY"), r.getMessage());
            assertEquals(Level.WARNING, r.getLevel());
        }
        assertTrue(logs.get(1).getMessage().contains("dropped 10 event(s) in the last 10 s"), logs.get(1).getMessage());
        assertEquals(3601, adapter.droppedOnErrorTotal());

        // A different failure gets its own stack trace at the next report.
        failure.set("[SQLITE_FULL] database or disk is full");
        clock.addAndGet(300_000L);
        adapter.onEvent(new Object(), 0, true);
        assertNotNull(logs.get(logs.size() - 1).getThrown());

        // Writes recover; after five quiet minutes the next failure is reported at once.
        failure.set(null);
        adapter.onEvent(new Object(), 0, true);
        clock.addAndGet(DisruptorEventHandlerAdapter.MAX_WARN_INTERVAL_MS);
        int before = logs.size();
        failure.set("[SQLITE_FULL] database or disk is full");
        adapter.onEvent(new Object(), 0, true);
        assertEquals(before + 1, logs.size());
        clock.addAndGet(RATE);
        adapter.onEvent(new Object(), 0, true);
        assertEquals(before + 2, logs.size(), "the interval started over");
    }

    private static Logger logger(List<LogRecord> sink) {
        Logger logger = Logger.getAnonymousLogger();
        logger.setUseParentHandlers(false);
        logger.addHandler(new Handler() {
            @Override public void publish(LogRecord record) { sink.add(record); }
            @Override public void flush() {}
            @Override public void close() {}
        });
        return logger;
    }

    private static String messages(List<LogRecord> logs) {
        StringBuilder sb = new StringBuilder();
        for (LogRecord r : logs) sb.append(r.getThrown() != null).append(" ").append(r.getMillis()).append(" ").append(r.getMessage()).append("\n");
        return sb.toString();
    }
}
