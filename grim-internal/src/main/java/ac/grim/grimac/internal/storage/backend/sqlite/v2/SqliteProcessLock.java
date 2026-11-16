package ac.grim.grimac.internal.storage.backend.sqlite.v2;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Keeps one process per SQLite file. Inside a process every write goes through one
 * connection, so SQLite never reports {@code SQLITE_BUSY}; a second process on the same
 * file (another server pointed at the same data folder) races it for the write lock,
 * and each lost race past the busy timeout drops a batch and logs a warning.
 *
 * <p>The guard is an OS lock on {@code <database>.lock}, held while the backend is open
 * and released by the OS if the process dies, so it can never go stale. Backends of the
 * same process (a reload) share it. Where the file system cannot lock, the guard is
 * skipped.
 */
@ApiStatus.Internal
final class SqliteProcessLock implements AutoCloseable {

    private static final Map<Path, Held> HELD = new HashMap<>();

    private final Path key;
    private boolean closed;

    private SqliteProcessLock(Path key) {
        this.key = key;
    }

    /** Thrown when another process holds the lock. */
    static final class InUseException extends Exception {
        InUseException(String message) { super(message); }
    }

    /** Acquires the guard for {@code database}, or returns null where it cannot be taken. */
    static @Nullable SqliteProcessLock acquire(@NotNull Path database, @NotNull Logger logger) throws InUseException {
        Path key = database.toAbsolutePath().normalize();
        synchronized (HELD) {
            Held held = HELD.get(key);
            if (held != null) {
                held.users++;
                return new SqliteProcessLock(key);
            }
            Path lockFile = key.resolveSibling(key.getFileName() + ".lock");
            FileChannel channel = null;
            try {
                channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock lock = channel.tryLock();
                if (lock == null) {
                    channel.close();
                    throw new InUseException(key + " is already open in another process, most likely another"
                        + " server using the same SQLite file. SQLite supports one server per file: give each"
                        + " server its own file, or route these categories to MySQL, MariaDB, Postgres or Mongo"
                        + " to share history between servers.");
                }
                HELD.put(key, new Held(channel, lock));
                return new SqliteProcessLock(key);
            } catch (IOException | OverlappingFileLockException | UnsupportedOperationException e) {
                if (channel != null) {
                    try { channel.close(); } catch (IOException ignored) {}
                }
                logger.log(Level.FINE, e, () -> "could not lock " + lockFile + "; not guarding against a second process");
                return null;
            }
        }
    }

    @Override
    public void close() {
        synchronized (HELD) {
            if (closed) return;
            closed = true;
            Held held = HELD.get(key);
            if (held == null || --held.users > 0) return;
            HELD.remove(key);
            try {
                held.lock.release();
            } catch (IOException ignored) {
                // Closing the channel below releases it as well.
            }
            try {
                held.channel.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static final class Held {
        final FileChannel channel;
        final FileLock lock;
        int users = 1;

        Held(FileChannel channel, FileLock lock) {
            this.channel = channel;
            this.lock = lock;
        }
    }
}
