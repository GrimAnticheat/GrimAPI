package ac.grim.grimac.internal.storage.util;

import org.jetbrains.annotations.ApiStatus;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * RFC 9562 §5.7 UUIDv7 generator. Top 48 bits are unix-ms, the next 4 are
 * version 7, then 12 bits of random ("rand_a"), the variant nibble, and 62
 * bits of random ("rand_b"). The time prefix makes UUIDv7s k-sortable, so
 * unique-index B-trees stay write-friendly and cursor paging works with a
 * single {@code WHERE id > ?} predicate.
 *
 * <p>Used by {@code ViolationSink} to mint ids producer-side and by the
 * per-backend schema migrations to synthesise stable replacement ids from
 * legacy {@code occurred_at} timestamps.
 *
 * <p>{@link #next()} is strictly monotonic within a single JVM, even
 * across calls that land in the same wall-clock millisecond (RFC 9562
 * §6.2 method 1). For a given UUID timestamp, a 14-bit sequence packed
 * across rand_a and the top of rand_b advances by one so each new id
 * sorts strictly after the previous; the remaining 60 bits stay random.
 * If the sequence is exhausted before the wall clock advances, the UUID
 * timestamp advances logically by 1 ms rather than wrapping the counter.
 * The Redis backend pages violations by ZSET score=occurred_at; without
 * this, two flags raised in the same ms could be returned in UUID order
 * opposite to their event order, breaking the by-id monotonic-paging
 * contract the SQL backends rely on.
 *
 * <p>{@link #fromTimestampMs(long)} stays fully random — it's only used
 * during schema migration to synthesise replacement ids from legacy
 * rows, where the {@code occurred_at} field already encodes order.
 */
@ApiStatus.Internal
public final class UuidV7 {

    /** Largest representable 48-bit unsigned millisecond timestamp. */
    private static final long TS_MASK = (1L << 48) - 1L;
    private static final long VERSION_BITS = 0x7L << 12;
    private static final long VARIANT_BITS = 0x2L << 62;
    private static final long RAND_A_MASK = 0x0FFFL;
    private static final long RAND_B_MASK = 0x3FFFFFFFFFFFFFFFL;
    private static final int SEQ_MAX = 0x3FFF;

    // Same-ms monotonicity state. seq is a 14-bit counter — 12 bits in
    // rand_a + top 2 bits of rand_b — so byte order strictly increases
    // for each new mint within the same UUID timestamp.
    private static long lastMs = -1L;
    private static int seq;

    private UuidV7() {}

    /**
     * Mint a UUIDv7 from the current wall clock. Strictly monotonic
     * within a single JVM. If the system clock steps backwards (NTP),
     * the previous UUID timestamp is held; if more than 16384 ids are
     * minted for one timestamp, the UUID timestamp advances logically
     * rather than wrapping and minting non-monotonic ids. The violation
     * record's {@code occurred_at} remains the authoritative event time.
     */
    public static synchronized UUID next() {
        long now = Math.min(System.currentTimeMillis(), TS_MASK);
        if (now > lastMs) {
            lastMs = now;
            seq = 0;
        } else if (seq < SEQ_MAX) {
            seq++;
        } else {
            if (lastMs == TS_MASK) {
                throw new IllegalStateException("UUIDv7 timestamp exhausted");
            }
            lastMs++;
            seq = 0;
        }
        long randA = ((long) (seq >>> 2)) & RAND_A_MASK;                                // top 12 bits of seq
        long randBHigh = ((long) (seq & 0x3)) << 60;                                    // bottom 2 bits in rand_b MSBs
        long randBLow = ThreadLocalRandom.current().nextLong() & 0x0FFFFFFFFFFFFFFFL;   // remaining 60 random bits
        long msb = (lastMs << 16) | VERSION_BITS | randA;
        long lsb = VARIANT_BITS | randBHigh | randBLow;
        return new UUID(msb, lsb);
    }

    /**
     * Mint a UUIDv7 whose timestamp prefix encodes {@code tsMs} (clamped into
     * the 48-bit unsigned range). Used for migration: a legacy {@code long id}
     * row's replacement UUID is synthesised from its {@code occurred_at} so
     * the new ids preserve the original chronological ordering.
     *
     * <p>Bogus inputs (negative ts, ts past year ~10895) are clamped rather
     * than rejected so a single bad legacy row doesn't abort migration.
     */
    public static UUID fromTimestampMs(long tsMs) {
        long ts = Math.max(0L, tsMs) & TS_MASK;
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long msb = (ts << 16) | VERSION_BITS | (r.nextLong() & RAND_A_MASK);
        long lsb = VARIANT_BITS | (r.nextLong() & RAND_B_MASK);
        return new UUID(msb, lsb);
    }
}
