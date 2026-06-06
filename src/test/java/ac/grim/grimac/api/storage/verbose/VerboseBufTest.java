package ac.grim.grimac.api.storage.verbose;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VerboseBufTest {

    @Test
    void roundTripsEveryPrimitiveType() {
        VerboseBuf out = new VerboseBuf(4);
        out.f64(Math.PI)
                .f32(1.5f)
                .vi(300)
                .zz(-42)
                .vl(1L << 40)
                .bool(true)
                .bool(false)
                .str("hello");

        VerboseBuf in = VerboseBuf.wrap(out.toByteArray());
        assertEquals(Math.PI, in.rf64());
        assertEquals(1.5f, in.rf32());
        assertEquals(300, in.rvi());
        assertEquals(-42, in.rzz());
        assertEquals(1L << 40, in.rvl());
        assertTrue(in.rbool());
        assertFalse(in.rbool());
        assertEquals("hello", in.rstr());
        assertEquals(0, in.remaining());
    }

    @Test
    void roundTripsVarintBoundaries() {
        int[] unsigned = {0, 1, 2, 126, 127, 128, 255, 300, 16_383, 16_384, 1 << 21, Integer.MAX_VALUE};
        int[] signed = {0, 1, -1, 63, -64, 64, -65, Integer.MAX_VALUE, Integer.MIN_VALUE};
        long[] longs = {0L, 1L, 127L, 128L, 16_383L, 16_384L, 1L << 32, Long.MAX_VALUE};

        VerboseBuf out = new VerboseBuf(1);
        for (int v : unsigned) out.vi(v);
        for (int v : signed) out.zz(v);
        for (long v : longs) out.vl(v);

        VerboseBuf in = VerboseBuf.wrap(out.toByteArray());
        for (int v : unsigned) assertEquals(v, in.rvi());
        for (int v : signed) assertEquals(v, in.rzz());
        for (long v : longs) assertEquals(v, in.rvl());
        assertEquals(0, in.remaining());
    }

    @Test
    void skipAdvancesWithoutDecoding() {
        VerboseBuf out = new VerboseBuf(8);
        out.f64(12.5).vi(321).str("skip-me").bool(true);

        VerboseBuf in = VerboseBuf.wrap(out.toByteArray());
        in.skip(VerboseSchema.TypeTag.F64.tag());
        assertEquals(321, in.rvi());
        in.skip(VerboseSchema.TypeTag.STR.tag());
        assertTrue(in.rbool());
        assertEquals(0, in.remaining());
    }

    @Test
    void wrapReadsBackingBytesWithoutCopying() {
        byte[] bytes = new VerboseBuf(2).vi(127).bool(true).toByteArray();
        VerboseBuf in = VerboseBuf.wrap(bytes);
        assertEquals(127, in.rvi());
        assertTrue(in.rbool());
        assertArrayEquals(bytes, in.array());
    }

    @Test
    void unsignedVarintWritersRejectNegativeValues() {
        assertThrows(IllegalArgumentException.class, () -> new VerboseBuf().vi(-1));
        assertThrows(IllegalArgumentException.class, () -> new VerboseBuf().vl(-1L));
    }
}
