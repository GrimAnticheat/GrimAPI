package ac.grim.grimac.api.storage.verbose;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VerboseTest {

    private static final VerboseRenderContext CTX = new VerboseRenderContext(-1, null);

    @Test
    void rendersPrimitivesWithLiteralText() {
        Verbose v = Verbose.of("delay={ulong}ms, diff={f64}, slot={sint}, ok={bool}, note={str}");
        byte[] data = v.write(new VerboseBuf())
                .ulong(42L).f64(1.5).sint(-3).bool(true).str("hi")
                .end().toByteArray();
        assertEquals("delay=42ms, diff=1.5, slot=-3, ok=true, note=hi", v.render(data, CTX));
    }

    @Test
    void appliesNumericFormat() {
        Verbose v = Verbose.of("diff={f64:%.4f}");
        byte[] data = v.write(new VerboseBuf()).f64(1.23456789).end().toByteArray();
        assertEquals("diff=1.2346", v.render(data, CTX));
    }

    @Test
    void rendersMcPosCursorAndSignedLong() {
        Verbose v = Verbose.of("pos={mcpos}, cursor={cursor}, id={slong}");
        byte[] data = v.write(new VerboseBuf())
                .mcPos(-30_000_000, -64, 29_999_999)
                .cursor(0.5f, 1.0f, -0.25f)
                .slong(-1234567890123L)
                .end().toByteArray();
        assertEquals(
                "pos=-30000000, -64, 29999999, cursor=0.5, 1.0, -0.25, id=-1234567890123",
                v.render(data, CTX));
    }

    @Test
    void optionalGroupRendersOnlyWhenGateTrue() {
        Verbose v = Verbose.of("action={uint}[, last={mcpos}]");
        byte[] present = v.write(new VerboseBuf())
                .uint(2).bool(true).mcPos(1, 2, 3)
                .end().toByteArray();
        byte[] absent = v.write(new VerboseBuf())
                .uint(2).bool(false).mcPos(0, 0, 0)
                .end().toByteArray();
        assertEquals("action=2, last=1, 2, 3", v.render(present, CTX));
        assertEquals("action=2", v.render(absent, CTX));
    }

    @Test
    void choiceGroupRendersSelectedBranch() {
        Verbose v = Verbose.of("[delay={ulong}ms|diff={f64}ms, balance={f64}ms], type={uint}");
        byte[] delayMode = v.write(new VerboseBuf())
                .bool(true).ulong(120L).f64(0).f64(0).uint(7)
                .end().toByteArray();
        byte[] diffMode = v.write(new VerboseBuf())
                .bool(false).ulong(0L).f64(2.5).f64(-1.5).uint(7)
                .end().toByteArray();
        assertEquals("delay=120ms, type=7", v.render(delayMode, CTX));
        assertEquals("diff=2.5ms, balance=-1.5ms, type=7", v.render(diffMode, CTX));
    }

    @Test
    void choiceBranchMayBePureLiteral() {
        Verbose v = Verbose.of("last=[{mcpos}|null]");
        byte[] present = v.write(new VerboseBuf()).bool(true).mcPos(5, 6, 7).end().toByteArray();
        byte[] absent = v.write(new VerboseBuf()).bool(false).mcPos(0, 0, 0).end().toByteArray();
        assertEquals("last=5, 6, 7", v.render(present, CTX));
        assertEquals("last=null", v.render(absent, CTX));
    }

    @Test
    void enumTagsRenderConstantNames() {
        VerboseTags.registerEnum("verbose_test_enum", Sample.values());
        VerboseTags.registerEnumLower("verbose_test_enum_lower", Sample.values());
        Verbose v = Verbose.of("a={verbose_test_enum}, b={verbose_test_enum_lower}");
        byte[] data = v.write(new VerboseBuf())
                .uint(VerboseTags.enumId(Sample.SECOND))
                .uint(VerboseTags.enumId(null))
                .end().toByteArray();
        assertEquals("a=SECOND, b=null", v.render(data, CTX));
    }

    @Test
    void writerRejectsWrongFieldType() {
        Verbose v = Verbose.of("delay={ulong}ms");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> v.write(new VerboseBuf()).f64(1.0));
        assertTrue(e.getMessage().contains("delay"));
    }

    @Test
    void writerRejectsMissingAndExtraFields() {
        Verbose v = Verbose.of("a={uint}, b={uint}");
        assertThrows(IllegalStateException.class, () -> v.write(new VerboseBuf()).uint(1).end());
        assertThrows(IllegalStateException.class, () -> v.write(new VerboseBuf()).uint(1).uint(2).uint(3));
    }

    @Test
    void versionIsContentDerivedAndStable() {
        Verbose a = Verbose.of("slot={sint}");
        Verbose b = Verbose.of("slot={sint} ");
        assertTrue(a.version() >= 1);
        assertNotEquals(a.version(), b.version());
        assertSame(a, Verbose.of("slot={sint}"));
        assertEquals(a.version(), Verbose.of("slot={sint}").version());
    }

    @Test
    void layoutBytesRoundTripTemplateAndFields() {
        Verbose v = Verbose.of("delay={ulong}ms, pos={mcpos}");
        VerboseSchema.Layout layout = VerboseSchema.decodeLayout(v.layoutBytes());
        assertEquals("delay={ulong}ms, pos={mcpos}", layout.template());
        assertEquals(
                List.of(VerboseSchema.TypeTag.VL, VerboseSchema.TypeTag.VL, VerboseSchema.TypeTag.ZZ),
                layout.fields().stream().map(VerboseSchema.Field::type).toList());
        assertEquals("delay", layout.fields().get(0).name());
    }

    @Test
    void layoutBytesWithoutTemplateStillDecode() {
        VerboseSchema schema = VerboseSchema.of("slot:zz");
        VerboseSchema.Layout layout = VerboseSchema.decodeLayout(schema.layoutBytes());
        assertNull(layout.template());
        assertEquals(1, layout.fields().size());
    }

    @Test
    void renderStoredFallsBackNullOnUnknownTag() {
        byte[] data = new VerboseBuf().vi(1).toByteArray();
        assertNull(Verbose.renderStored("x={no_such_tag_registered}", data, CTX));
    }

    @Test
    void renderStoredDecodesForeignTemplate() {
        // Simulates a row + template written by another build: only the
        // template string and bytes survive, no shared Verbose instance.
        Verbose writer = Verbose.of("time={ulong}ms, positions={uint}");
        byte[] data = writer.write(new VerboseBuf()).ulong(95L).uint(3).end().toByteArray();
        assertEquals("time=95ms, positions=3",
                Verbose.renderStored("time={ulong}ms, positions={uint}", data, CTX));
    }

    @Test
    void escapesLiteralBrackets() {
        Verbose v = Verbose.of("\\[{uint}\\] \\{x\\}");
        byte[] data = v.write(new VerboseBuf()).uint(9).end().toByteArray();
        assertEquals("[9] {x}", v.render(data, CTX));
    }

    @Test
    void renderReturnsEmptyOnTruncatedPayload() {
        Verbose v = Verbose.of("a={uint}, b={f64}");
        byte[] truncated = new VerboseBuf().vi(1).toByteArray();
        assertEquals("", v.render(truncated, CTX));
    }

    @Test
    void shapesSelectByVarintAndCarryOnlySelectedFields() {
        Verbose v = Verbose.of("expectedSlot={sint}, slot={sint}")
                .or("invalid title '{str}'")
                .or("no nbt");
        byte[] slot = v.write(new VerboseBuf(), 0).sint(3).sint(7).end().toByteArray();
        byte[] title = v.write(new VerboseBuf(), 1).str("a".repeat(5)).end().toByteArray();
        byte[] noNbt = v.write(new VerboseBuf(), 2).end().toByteArray();
        assertEquals("expectedSlot=3, slot=7", v.render(slot, CTX));
        assertEquals("invalid title 'aaaaa'", v.render(title, CTX));
        assertEquals("no nbt", v.render(noNbt, CTX));
        // selector + selected fields only — the literal-only shape is 1 byte
        assertEquals(1, noNbt.length);
    }

    @Test
    void orChainingInternsToPipeSeparatedTemplate() {
        Verbose chained = Verbose.of("a={uint}").or("b={uint}");
        assertSame(Verbose.of("a={uint}|b={uint}"), chained);
        assertEquals("a={uint}|b={uint}", chained.template());
    }

    @Test
    void shapeWritersValidateSelectorAndArity() {
        Verbose multi = Verbose.of("a={uint}").or("b={str}");
        Verbose single = Verbose.of("a={uint}");
        assertThrows(IllegalStateException.class, () -> multi.write(new VerboseBuf()));
        assertThrows(IllegalStateException.class, () -> single.write(new VerboseBuf(), 0));
        assertThrows(IllegalArgumentException.class, () -> multi.write(new VerboseBuf(), 2));
        // wrong shape's field type
        assertThrows(IllegalStateException.class, () -> multi.write(new VerboseBuf(), 1).uint(1));
    }

    @Test
    void shapeLayoutRoundTripsThroughStoredTemplate() {
        Verbose v = Verbose.of("delay={ulong}ms").or("no nbt");
        VerboseSchema.Layout layout = VerboseSchema.decodeLayout(v.layoutBytes());
        assertEquals("delay={ulong}ms|no nbt", layout.template());
        // persisted flat list = selector + all shapes' fields
        assertEquals("shape", layout.fields().get(0).name());
        byte[] data = v.write(new VerboseBuf(), 1).end().toByteArray();
        assertEquals("no nbt", Verbose.renderStored(layout.template(), data, CTX));
    }

    @Test
    void groupPipeStillMeansBranchInsideGroups() {
        Verbose v = Verbose.of("x=[{uint}|none], tail={uint}");
        byte[] data = v.write(new VerboseBuf()).bool(false).uint(0).uint(9).end().toByteArray();
        assertEquals("x=none, tail=9", v.render(data, CTX));
    }

    @Test
    void rejectsMalformedTemplates() {
        // Parsing is lazy; schema() forces it.
        assertThrows(IllegalArgumentException.class, () -> Verbose.of("{unclosed").schema());
        assertThrows(IllegalArgumentException.class, () -> Verbose.of("[unclosed {uint}").schema());
        assertThrows(IllegalArgumentException.class, () -> Verbose.of("no fields at all").schema());
        assertThrows(IllegalArgumentException.class, () -> Verbose.of("{}").schema());
    }

    private enum Sample {
        FIRST, SECOND
    }
}
