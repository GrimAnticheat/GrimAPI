package ac.grim.grimac.api.storage.verbose;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VerboseSchemaTest {

    private static final VerboseRenderContext UNKNOWN_CONTEXT = new VerboseRenderContext(-1, null);

    @Test
    void layoutEncodeDecodeEncodeIsStable() {
        VerboseSchema schema = VerboseSchema.of(3,
                "offset:f64",
                "buffer:f32",
                "flagId:vi",
                "delta:zz",
                "age:vl",
                "ok:bool",
                "note:str",
                "face:enum");

        byte[] encoded = schema.layoutBytes();
        VerboseSchema.Layout decoded = VerboseSchema.decodeLayout(encoded);

        assertEquals(schema.fields(), decoded.fields());
        assertArrayEquals(encoded, decoded.layoutBytes());
        assertArrayEquals(encoded, VerboseSchema.fromLayoutBytes(encoded).layoutBytes());
    }

    @Test
    void posExpandsToThreeF32Fields() {
        VerboseSchema schema = VerboseSchema.of("pos:pos", "speed:f32");

        assertEquals(List.of(
                new VerboseSchema.Field("pos.x", VerboseSchema.TypeTag.F32),
                new VerboseSchema.Field("pos.y", VerboseSchema.TypeTag.F32),
                new VerboseSchema.Field("pos.z", VerboseSchema.TypeTag.F32),
                new VerboseSchema.Field("speed", VerboseSchema.TypeTag.F32)
        ), schema.fields());
    }

    @Test
    void formatterUsesSchemaOrder() {
        VerboseSchema schema = VerboseSchema.of(2, "offset:f64", "flagId:vi", "ok:bool");
        VerboseBuf payload = schema.write(new VerboseBuf()).f64(1.25).vi(7).bool(true);

        StringBuilder rendered = new StringBuilder();
        schema.formatter().render(VerboseBuf.wrap(payload.toByteArray()), UNKNOWN_CONTEXT, VerboseSink.into(rendered));

        assertEquals("offset=1.25, flagId=7, ok=true", rendered.toString());
        assertEquals(2, schema.formatter().version());
    }

    @Test
    void rejectsNonPositiveVersions() {
        assertThrows(IllegalArgumentException.class, () -> VerboseSchema.of(0, "offset:f64"));
        assertThrows(IllegalArgumentException.class, () -> VerboseSchema.of(-1, "offset:f64"));
        assertThrows(IllegalArgumentException.class,
                () -> VerboseSchema.of("offset:f64").withVersion(0));
    }

    @Test
    void assertionGuardRejectsWrongPrimitiveOrder() {
        assumeTrue(VerboseSchema.class.desiredAssertionStatus());
        VerboseSchema schema = VerboseSchema.of("offset:f64", "ok:bool");

        AssertionError error = assertThrows(AssertionError.class,
                () -> schema.write(new VerboseBuf()).f64(1.25).vi(7));

        assertTrue(error.getMessage().contains("declared bool"));
    }

    @Test
    void assertionGuardRejectsMissingFieldsAtSnapshot() {
        assumeTrue(VerboseSchema.class.desiredAssertionStatus());
        VerboseSchema schema = VerboseSchema.of("offset:f64", "ok:bool");

        AssertionError error = assertThrows(AssertionError.class,
                () -> schema.write(new VerboseBuf()).f64(1.25).toByteArray());

        assertTrue(error.getMessage().contains("wrote 1 fields but declared 2"));
    }
}
