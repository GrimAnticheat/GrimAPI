package ac.grim.grimac.internal.storage.verbose;

import ac.grim.grimac.api.storage.verbose.VerboseBuf;
import ac.grim.grimac.api.storage.verbose.VerboseRenderContext;
import ac.grim.grimac.api.storage.verbose.VerboseSchema;
import ac.grim.grimac.api.storage.verbose.VerboseSink;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GenericVerboseReaderTest {

    private static final VerboseRenderContext UNKNOWN_CONTEXT = new VerboseRenderContext(-1, null);

    @Test
    void rendersKnownPayloadAgainstLayout() throws Exception {
        VerboseSchema schema = VerboseSchema.of(
                "offset:f64",
                "count:vi",
                "delta:zz",
                "ok:bool",
                "note:str");
        VerboseBuf payload = schema.write(new VerboseBuf())
                .f64(1.25)
                .vi(300)
                .zz(-42)
                .bool(true)
                .str("hello");

        StringBuilder rendered = new StringBuilder();
        GenericVerboseReader.render(
                VerboseSchema.decodeLayout(schema.layoutBytes()),
                VerboseBuf.wrap(payload.toByteArray()),
                UNKNOWN_CONTEXT,
                VerboseSink.into(rendered));

        assertEquals("offset=1.25, count=300, delta=-42, ok=true, note=hello", rendered.toString());
    }

    @Test
    void truncatedPayloadSignalsUnderflow() {
        VerboseSchema schema = VerboseSchema.of("offset:f64", "count:vi");
        byte[] truncated = new VerboseBuf().f64(1.25).toByteArray();

        assertThrows(GenericVerboseReader.UnderflowException.class, () ->
                GenericVerboseReader.render(
                        VerboseSchema.decodeLayout(schema.layoutBytes()),
                        VerboseBuf.wrap(truncated),
                        UNKNOWN_CONTEXT,
                        VerboseSink.into(new StringBuilder())));
    }
}
