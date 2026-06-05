package ac.grim.grimac.internal.storage.codec;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.model.CheckCatalogRecord;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.ServerInstanceRecord;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the build-time-captured codec bindings against the canonical reflective
 * validator. If the capture tool ever diverges from {@link CodecIntrospection}
 * — wrong shape, wrong accessor name, wrong ctor param order — these fail at
 * (un-obfuscated) test time, before the divergence can corrupt data in an
 * obfuscated build where the reflective path can't run.
 * <p>
 * The capture resource is on the test runtime classpath (built by the
 * {@code generateCodecBindings} task), so {@link CapturedBindings} is populated
 * and the codec factory exercises the captured path here.
 */
@DisplayName("Captured codec bindings parity (build-time capture vs reflection)")
class CapturedBindingParityTest {

    /** The records V2BuiltinKinds builds codecs for — all must be captured + correct.
     *  (SettingRecord is intentionally absent: settings are a KeyValueScoped kind,
     *  not a @Persistent codec record.) */
    private static final List<Class<?>> BUILTIN_RECORDS = List.of(
        ViolationRecord.class, SessionRecord.class, PlayerIdentity.class,
        ServerInstanceRecord.class, ServerStartupRecord.class, CheckCatalogRecord.class);

    @Test
    @DisplayName("every builtin record was captured at build time")
    void allCaptured() {
        for (Class<?> r : BUILTIN_RECORDS) {
            assertNotNull(CapturedBindings.shape(r),
                r.getName() + " has no captured binding — the capture tool didn't cover it");
            assertNotNull(CapturedBindings.layout(r), r.getName() + " has no captured layout");
        }
        assertTrue(CapturedBindings.size() >= BUILTIN_RECORDS.size(),
            "expected at least " + BUILTIN_RECORDS.size() + " captured records, got " + CapturedBindings.size());
    }

    @Test
    @DisplayName("captured shape equals the reflective shape")
    void shapeParity() {
        for (Class<?> r : BUILTIN_RECORDS) {
            EncodeShape reflective = CodecIntrospection.inspectReflective(r);
            EncodeShape captured = CapturedBindings.shape(r);
            assertEquals(reflective, captured, "shape mismatch for " + r.getName());
        }
    }

    @Test
    @DisplayName("captured layout (accessor names, ctor types, version) equals reflection")
    void layoutParity() {
        for (Class<?> r : BUILTIN_RECORDS) {
            EncodeShape shape = CodecIntrospection.inspectReflective(r);
            RecordLayout reflective = RecordLayout.fromReflection(r, shape);
            RecordLayout captured = CapturedBindings.layout(r);
            assertArrayEquals(reflective.accessorNames(), captured.accessorNames(),
                "accessorNames mismatch for " + r.getName());
            assertArrayEquals(reflective.ctorParamTypes(), captured.ctorParamTypes(),
                "ctorParamTypes mismatch for " + r.getName());
            assertEquals(reflective.version(), captured.version(), "version mismatch for " + r.getName());
        }
    }

    @Test
    @DisplayName("captured-path codec binds accessors + constructor by name/type")
    void accessorAndConstructorRoundTrip() {
        UUID id = UUID.fromString("00000000-0000-0000-0000-000000000001");
        PlayerIdentity original = new PlayerIdentity(id, "Bob", 100L, 200L);

        // Built via the captured layout (resource present on the test classpath).
        MethodHandleCodec<PlayerIdentity> codec =
            MethodHandleCodecFactory.get().baseCodecFor(PlayerIdentity.class);

        // findVirtual-bound accessors read the right values by encoded field name.
        assertEquals(id, codec.indexField(original, "uuid"));
        assertEquals("Bob", codec.indexField(original, "current_name"));
        assertEquals(100L, codec.indexField(original, "first_seen"));
        assertEquals(200L, codec.indexField(original, "last_seen"));

        // findConstructor-bound canonical ctor reconstructs the record.
        PlayerIdentity rebuilt = codec.construct(new Object[]{id, "Bob", 100L, 200L});
        assertEquals(original, rebuilt);
    }
}
