package ac.grim.grimac.api.storage.verbose;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Global registry of {@link Verbose} template placeholder tags.
 *
 * <p>A tag binds a placeholder name (for example {@code {block}}) to the
 * primitive wire fields it occupies and the function that turns those wire
 * values back into display text. Primitive tags ({@code f64}, {@code vi},
 * {@code str}, ...) are built in; domain tags are registered once by the
 * consuming plugin before any template using them is written or rendered.
 *
 * <p>Registration is idempotent for an identical wire shape and append-only
 * otherwise: a tag's wire shape can never change, because stored payloads
 * decode through it forever. To change a tag's encoding, register a new tag
 * under a new name.
 */
@ApiStatus.Experimental
public final class VerboseTags {

    /** Encoded enum value used for {@code null} ({@link #enumId(Enum)}). */
    public static final int ENUM_NULL = 0;

    private static final int MC_XZ_BITS = 26;
    private static final int MC_XZ_MASK = (1 << MC_XZ_BITS) - 1;

    private static final ConcurrentMap<String, Tag> TAGS = new ConcurrentHashMap<>();

    static {
        registerPrimitive("f64", VerboseSchema.TypeTag.F64,
                (in, ctx, out, fmt) -> appendFormatted(out, fmt, in.rf64()));
        registerPrimitive("f32", VerboseSchema.TypeTag.F32,
                (in, ctx, out, fmt) -> appendFormatted(out, fmt, in.rf32()));
        registerPrimitive("uint", VerboseSchema.TypeTag.VI,
                (in, ctx, out, fmt) -> appendFormatted(out, fmt, in.rvi()));
        registerPrimitive("sint", VerboseSchema.TypeTag.ZZ,
                (in, ctx, out, fmt) -> appendFormatted(out, fmt, in.rzz()));
        registerPrimitive("ulong", VerboseSchema.TypeTag.VL,
                (in, ctx, out, fmt) -> appendFormatted(out, fmt, in.rvl()));
        registerPrimitive("bool", VerboseSchema.TypeTag.BOOL,
                (in, ctx, out, fmt) -> out.append(in.rbool()));
        registerPrimitive("str", VerboseSchema.TypeTag.STR,
                (in, ctx, out, fmt) -> out.append(in.rstr()));
        register("mcpos",
                List.of(VerboseSchema.TypeTag.VL, VerboseSchema.TypeTag.ZZ),
                (in, ctx, out, fmt) -> {
                    long xz = in.rvl();
                    int y = in.rzz();
                    out.append(unpackMcBlockX(xz)).append(", ")
                            .append(y).append(", ")
                            .append(unpackMcBlockZ(xz));
                });
        register("cursor",
                List.of(VerboseSchema.TypeTag.F32, VerboseSchema.TypeTag.F32, VerboseSchema.TypeTag.F32),
                (in, ctx, out, fmt) -> out.append(in.rf32()).append(", ")
                        .append(in.rf32()).append(", ")
                        .append(in.rf32()));
        register("slong",
                List.of(VerboseSchema.TypeTag.ZZ, VerboseSchema.TypeTag.ZZ),
                (in, ctx, out, fmt) -> {
                    int high = in.rzz();
                    int low = in.rzz();
                    appendFormatted(out, fmt, ((long) high << 32) | (low & 0xFFFF_FFFFL));
                });
    }

    private VerboseTags() {
    }

    /**
     * Register a domain tag. Idempotent when {@code name} is already bound to
     * the same wire shape; throws if the wire shape differs.
     */
    public static void register(
            @NotNull String name,
            @NotNull List<VerboseSchema.TypeTag> wire,
            @NotNull Renderer renderer) {
        if (name.isEmpty()) throw new IllegalArgumentException("name");
        if (wire.isEmpty()) throw new IllegalArgumentException("tag " + name + " declares no wire fields");
        Tag tag = new Tag(name, List.copyOf(wire), renderer);
        Tag previous = TAGS.putIfAbsent(name, tag);
        if (previous != null && !previous.wire().equals(tag.wire())) {
            throw new IllegalStateException("verbose tag " + name
                    + " already registered with wire " + previous.wire()
                    + "; register a new tag name instead of changing " + tag.wire());
        }
    }

    /**
     * Register a tag rendering a single {@code vi} field as an enum constant
     * name via {@link #enumId(Enum)} encoding ({@code 0 = null},
     * {@code ordinal + 1} otherwise).
     */
    public static void registerEnum(@NotNull String name, Enum<?> @NotNull [] values) {
        register(name, List.of(VerboseSchema.TypeTag.VI),
                (in, ctx, out, fmt) -> out.append(enumName(in.rvi(), values)));
    }

    /** Like {@link #registerEnum(String, Enum[])} but lower-cases the constant name. */
    public static void registerEnumLower(@NotNull String name, Enum<?> @NotNull [] values) {
        register(name, List.of(VerboseSchema.TypeTag.VI),
                (in, ctx, out, fmt) -> out.append(enumName(in.rvi(), values).toLowerCase(Locale.ROOT)));
    }

    /** Null-safe enum encoding used by {@link #registerEnum(String, Enum[])} tags. */
    public static int enumId(@Nullable Enum<?> value) {
        return value == null ? ENUM_NULL : value.ordinal() + 1;
    }

    public static @NotNull String enumName(int encoded, Enum<?> @NotNull [] values) {
        if (encoded == ENUM_NULL) return "null";
        int ordinal = encoded - 1;
        return ordinal >= 0 && ordinal < values.length ? values[ordinal].name() : "unknown(" + encoded + ")";
    }

    static @Nullable Tag get(@NotNull String name) {
        return TAGS.get(name);
    }

    static long packMcBlockXZ(int x, int z) {
        return ((long) (x & MC_XZ_MASK) << MC_XZ_BITS) | (z & MC_XZ_MASK);
    }

    static int unpackMcBlockX(long xz) {
        return signExtend26((int) (xz >>> MC_XZ_BITS));
    }

    static int unpackMcBlockZ(long xz) {
        return signExtend26((int) xz & MC_XZ_MASK);
    }

    private static int signExtend26(int value) {
        return (value << (Integer.SIZE - MC_XZ_BITS)) >> (Integer.SIZE - MC_XZ_BITS);
    }

    private static void registerPrimitive(
            @NotNull String name,
            @NotNull VerboseSchema.TypeTag wire,
            @NotNull Renderer renderer) {
        register(name, List.of(wire), renderer);
    }

    private static void appendFormatted(@NotNull StringBuilder out, @Nullable String fmt, double value) {
        if (fmt == null) out.append(value);
        else out.append(String.format(Locale.ROOT, fmt, value));
    }

    private static void appendFormatted(@NotNull StringBuilder out, @Nullable String fmt, float value) {
        if (fmt == null) out.append(value);
        else out.append(String.format(Locale.ROOT, fmt, value));
    }

    private static void appendFormatted(@NotNull StringBuilder out, @Nullable String fmt, int value) {
        if (fmt == null) out.append(value);
        else out.append(String.format(Locale.ROOT, fmt, value));
    }

    private static void appendFormatted(@NotNull StringBuilder out, @Nullable String fmt, long value) {
        if (fmt == null) out.append(value);
        else out.append(String.format(Locale.ROOT, fmt, value));
    }

    /** Decodes one tag's wire fields from {@code in} and appends display text to {@code out}. */
    @FunctionalInterface
    public interface Renderer {
        void render(
                @NotNull VerboseBuf in,
                @NotNull VerboseRenderContext ctx,
                @NotNull StringBuilder out,
                @Nullable String fmt);
    }

    record Tag(@NotNull String name,
               @NotNull List<VerboseSchema.TypeTag> wire,
               @NotNull Renderer renderer) {
    }
}
