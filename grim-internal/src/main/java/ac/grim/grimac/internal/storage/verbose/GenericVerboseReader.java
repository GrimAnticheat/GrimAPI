package ac.grim.grimac.internal.storage.verbose;

import ac.grim.grimac.api.storage.verbose.VerboseBuf;
import ac.grim.grimac.api.storage.verbose.VerboseSchema;
import ac.grim.grimac.api.storage.verbose.VerboseSink;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class GenericVerboseReader {

    private GenericVerboseReader() {}

    public static void render(
            @NotNull VerboseSchema.Layout layout,
            @NotNull VerboseBuf in,
            @NotNull VerboseSink out) throws UnderflowException {
        try {
            for (VerboseSchema.Field field : layout.fields()) {
                renderField(field, in, out);
            }
        } catch (VerboseBuf.UnderflowException | IllegalArgumentException e) {
            throw new UnderflowException(e);
        }
    }

    private static void renderField(
            @NotNull VerboseSchema.Field field,
            @NotNull VerboseBuf in,
            @NotNull VerboseSink out) {
        out.key(field.name());
        switch (field.type()) {
            case F64 -> {
                require(in, 8);
                out.num(in.rf64());
            }
            case F32 -> {
                require(in, 4);
                out.num(in.rf32());
            }
            case VI, ENUM -> {
                require(in, 1);
                out.num(in.rvi());
            }
            case ZZ -> {
                require(in, 1);
                out.num(in.rzz());
            }
            case VL -> {
                require(in, 1);
                out.num(in.rvl());
            }
            case BOOL -> {
                require(in, 1);
                out.bool(in.rbool());
            }
            case STR -> {
                require(in, 1);
                out.text(in.rstr());
            }
        }
    }

    private static void require(@NotNull VerboseBuf in, int bytes) {
        if (in.remaining() < bytes) {
            throw new VerboseBuf.UnderflowException("verbose payload truncated");
        }
    }

    public static final class UnderflowException extends Exception {
        public UnderflowException(@NotNull Throwable cause) {
            super("verbose payload truncated", cause);
        }
    }
}
