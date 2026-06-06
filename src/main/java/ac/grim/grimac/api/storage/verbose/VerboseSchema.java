package ac.grim.grimac.api.storage.verbose;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Ordered binary verbose field layout.
 */
@ApiStatus.Experimental
public final class VerboseSchema {

    private static final String UTF_8 = "UTF-8";

    private final int version;
    private final @NotNull List<Field> fields;
    private volatile byte[] layoutBytes;

    private VerboseSchema(int version, @NotNull List<Field> fields) {
        if (version < 0) throw new IllegalArgumentException("version");
        this.version = version;
        this.fields = List.copyOf(fields);
        if (this.fields.isEmpty()) throw new IllegalArgumentException("schema must declare at least one field");
    }

    public static @NotNull VerboseSchema of(String @NotNull ... declarations) {
        return of(1, declarations);
    }

    public static @NotNull VerboseSchema of(int version, String @NotNull ... declarations) {
        if (declarations == null) throw new IllegalArgumentException("declarations");
        List<Field> fields = new ArrayList<>(declarations.length);
        for (String declaration : declarations) parseDeclaration(fields, declaration);
        return new VerboseSchema(version, fields);
    }

    public static @NotNull VerboseSchema fromLayoutBytes(byte @NotNull [] layout) {
        return new VerboseSchema(1, decodeLayout(layout).fields());
    }

    public @NotNull VerboseSchema withVersion(int version) {
        return new VerboseSchema(version, fields);
    }

    public int version() {
        return version;
    }

    public @NotNull List<Field> fields() {
        return fields;
    }

    public byte @NotNull [] layoutBytes() {
        byte[] cached = layoutBytes;
        if (cached == null) {
            cached = encodeLayout(fields);
            layoutBytes = cached;
        }
        return cached.clone();
    }

    public @NotNull VerboseBuf write(@NotNull VerboseBuf out) {
        return out.clear();
    }

    public @NotNull VerboseFormatter formatter() {
        return new SchemaFormatter(version, new Layout(fields));
    }

    public static @NotNull Layout decodeLayout(byte @NotNull [] layout) {
        if (layout == null) throw new IllegalArgumentException("layout");
        VerboseBuf in = VerboseBuf.wrap(layout);
        int count = in.rvi();
        List<Field> fields = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int nameLen = in.rvi();
            if (nameLen > VerboseBuf.MAX_STRING_BYTES) {
                throw new IllegalArgumentException("field name exceeds " + VerboseBuf.MAX_STRING_BYTES + " bytes");
            }
            byte[] nameBytes = new byte[nameLen];
            for (int j = 0; j < nameLen; j++) {
                nameBytes[j] = (byte) in.readUnsignedByte();
            }
            TypeTag type = TypeTag.fromTag(in.readUnsignedByte());
            fields.add(new Field(utf8(nameBytes, 0, nameBytes.length), type));
        }
        if (in.remaining() != 0) {
            throw new IllegalArgumentException("layout has trailing bytes");
        }
        return new Layout(fields);
    }

    private static byte @NotNull [] encodeLayout(@NotNull List<Field> fields) {
        VerboseBuf out = new VerboseBuf(Math.max(8, fields.size() * 8));
        out.vi(fields.size());
        for (Field field : fields) {
            byte[] name = utf8(field.name());
            if (name.length > VerboseBuf.MAX_STRING_BYTES) {
                throw new IllegalArgumentException("field name exceeds " + VerboseBuf.MAX_STRING_BYTES + " bytes");
            }
            out.vi(name.length);
            out.writeBytes(name);
            out.writeByte(field.type().tag());
        }
        return out.toByteArray();
    }

    private static void parseDeclaration(@NotNull List<Field> out, String declaration) {
        if (declaration == null) throw new IllegalArgumentException("declaration");
        int colon = declaration.indexOf(':');
        if (colon <= 0 || colon == declaration.length() - 1 || declaration.indexOf(':', colon + 1) >= 0) {
            throw new IllegalArgumentException("verbose field declaration must be name:type: " + declaration);
        }
        String name = declaration.substring(0, colon).trim();
        String type = declaration.substring(colon + 1).trim().toLowerCase(Locale.ROOT);
        if (name.isEmpty()) throw new IllegalArgumentException("field name");
        if ("pos".equals(type)) {
            out.add(new Field(name + ".x", TypeTag.F32));
            out.add(new Field(name + ".y", TypeTag.F32));
            out.add(new Field(name + ".z", TypeTag.F32));
            return;
        }
        out.add(new Field(name, TypeTag.fromName(type)));
    }

    private static byte @NotNull [] utf8(@NotNull String value) {
        try {
            return value.getBytes(UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError("UTF-8 unavailable", e);
        }
    }

    private static @NotNull String utf8(byte @NotNull [] bytes, int offset, int length) {
        try {
            return new String(bytes, offset, length, UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError("UTF-8 unavailable", e);
        }
    }

    private static final class SchemaFormatter implements VerboseFormatter {
        private final int version;
        private final Layout layout;

        private SchemaFormatter(int version, @NotNull Layout layout) {
            this.version = version;
            this.layout = layout;
        }

        @Override public int version() { return version; }

        @Override
        public void render(@NotNull VerboseBuf in, @NotNull VerboseSink out) {
            for (Field field : layout.fields()) {
                renderField(field, in, out);
            }
        }
    }

    public record Field(@NotNull String name, @NotNull TypeTag type) {
        public Field {
            if (name == null || name.isEmpty()) throw new IllegalArgumentException("name");
            if (type == null) throw new IllegalArgumentException("type");
        }
    }

    public record Layout(@NotNull List<Field> fields) {
        public Layout {
            fields = List.copyOf(fields);
            if (fields.isEmpty()) throw new IllegalArgumentException("fields");
        }

        public byte @NotNull [] layoutBytes() {
            return encodeLayout(fields);
        }
    }

    public enum TypeTag {
        F64(0x01, "f64"),
        F32(0x02, "f32"),
        VI(0x03, "vi"),
        ZZ(0x04, "zz"),
        VL(0x05, "vl"),
        BOOL(0x06, "bool"),
        STR(0x07, "str"),
        ENUM(0x08, "enum");

        private final int tag;
        private final String wireName;

        TypeTag(int tag, @NotNull String wireName) {
            this.tag = tag;
            this.wireName = wireName;
        }

        public int tag() {
            return tag;
        }

        public @NotNull String wireName() {
            return wireName;
        }

        public static @NotNull TypeTag fromTag(int tag) {
            for (TypeTag type : values()) {
                if (type.tag == tag) return type;
            }
            throw new IllegalArgumentException("unknown verbose type tag " + tag);
        }

        public static @NotNull TypeTag fromName(@NotNull String name) {
            for (TypeTag type : values()) {
                if (type.wireName.equals(name)) return type;
            }
            throw new IllegalArgumentException("unknown verbose type " + name);
        }
    }

    private static void renderField(@NotNull Field field, @NotNull VerboseBuf in, @NotNull VerboseSink out) {
        out.key(field.name());
        switch (field.type()) {
            case F64 -> out.num(in.rf64());
            case F32 -> out.num(in.rf32());
            case VI, ENUM -> out.num(in.rvi());
            case ZZ -> out.num(in.rzz());
            case VL -> out.num(in.rvl());
            case BOOL -> out.bool(in.rbool());
            case STR -> out.text(in.rstr());
        }
    }
}
