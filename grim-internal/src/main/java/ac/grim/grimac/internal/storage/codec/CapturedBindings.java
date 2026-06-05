package ac.grim.grimac.internal.storage.codec;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.codec.EncodeShape.FieldDef;
import ac.grim.grimac.api.storage.codec.FieldKind;
import ac.grim.grimac.api.storage.codec.MergeMode;
import ac.grim.grimac.api.storage.codec.SearchType;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime registry of build-time-captured codec metadata. Loaded once at
 * class-init from a resource ({@value #RESOURCE}) baked into {@code grim-internal}
 * by the codec-binding capture tool.
 * <p>
 * <b>Why this exists:</b> the v2 codec is record-reflection-based. Allatori
 * strips the {@code RecordComponents} class attribute when it processes a
 * class, so {@code Class.isRecord()} returns false and
 * {@code getRecordComponents()} returns null at runtime — the codec can't
 * introspect its own builtin records in an obfuscated build. The capture tool
 * runs the real {@link CodecIntrospection#inspect} plus reflection on the
 * <em>pre</em>-obfuscation classes (where the attribute is intact) and writes
 * the resolved {@link EncodeShape} + {@link RecordLayout} here. At runtime the
 * codec reads those instead of reflecting, and binds accessors / the
 * constructor by name+type against the kept public api surface — no record
 * attribute needed.
 * <p>
 * Records not present here (extension records, or any build where the tool
 * didn't run) fall back to live reflection, which is correct for
 * un-obfuscated jars and for extension jars that Grim's obfuscator never
 * touches.
 */
@ApiStatus.Internal
public final class CapturedBindings {

    /** Classpath resource the capture tool writes; merged into the shaded jar verbatim. */
    static final String RESOURCE = "/META-INF/grim/codec-bindings.tsv";

    /** Captured metadata for one record: the validated shape + the reflection-free layout. */
    private record Captured(@NotNull EncodeShape shape, @NotNull RecordLayout layout) {}

    private static final Map<String, Captured> BY_NAME = load();

    private CapturedBindings() {}

    /** Captured shape for {@code recordType}, or null if it wasn't captured (use reflection). */
    public static @Nullable EncodeShape shape(@NotNull Class<?> recordType) {
        Captured c = BY_NAME.get(recordType.getName());
        return c == null ? null : c.shape();
    }

    /** Captured layout for {@code recordType}, or null if it wasn't captured (use reflection). */
    public static @Nullable RecordLayout layout(@NotNull Class<?> recordType) {
        Captured c = BY_NAME.get(recordType.getName());
        return c == null ? null : c.layout();
    }

    /** Number of captured records — diagnostics / tests. */
    public static int size() { return BY_NAME.size(); }

    private static @NotNull Map<String, Captured> load() {
        Map<String, Captured> out = new ConcurrentHashMap<>();
        try (InputStream in = CapturedBindings.class.getResourceAsStream(RESOURCE)) {
            if (in == null) return out; // tool didn't run / resource absent — reflection fallback everywhere
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                parse(r, out);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to read captured codec bindings " + RESOURCE, e);
        }
        return out;
    }

    /** Parse the TSV: blocks of one R line, one S line, N F lines, separated by blank lines. */
    private static void parse(@NotNull BufferedReader r, @NotNull Map<String, Captured> out) throws IOException {
        List<String> block = new ArrayList<>();
        String line;
        while ((line = r.readLine()) != null) {
            if (line.isEmpty() || line.charAt(0) == '#') {
                if (!block.isEmpty()) { parseBlock(block, out); block.clear(); }
                continue;
            }
            block.add(line);
        }
        if (!block.isEmpty()) parseBlock(block, out);
    }

    private static void parseBlock(@NotNull List<String> block, @NotNull Map<String, Captured> out) {
        String recordFqcn = null;
        int totalComponents = 0;
        int version = 1;
        Class<?>[] ctorParamTypes = new Class<?>[0];
        String idField = null, timestampField = null;
        List<String> partition = List.of(), indexed = List.of(), searchable = List.of();
        List<FieldDef> fields = new ArrayList<>();
        List<String> accessorNames = new ArrayList<>();

        for (String l : block) {
            String[] t = l.split("\t", -1);
            switch (t[0]) {
                case "R" -> {
                    recordFqcn = t[1];
                    totalComponents = Integer.parseInt(t[2]);
                    version = Integer.parseInt(t[3]);
                    ctorParamTypes = resolveTypes(t[4]);
                }
                case "S" -> {
                    idField = t[1];
                    timestampField = t[2].isEmpty() ? null : t[2];
                    partition = csv(t[3]);
                    indexed = csv(t[4]);
                    searchable = csv(t[5]);
                }
                case "F" -> {
                    SearchType st = t[5].isEmpty() ? null : SearchType.valueOf(t[5]);
                    fields.add(new FieldDef(
                        t[1],                       // encoded name
                        resolveType(t[2]),          // java type
                        FieldKind.valueOf(t[3]),    // kind
                        Boolean.parseBoolean(t[4]), // nullable
                        st,                         // search type
                        Integer.parseInt(t[6]),     // vector dimension
                        Integer.parseInt(t[7]),     // record index
                        MergeMode.valueOf(t[8]),    // merge mode
                        Long.parseLong(t[9])));     // sentinel value
                    accessorNames.add(t[10]);
                }
                default -> throw new IllegalStateException("unexpected codec-binding line: " + l);
            }
        }
        if (recordFqcn == null) throw new IllegalStateException("codec-binding block missing R line: " + block);

        EncodeShape shape = new EncodeShape(
            idField, timestampField, partition, indexed, searchable, fields, totalComponents);
        RecordLayout layout = new RecordLayout(accessorNames.toArray(new String[0]), ctorParamTypes, version);
        out.put(recordFqcn, new Captured(shape, layout));
    }

    private static @NotNull List<String> csv(@NotNull String s) {
        if (s.isEmpty()) return List.of();
        return List.of(s.split(","));
    }

    private static @NotNull Class<?>[] resolveTypes(@NotNull String csv) {
        if (csv.isEmpty()) return new Class<?>[0];
        String[] parts = csv.split(",");
        Class<?>[] types = new Class<?>[parts.length];
        for (int i = 0; i < parts.length; i++) types[i] = resolveType(parts[i]);
        return types;
    }

    /** Resolve a captured type name to a Class. Mirrors {@code CodecBindingCaptureTool.typeName}. */
    static @NotNull Class<?> resolveType(@NotNull String name) {
        return switch (name) {
            case "int" -> int.class;
            case "long" -> long.class;
            case "double" -> double.class;
            case "float" -> float.class;
            case "boolean" -> boolean.class;
            case "byte" -> byte.class;
            case "short" -> short.class;
            case "char" -> char.class;
            case "byte[]" -> byte[].class;
            case "float[]" -> float[].class;
            default -> {
                try {
                    yield Class.forName(name);
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("captured codec binding references missing type: " + name, e);
                }
            }
        };
    }
}
