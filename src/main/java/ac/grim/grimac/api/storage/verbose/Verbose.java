package ac.grim.grimac.api.storage.verbose;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A verbose payload described by a single display template.
 *
 * <p>The template is the only declaration a check writes. It is
 * simultaneously:
 * <ul>
 *   <li>the binary layout — each {@code {tag}} placeholder maps to the wire
 *       fields its {@link VerboseTags tag} declares,</li>
 *   <li>the writer contract — {@link #write(VerboseBuf)} returns a cursor
 *       that validates every write against the placeholder sequence,</li>
 *   <li>the renderer — display text is the template with placeholders
 *       substituted by decoded values, and</li>
 *   <li>the schema version — derived from the template text itself, so
 *       editing a template automatically versions the stored format.</li>
 * </ul>
 *
 * <p>Grammar: literal text plus {@code {tag}} or {@code {tag:fmt}}
 * placeholders, where {@code fmt} is an optional {@code String.format}
 * pattern for numeric tags. Square-bracket groups encode a leading
 * {@code bool} gate: {@code [a]} renders {@code a} only when the gate is
 * true; {@code [a|b]} renders {@code a} when true and {@code b} when false.
 * Group fields are always written (use zero/placeholder values for the
 * branch not taken) so payloads stay decodable from the flat field list.
 * Escape literal {@code { } [ ] | \} characters with a backslash.
 *
 * <p>A top-level {@code |} (equivalently, chained {@link #or(String)} calls)
 * separates whole payload <em>shapes</em>. Multi-shape payloads start with a
 * varint shape selector and carry only the selected shape's fields; write
 * them with {@link #write(VerboseBuf, int)}. Use inline {@code [a|b]} groups
 * for a small alternative fragment inside one shape, and shapes for flags
 * whose payloads differ structurally.
 *
 * <pre>{@code
 * private static final Verbose V =
 *     Verbose.of("delay={ulong}ms, diff={f64:%.4f}, type={block}");
 *
 * flagAndAlert(V.write(verbose()).ulong(delay).f64(diff).sint(VerboseCodecs.block(type, version)));
 * }</pre>
 *
 * <p>Built-in tags (writer method matches the tag name unless noted):
 * <table border="1">
 *   <tr><th>Tag</th><th>Java type</th><th>Wire encoding</th><th>Bytes</th><th>Use for</th></tr>
 *   <tr><td>{@code {uint}}</td><td>int ≥ 0</td><td>unsigned varint</td><td>1-5</td><td>counts, ids, ordinals</td></tr>
 *   <tr><td>{@code {sint}}</td><td>int</td><td>zigzag varint</td><td>1-5</td><td>anything possibly negative</td></tr>
 *   <tr><td>{@code {ulong}}</td><td>long ≥ 0</td><td>unsigned varlong</td><td>1-10</td><td>timestamps, durations</td></tr>
 *   <tr><td>{@code {slong}}</td><td>long</td><td>two zigzag varints</td><td>2-10</td><td>signed longs (keepalive ids)</td></tr>
 *   <tr><td>{@code {f64}} / {@code {f32}}</td><td>double / float</td><td>raw IEEE LE</td><td>8 / 4</td><td>measurements worth keeping exact</td></tr>
 *   <tr><td>{@code {bool}}</td><td>boolean</td><td>1 byte</td><td>1</td><td>flags; also gates groups</td></tr>
 *   <tr><td>{@code {str}}</td><td>CharSequence</td><td>varint len + UTF-8</td><td>1+n</td><td>dynamic evidence text only</td></tr>
 *   <tr><td>{@code {mcpos}}</td><td>3 ints (writer {@code mcPos(x,y,z)})</td><td>26-bit packed XZ varlong + zigzag Y</td><td>3-11</td><td>block positions (±30M)</td></tr>
 *   <tr><td>{@code {cursor}}</td><td>3 floats (writer {@code cursor(x,y,z)})</td><td>raw f32 ×3</td><td>12</td><td>cursor/hit vectors</td></tr>
 * </table>
 */
@ApiStatus.Experimental
public final class Verbose {

    private static final ConcurrentMap<String, Verbose> INTERNED = new ConcurrentHashMap<>();

    private final @NotNull String template;
    private final int version;
    private volatile @Nullable Parsed parsed;

    private Verbose(@NotNull String template) {
        this.template = template;
        this.version = versionOf(template);
    }

    /**
     * Intern the template. Parsing is deferred to first use so domain tags
     * may be registered after class initialization but before any flag.
     */
    public static @NotNull Verbose of(@NotNull String template) {
        if (template.isEmpty()) throw new IllegalArgumentException("template");
        return INTERNED.computeIfAbsent(template, Verbose::new);
    }

    /**
     * Append another payload shape. Sugar over the top-level {@code |}
     * separator: {@code Verbose.of("a").or("b")} and {@code Verbose.of("a|b")}
     * intern to the same instance. Shape indexes follow declaration order and
     * are written by {@link #write(VerboseBuf, int)} as a leading varint;
     * only the selected shape's fields follow it on the wire.
     */
    public @NotNull Verbose or(@NotNull String nextShape) {
        if (nextShape.isEmpty()) throw new IllegalArgumentException("nextShape");
        return of(template + "|" + nextShape);
    }

    public @NotNull String template() {
        return template;
    }

    /** Number of payload shapes (1 unless the template uses top-level {@code |}). */
    public int shapes() {
        return parsed().cases.size();
    }

    /** The wire fields of one shape, in write order. For audit tooling/tests. */
    public @NotNull List<VerboseSchema.Field> shapeFields(int shape) {
        return parsed().cases.get(shape).fields();
    }

    /** Content-derived schema version: editing the template re-versions the format. */
    public int version() {
        return version;
    }

    /** Flattened wire layout, versioned by the template hash. */
    public @NotNull VerboseSchema schema() {
        return parsed().schema;
    }

    /** Layout bytes carrying both the wire fields and the template text. */
    public byte @NotNull [] layoutBytes() {
        return VerboseSchema.encodeLayout(parsed().schema.fields(), template);
    }

    /** Clear {@code buf} and return a validating writer over this template's fields. */
    public @NotNull Writer write(@NotNull VerboseBuf buf) {
        Parsed parsed = parsed();
        if (parsed.cases.size() > 1) {
            throw new IllegalStateException("verbose template \"" + template
                    + "\" defines " + parsed.cases.size() + " shapes; use write(buf, shape)");
        }
        return new Writer(this, buf.clear(), parsed.cases.get(0).fields());
    }

    /**
     * Multi-shape writer: writes the {@code shape} selector varint and
     * validates against that shape's fields only.
     */
    public @NotNull Writer write(@NotNull VerboseBuf buf, int shape) {
        Parsed parsed = parsed();
        if (parsed.cases.size() <= 1) {
            throw new IllegalStateException("verbose template \"" + template
                    + "\" defines one shape; use write(buf)");
        }
        if (shape < 0 || shape >= parsed.cases.size()) {
            throw new IllegalArgumentException("verbose template \"" + template
                    + "\" has shapes 0.." + (parsed.cases.size() - 1) + "; got " + shape);
        }
        VerboseBuf cleared = buf.clear();
        cleared.vi(shape);
        return new Writer(this, cleared, parsed.cases.get(shape).fields());
    }

    /** Render a stored payload; returns {@code ""} on any decode failure. */
    public @NotNull String render(byte @NotNull [] data, @NotNull VerboseRenderContext ctx) {
        StringBuilder out = new StringBuilder(template.length() + 16);
        try {
            renderTo(VerboseBuf.wrap(data), ctx, out);
        } catch (Throwable t) {
            return "";
        }
        return out.toString();
    }

    /**
     * Render a payload through a template recovered from storage (for
     * history rows written by other builds). Returns {@code null} when the
     * template cannot be parsed or rendered here, e.g. an unregistered tag,
     * letting the caller fall back to generic field rendering.
     */
    public static @Nullable String renderStored(
            @NotNull String template,
            byte @NotNull [] data,
            @NotNull VerboseRenderContext ctx) {
        if (template.isEmpty()) return null;
        try {
            Verbose verbose = of(template);
            StringBuilder out = new StringBuilder(template.length() + 16);
            verbose.renderTo(VerboseBuf.wrap(data), ctx, out);
            return out.toString();
        } catch (Throwable t) {
            return null;
        }
    }

    /** Adapter for render paths addressed by {@code (flavor, checkId, version)}. */
    public @NotNull VerboseFormatter asFormatter() {
        return new VerboseFormatter() {
            @Override
            public int version() {
                return version;
            }

            @Override
            public void render(
                    @NotNull VerboseBuf in,
                    @NotNull VerboseRenderContext ctx,
                    @NotNull VerboseSink out) {
                StringBuilder text = new StringBuilder(template.length() + 16);
                renderTo(in, ctx, text);
                out.text(text);
            }
        };
    }

    private void renderTo(@NotNull VerboseBuf in, @NotNull VerboseRenderContext ctx, @NotNull StringBuilder out) {
        Parsed parsed = parsed();
        List<Node> nodes;
        if (parsed.cases.size() > 1) {
            int shape = in.rvi();
            if (shape >= parsed.cases.size()) {
                throw new IllegalArgumentException("payload shape " + shape
                        + " out of range for template \"" + template + "\"");
            }
            nodes = parsed.cases.get(shape).nodes();
        } else {
            nodes = parsed.cases.get(0).nodes();
        }
        renderNodes(nodes, in, ctx, out, new StringBuilder(), true);
    }

    private static void renderNodes(
            @NotNull List<Node> nodes,
            @NotNull VerboseBuf in,
            @NotNull VerboseRenderContext ctx,
            @NotNull StringBuilder out,
            @NotNull StringBuilder scratch,
            boolean active) {
        for (Node node : nodes) {
            if (node instanceof Lit lit) {
                if (active) out.append(lit.text());
            } else if (node instanceof Field field) {
                // Inactive branches still consume their wire fields; the
                // decoded text goes to scratch and is discarded.
                StringBuilder target = active ? out : scratch;
                if (!active) scratch.setLength(0);
                field.tag().renderer().render(in, ctx, target, field.fmt());
            } else if (node instanceof Group group) {
                boolean gate = in.rbool();
                renderNodes(group.whenTrue(), in, ctx, out, scratch, active && gate);
                renderNodes(group.whenFalse(), in, ctx, out, scratch, active && !gate);
            }
        }
    }

    private @NotNull Parsed parsed() {
        Parsed cached = parsed;
        if (cached == null) {
            cached = Parser.parse(template, version);
            parsed = cached;
        }
        return cached;
    }

    private static int versionOf(@NotNull String template) {
        int hash = 0x811c9dc5;
        for (int i = 0; i < template.length(); i++) {
            hash ^= template.charAt(i);
            hash *= 0x01000193;
        }
        hash &= 0x7fffffff;
        return hash == 0 ? 1 : hash;
    }

    /**
     * Validating write cursor. Every write is checked against the template's
     * field sequence so a callsite that drifts from its template fails
     * immediately instead of storing an undecodable payload.
     */
    public static final class Writer {
        private static final int MC_POS_LIMIT = (1 << 25) - 1; // signed 26-bit

        private final @NotNull Verbose owner;
        private final @NotNull VerboseBuf buf;
        private final @NotNull List<VerboseSchema.Field> fields;
        private int next;

        private Writer(@NotNull Verbose owner, @NotNull VerboseBuf buf, @NotNull List<VerboseSchema.Field> fields) {
            this.owner = owner;
            this.buf = buf;
            this.fields = fields;
        }

        public @NotNull Writer f64(double v) {
            expect(VerboseSchema.TypeTag.F64);
            buf.f64(v);
            return this;
        }

        public @NotNull Writer f32(float v) {
            expect(VerboseSchema.TypeTag.F32);
            buf.f32(v);
            return this;
        }

        /** Unsigned varint ({@code {uint}}): 1-5 bytes, rejects negatives. */
        public @NotNull Writer uint(int v) {
            expect(VerboseSchema.TypeTag.VI);
            buf.vi(v);
            return this;
        }

        /** Signed zigzag varint ({@code {sint}}): small magnitudes stay small, sign included. */
        public @NotNull Writer sint(int v) {
            expect(VerboseSchema.TypeTag.ZZ);
            buf.zz(v);
            return this;
        }

        /** Unsigned varlong ({@code {ulong}}): 1-10 bytes, rejects negatives. */
        public @NotNull Writer ulong(long v) {
            expect(VerboseSchema.TypeTag.VL);
            buf.vl(v);
            return this;
        }

        public @NotNull Writer bool(boolean v) {
            expect(VerboseSchema.TypeTag.BOOL);
            buf.bool(v);
            return this;
        }

        public @NotNull Writer str(@NotNull CharSequence v) {
            expect(VerboseSchema.TypeTag.STR);
            buf.str(v);
            return this;
        }

        /**
         * Writes a {@code {mcpos}} placeholder: X and Z packed as two signed
         * 26-bit ints in one varlong (covers Minecraft's ±30,000,000 world
         * border) plus Y as a zigzag varint.
         */
        public @NotNull Writer mcPos(int x, int y, int z) {
            if (x < -MC_POS_LIMIT || x > MC_POS_LIMIT || z < -MC_POS_LIMIT || z > MC_POS_LIMIT) {
                throw new IllegalArgumentException("mcPos x/z out of 26-bit range: " + x + ", " + z);
            }
            expect(VerboseSchema.TypeTag.VL);
            expect(VerboseSchema.TypeTag.ZZ);
            buf.vl(VerboseTags.packMcBlockXZ(x, z)).zz(y);
            return this;
        }

        /** Writes a {@code {cursor}} placeholder (three raw floats). */
        public @NotNull Writer cursor(float x, float y, float z) {
            expect(VerboseSchema.TypeTag.F32);
            expect(VerboseSchema.TypeTag.F32);
            expect(VerboseSchema.TypeTag.F32);
            buf.f32(x).f32(y).f32(z);
            return this;
        }

        /** Writes a {@code {slong}} placeholder (signed long as two zigzag ints). */
        public @NotNull Writer slong(long v) {
            expect(VerboseSchema.TypeTag.ZZ);
            expect(VerboseSchema.TypeTag.ZZ);
            buf.zz((int) (v >> 32)).zz((int) v);
            return this;
        }

        public @NotNull Verbose verbose() {
            return owner;
        }

        /** Verify all template fields were written and return the payload buffer. */
        public @NotNull VerboseBuf end() {
            if (next != fields.size()) {
                throw new IllegalStateException("verbose template \"" + owner.template
                        + "\" expects " + fields.size() + " fields but " + next + " were written");
            }
            return buf;
        }

        private void expect(@NotNull VerboseSchema.TypeTag actual) {
            if (next >= fields.size()) {
                throw new IllegalStateException("verbose template \"" + owner.template
                        + "\" declares " + fields.size() + " fields; extra " + actual.wireName() + " write");
            }
            VerboseSchema.Field expected = fields.get(next);
            if (expected.type() != actual) {
                throw new IllegalStateException("verbose template \"" + owner.template
                        + "\" field " + next + " (" + expected.name() + ") is "
                        + expected.type().wireName() + " but callsite wrote " + actual.wireName());
            }
            next++;
        }
    }

    private record Parsed(@NotNull VerboseSchema schema, @NotNull List<Case> cases) {
    }

    private record Case(@NotNull List<Node> nodes, @NotNull List<VerboseSchema.Field> fields) {
    }

    private sealed interface Node permits Lit, Field, Group {
    }

    private record Lit(@NotNull String text) implements Node {
    }

    private record Field(@NotNull VerboseTags.Tag tag, @Nullable String fmt) implements Node {
    }

    private record Group(@NotNull List<Node> whenTrue, @NotNull List<Node> whenFalse) implements Node {
    }

    private static final class Parser {
        private final @NotNull String template;
        private int pos;

        private Parser(@NotNull String template) {
            this.template = template;
        }

        static @NotNull Parsed parse(@NotNull String template, int version) {
            Parser parser = new Parser(template);
            List<Case> cases = new ArrayList<>();
            int[] groupSeq = new int[1];
            while (true) {
                List<Node> nodes = parser.parseNodes(false);
                List<VerboseSchema.Field> fields = new ArrayList<>();
                flatten(nodes, fields, groupSeq);
                cases.add(new Case(nodes, List.copyOf(fields)));
                if (parser.pos >= template.length()) break;
                char c = template.charAt(parser.pos);
                if (c == '|') {
                    parser.pos++;
                } else {
                    throw new IllegalArgumentException("unbalanced ']' at " + parser.pos + " in: " + template);
                }
            }

            List<VerboseSchema.Field> schemaFields = new ArrayList<>();
            if (cases.size() > 1) {
                // Multi-shape: the wire carries a varint selector followed by
                // the selected shape's fields only. The persisted flat list is
                // the selector plus every shape's fields in order — decodable
                // through the stored template, advisory for generic fallback.
                schemaFields.add(new VerboseSchema.Field("shape", VerboseSchema.TypeTag.VI));
                for (Case c : cases) schemaFields.addAll(c.fields());
            } else {
                schemaFields.addAll(cases.get(0).fields());
                if (schemaFields.isEmpty()) {
                    throw new IllegalArgumentException("verbose template declares no fields: " + template);
                }
            }
            return new Parsed(VerboseSchema.of(version, schemaFields), List.copyOf(cases));
        }

        private @NotNull List<Node> parseNodes(boolean inGroup) {
            List<Node> nodes = new ArrayList<>();
            StringBuilder literal = new StringBuilder();
            while (pos < template.length()) {
                char c = template.charAt(pos);
                if (c == '\\') {
                    if (pos + 1 >= template.length()) {
                        throw new IllegalArgumentException("dangling escape in: " + template);
                    }
                    literal.append(template.charAt(pos + 1));
                    pos += 2;
                } else if (c == '{') {
                    flushLiteral(nodes, literal);
                    nodes.add(parsePlaceholder());
                } else if (c == '[') {
                    flushLiteral(nodes, literal);
                    pos++;
                    List<Node> whenTrue = parseNodes(true);
                    List<Node> whenFalse = List.of();
                    if (pos < template.length() && template.charAt(pos) == '|') {
                        pos++;
                        whenFalse = parseNodes(true);
                    }
                    if (pos >= template.length() || template.charAt(pos) != ']') {
                        throw new IllegalArgumentException("unclosed '[' in: " + template);
                    }
                    pos++;
                    nodes.add(new Group(whenTrue, whenFalse));
                } else if (c == '|' || (inGroup && c == ']')) {
                    // '|' separates group branches when inside a group and
                    // top-level payload shapes otherwise; the caller consumes it.
                    break;
                } else if (c == '}') {
                    throw new IllegalArgumentException("unbalanced '}' at " + pos + " in: " + template);
                } else {
                    literal.append(c);
                    pos++;
                }
            }
            flushLiteral(nodes, literal);
            return List.copyOf(nodes);
        }

        private @NotNull Node parsePlaceholder() {
            int close = template.indexOf('}', pos);
            if (close < 0) {
                throw new IllegalArgumentException("unclosed '{' at " + pos + " in: " + template);
            }
            String body = template.substring(pos + 1, close);
            pos = close + 1;
            int colon = body.indexOf(':');
            String name = colon < 0 ? body : body.substring(0, colon);
            String fmt = colon < 0 ? null : body.substring(colon + 1);
            if (name.isEmpty()) {
                throw new IllegalArgumentException("empty placeholder in: " + template);
            }
            VerboseTags.Tag tag = VerboseTags.get(name);
            if (tag == null) {
                throw new IllegalArgumentException("unknown verbose tag {" + name + "} in: " + template);
            }
            return new Field(tag, fmt);
        }

        private static void flushLiteral(@NotNull List<Node> nodes, @NotNull StringBuilder literal) {
            if (literal.length() > 0) {
                nodes.add(new Lit(literal.toString()));
                literal.setLength(0);
            }
        }

        private static void flatten(
                @NotNull List<Node> nodes,
                @NotNull List<VerboseSchema.Field> fields,
                int @NotNull [] groupSeq) {
            String pendingLabel = null;
            for (Node node : nodes) {
                if (node instanceof Lit lit) {
                    pendingLabel = labelFrom(lit.text());
                } else if (node instanceof Field field) {
                    List<VerboseSchema.TypeTag> wire = field.tag().wire();
                    String base = pendingLabel != null ? pendingLabel : field.tag().name() + "." + fields.size();
                    for (int i = 0; i < wire.size(); i++) {
                        String name = wire.size() == 1 ? base : base + "." + i;
                        fields.add(new VerboseSchema.Field(name, wire.get(i)));
                    }
                    pendingLabel = null;
                } else if (node instanceof Group group) {
                    fields.add(new VerboseSchema.Field("case." + groupSeq[0]++, VerboseSchema.TypeTag.BOOL));
                    flatten(group.whenTrue(), fields, groupSeq);
                    flatten(group.whenFalse(), fields, groupSeq);
                    pendingLabel = null;
                }
            }
        }

        /** Derive a field name from the trailing {@code word=} of the preceding literal. */
        private static @Nullable String labelFrom(@NotNull String literal) {
            int end = literal.length();
            if (end == 0) return null;
            if (literal.charAt(end - 1) == '=') end--;
            int start = end;
            while (start > 0 && isLabelChar(literal.charAt(start - 1))) start--;
            return start == end ? null : literal.substring(start, end);
        }

        private static boolean isLabelChar(char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-';
        }
    }
}
