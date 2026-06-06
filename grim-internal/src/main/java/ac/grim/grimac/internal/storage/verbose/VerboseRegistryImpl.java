package ac.grim.grimac.internal.storage.verbose;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.event.VerboseSchemaEvent;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.VerboseSchemaRecord;
import ac.grim.grimac.api.storage.verbose.VerboseFormatter;
import ac.grim.grimac.api.storage.verbose.VerboseSchema;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

@ApiStatus.Internal
public final class VerboseRegistryImpl implements VerboseRegistry {

    private final @NotNull DataStore store;
    private final @NotNull CheckRegistry checks;
    private final int flavor;
    private final @NotNull Category<VerboseSchemaEvent> category;
    private final @NotNull Logger logger;

    private final ConcurrentMap<String, VerboseSchema> schemasByStableKey = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, VerboseFormatter> formattersByStableKey = new ConcurrentHashMap<>();
    private final ConcurrentMap<FormatterKey, VerboseFormatter> formattersByTuple = new ConcurrentHashMap<>();
    private final ConcurrentMap<LayoutKey, Optional<VerboseSchema.Layout>> layoutCache = new ConcurrentHashMap<>();

    public VerboseRegistryImpl(
            @NotNull DataStore store,
            @NotNull CheckRegistry checks,
            int flavor) {
        this(store, checks, flavor, Categories.VERBOSE_SCHEMA,
                Logger.getLogger(VerboseRegistryImpl.class.getName()));
    }

    public VerboseRegistryImpl(
            @NotNull DataStore store,
            @NotNull CheckRegistry checks,
            int flavor,
            @NotNull Category<VerboseSchemaEvent> category,
            @NotNull Logger logger) {
        this.store = store;
        this.checks = checks;
        this.flavor = flavor;
        this.category = category;
        this.logger = logger;
    }

    @Override
    public void register(@NotNull String stableKey, @NotNull VerboseSchema schema) {
        if (stableKey.isEmpty()) throw new IllegalArgumentException("stableKey");
        VerboseSchema previous = schemasByStableKey.put(stableKey, schema);
        if (previous != null
                && (previous.version() != schema.version()
                || !Arrays.equals(previous.layoutBytes(), schema.layoutBytes()))) {
            logger.warning(() -> "verbose schema for " + stableKey
                    + " was registered more than once with different content");
        }
        resolveAndIntern(stableKey, schema, checks);
    }

    @Override
    public void registerFormatter(@NotNull String stableKey, @NotNull VerboseFormatter formatter) {
        if (stableKey.isEmpty()) throw new IllegalArgumentException("stableKey");
        formattersByStableKey.put(stableKey, formatter);
        checks.getId(stableKey).ifPresent(checkId ->
                formattersByTuple.put(new FormatterKey(flavor, checkId, formatter.version()), formatter));
    }

    @Override
    public @NotNull Map<Integer, Integer> checkIdVersions(@NotNull CheckRegistry checks) {
        Map<Integer, Integer> versions = new LinkedHashMap<>();
        for (Map.Entry<String, VerboseSchema> entry : schemasByStableKey.entrySet()) {
            checks.getId(entry.getKey()).ifPresent(checkId -> {
                VerboseSchema schema = entry.getValue();
                versions.put(checkId, schema.version());
                resolveAndIntern(entry.getKey(), schema, checks);
            });
        }
        for (Map.Entry<String, VerboseFormatter> entry : formattersByStableKey.entrySet()) {
            checks.getId(entry.getKey()).ifPresent(checkId ->
                    formattersByTuple.put(new FormatterKey(flavor, checkId, entry.getValue().version()), entry.getValue()));
        }
        return Map.copyOf(versions);
    }

    @Override
    public @Nullable VerboseFormatter codeFormatter(int flavor, int checkId, int version) {
        if (flavor != this.flavor) return null;
        FormatterKey key = new FormatterKey(flavor, checkId, version);
        VerboseFormatter cached = formattersByTuple.get(key);
        if (cached != null) return cached;
        Optional<String> stableKey = checks.stableKeyFor(checkId);
        if (stableKey.isEmpty()) return null;
        VerboseFormatter formatter = formattersByStableKey.get(stableKey.get());
        if (formatter == null || formatter.version() != version) return null;
        formattersByTuple.putIfAbsent(key, formatter);
        return formatter;
    }

    @Override
    public @Nullable VerboseSchema.Layout layout(int flavor, int checkId, int version) {
        LayoutKey key = new LayoutKey(flavor, checkId, version);
        Optional<VerboseSchema.Layout> cached = layoutCache.get(key);
        if (cached != null) return cached.orElse(null);

        LayoutLookup loaded = loadLayout(key);
        if (!loaded.cacheable()) {
            Optional<VerboseSchema.Layout> raced = layoutCache.get(key);
            return raced != null ? raced.orElse(null) : loaded.layout().orElse(null);
        }

        Optional<VerboseSchema.Layout> previous = layoutCache.putIfAbsent(key, loaded.layout());
        return (previous != null ? previous : loaded.layout()).orElse(null);
    }

    private void resolveAndIntern(
            @NotNull String stableKey,
            @NotNull VerboseSchema schema,
            @NotNull CheckRegistry checks) {
        Optional<Integer> checkId = checks.getId(stableKey);
        if (checkId.isEmpty()) return;

        LayoutKey key = new LayoutKey(flavor, checkId.get(), schema.version());
        Optional<VerboseSchema.Layout> effective = internSchema(stableKey, checkId.get(), schema);
        layoutCache.put(key, effective);

        VerboseFormatter formatter = formattersByStableKey.get(stableKey);
        if (formatter != null) {
            formattersByTuple.put(new FormatterKey(flavor, checkId.get(), formatter.version()), formatter);
        }
    }

    private @NotNull Optional<VerboseSchema.Layout> internSchema(
            @NotNull String stableKey,
            int checkId,
            @NotNull VerboseSchema schema) {
        String schemaKey = VerboseSchemaRecord.keyOf(flavor, checkId, schema.version());
        byte[] layoutBytes = schema.layoutBytes();
        // loadRecord().join() may block the calling pool thread on first resolution; amortized once per schema.
        RecordLookup existing = loadRecord(schemaKey);
        if (existing.isPresent()) {
            VerboseSchemaRecord row = existing.record().get();
            if (!Arrays.equals(row.layout(), layoutBytes)) {
                logger.warning(() -> "verbose schema conflict for " + schemaKey
                        + " (" + stableKey + "): keeping existing durable layout");
            }
            try {
                return Optional.of(VerboseSchema.decodeLayout(row.layout()));
            } catch (RuntimeException e) {
                logger.log(Level.WARNING, "stored verbose schema layout is unreadable for " + schemaKey, e);
                return Optional.empty();
            }
        }
        if (existing.lookupFailed()) {
            return Optional.of(new VerboseSchema.Layout(schema.fields()));
        }

        long now = System.currentTimeMillis();
        store.submit(category, event -> event
                .schemaKey(schemaKey)
                .flavor(flavor)
                .checkId(checkId)
                .version(schema.version())
                .layout(layoutBytes)
                .introducedAt(now));
        return Optional.of(new VerboseSchema.Layout(schema.fields()));
    }

    private @NotNull LayoutLookup loadLayout(@NotNull LayoutKey key) {
        if (key.flavor() == flavor) {
            Optional<String> stableKey = checks.stableKeyFor(key.checkId());
            if (stableKey.isPresent()) {
                VerboseSchema schema = schemasByStableKey.get(stableKey.get());
                if (schema != null && schema.version() == key.version()) {
                    return LayoutLookup.confirmed(Optional.of(new VerboseSchema.Layout(schema.fields())));
                }
            }
        }

        String schemaKey = VerboseSchemaRecord.keyOf(key.flavor(), key.checkId(), key.version());
        // loadRecord().join() may block the calling pool thread on first resolution; amortized once per schema.
        RecordLookup row = loadRecord(schemaKey);
        if (row.lookupFailed()) return LayoutLookup.retryableMiss();
        if (row.isEmpty()) return LayoutLookup.confirmed(Optional.empty());
        try {
            return LayoutLookup.confirmed(Optional.of(VerboseSchema.decodeLayout(row.record().get().layout())));
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "stored verbose schema layout is unreadable for " + schemaKey, e);
            return LayoutLookup.confirmed(Optional.empty());
        }
    }

    @SuppressWarnings("unchecked")
    private @NotNull RecordLookup loadRecord(@NotNull String schemaKey) {
        try {
            CompletionStage<Optional<VerboseSchemaRecord>> stage =
                    (CompletionStage<Optional<VerboseSchemaRecord>>) (CompletionStage<?>)
                            store.execute(new EntityOps.GetByIdOp<>(category, schemaKey));
            return RecordLookup.confirmed(stage.toCompletableFuture().join());
        } catch (RuntimeException e) {
            logger.log(Level.FINE, "verbose schema lookup failed for " + schemaKey, e);
            return RecordLookup.failure();
        }
    }

    private record LayoutLookup(@NotNull Optional<VerboseSchema.Layout> layout, boolean cacheable) {
        private static @NotNull LayoutLookup confirmed(@NotNull Optional<VerboseSchema.Layout> layout) {
            return new LayoutLookup(layout, true);
        }

        private static @NotNull LayoutLookup retryableMiss() {
            return new LayoutLookup(Optional.empty(), false);
        }
    }

    private record RecordLookup(@NotNull Optional<VerboseSchemaRecord> record, boolean lookupFailed) {
        private static @NotNull RecordLookup confirmed(@NotNull Optional<VerboseSchemaRecord> record) {
            return new RecordLookup(record, false);
        }

        private static @NotNull RecordLookup failure() {
            return new RecordLookup(Optional.empty(), true);
        }

        private boolean isPresent() {
            return record.isPresent();
        }

        private boolean isEmpty() {
            return record.isEmpty();
        }
    }

    private record LayoutKey(int flavor, int checkId, int version) {}

    private record FormatterKey(int flavor, int checkId, int version) {}
}
