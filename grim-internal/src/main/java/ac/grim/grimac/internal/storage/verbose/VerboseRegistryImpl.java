package ac.grim.grimac.internal.storage.verbose;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.event.VerboseSchemaEvent;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.VerboseSchemaRecord;
import ac.grim.grimac.api.storage.verbose.Verbose;
import ac.grim.grimac.api.storage.verbose.VerboseBuf;
import ac.grim.grimac.api.storage.verbose.VerboseFormatter;
import ac.grim.grimac.api.storage.verbose.VerboseRenderContext;
import ac.grim.grimac.api.storage.verbose.VerboseSchema;
import ac.grim.grimac.api.storage.verbose.VerboseSink;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

@ApiStatus.Internal
public final class VerboseRegistryImpl implements VerboseRegistry {

    private final @Nullable DataStore store;
    private final @NotNull CheckRegistry checks;
    private final int flavor;
    private final @NotNull Category<VerboseSchemaEvent> category;
    private final @NotNull Logger logger;

    private final ConcurrentMap<String, VerboseSchema> schemasByStableKey = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Verbose> templatesByStableKey = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, VerboseFormatter> formattersByStableKey = new ConcurrentHashMap<>();
    private final ConcurrentMap<FormatterKey, VerboseFormatter> formattersByTuple = new ConcurrentHashMap<>();
    private final ConcurrentMap<LayoutKey, Optional<VerboseSchema.Layout>> layoutCache = new ConcurrentHashMap<>();
    private final Object changeLock = new Object();
    private volatile @Nullable Runnable changeListener;
    private int changeBatchDepth;
    private boolean changePending;

    public VerboseRegistryImpl(
            @Nullable DataStore store,
            @NotNull CheckRegistry checks,
            int flavor) {
        this(store, checks, flavor, Categories.VERBOSE_SCHEMA,
                Logger.getLogger(VerboseRegistryImpl.class.getName()));
    }

    public VerboseRegistryImpl(
            @Nullable DataStore store,
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
        if (!isBinaryVersion(schema.version())) throw new IllegalArgumentException("schema version must be positive");
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
        if (!isBinaryVersion(formatter.version())) throw new IllegalArgumentException("formatter version must be positive");
        formattersByStableKey.put(stableKey, formatter);
        checks.getId(stableKey).ifPresent(checkId ->
                formattersByTuple.put(new FormatterKey(flavor, checkId, formatter.version()), formatter));
    }

    @Override
    public void registerTemplate(
            @NotNull String stableKey,
            @NotNull String checkName,
            @Nullable String description,
            @Nullable String pluginVersion,
            @NotNull Verbose verbose) {
        if (stableKey.isEmpty()) throw new IllegalArgumentException("stableKey");
        Verbose existing = templatesByStableKey.get(stableKey);
        if (existing == verbose) return; // hot path: already registered, one map read
        if (existing != null && existing.template().equals(verbose.template())) return;
        if (existing != null) {
            logger.warning(() -> "verbose template for " + stableKey + " replaced without restart: \""
                    + existing.template() + "\" -> \"" + verbose.template() + "\"");
        }

        templatesByStableKey.put(stableKey, verbose);
        schemasByStableKey.put(stableKey, verbose.schema());
        VerboseFormatter formatter = verbose.asFormatter();
        formattersByStableKey.put(stableKey, formatter);
        checks.intern(stableKey, checkName, description, pluginVersion);
        resolveAndIntern(stableKey, verbose.schema(), checks);
        checks.getId(stableKey).ifPresent(checkId ->
                formattersByTuple.put(new FormatterKey(flavor, checkId, verbose.version()), formatter));

        emitChange();
    }

    @Override
    public void registerTemplates(@NotNull Runnable registration) {
        Objects.requireNonNull(registration, "registration");
        synchronized (changeLock) {
            changeBatchDepth++;
        }
        try {
            registration.run();
        } finally {
            Runnable listener = null;
            synchronized (changeLock) {
                changeBatchDepth--;
                if (changeBatchDepth == 0 && changePending) {
                    changePending = false;
                    listener = changeListener;
                }
            }
            runChangeListener(listener);
        }
    }

    @Override
    public void onChange(@Nullable Runnable listener) {
        this.changeListener = listener;
    }

    private void emitChange() {
        Runnable listener;
        synchronized (changeLock) {
            if (changeBatchDepth > 0) {
                changePending = true;
                return;
            }
            listener = changeListener;
        }
        runChangeListener(listener);
    }

    private void runChangeListener(@Nullable Runnable listener) {
        if (listener == null) return;
        try {
            listener.run();
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "verbose registry change listener failed", e);
        }
    }

    @Override
    public @NotNull String render(
            @NotNull String stableKey,
            byte @NotNull [] data,
            @NotNull VerboseRenderContext ctx) {
        if (stableKey.isEmpty() || data.length == 0) return "";
        try {
            VerboseFormatter formatter = formattersByStableKey.get(stableKey);
            if (formatter != null) {
                StringBuilder rendered = new StringBuilder();
                formatter.render(VerboseBuf.wrap(data), ctx, VerboseSink.into(rendered));
                return rendered.toString();
            }

            VerboseSchema schema = schemasByStableKey.get(stableKey);
            if (schema != null) {
                StringBuilder rendered = new StringBuilder();
                GenericVerboseReader.render(
                        new VerboseSchema.Layout(schema.fields()),
                        VerboseBuf.wrap(data),
                        ctx,
                        VerboseSink.into(rendered));
                return rendered.toString();
            }
        } catch (Throwable ignored) {
            return "";
        }
        return "";
    }

    @Override
    public @NotNull Map<Integer, Integer> checkIdVersions(@NotNull CheckRegistry checks) {
        Map<Integer, Integer> versions = new LinkedHashMap<>();
        for (Map.Entry<String, VerboseSchema> entry : schemasByStableKey.entrySet()) {
            Optional<Integer> checkId = checks.getId(entry.getKey());
            if (checkId.isEmpty()) continue;
            VerboseSchema schema = entry.getValue();
            if (!isBinaryVersion(schema.version())) continue;
            versions.put(checkId.get(), schema.version());
            resolveAndIntern(entry.getKey(), schema, checks);
        }
        for (Map.Entry<String, VerboseFormatter> entry : formattersByStableKey.entrySet()) {
            VerboseFormatter formatter = entry.getValue();
            if (!isBinaryVersion(formatter.version())) continue;
            checks.getId(entry.getKey()).ifPresent(checkId ->
                    formattersByTuple.put(new FormatterKey(flavor, checkId, formatter.version()), formatter));
        }
        return Map.copyOf(versions);
    }

    @Override
    public @Nullable VerboseFormatter codeFormatter(int flavor, int checkId, int version) {
        if (flavor != this.flavor) return null;
        if (!isBinaryVersion(version)) return null;
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
        if (!isBinaryVersion(version)) return null;
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
        if (!isBinaryVersion(schema.version())) return;
        Optional<Integer> checkId = checks.getId(stableKey);
        if (checkId.isEmpty()) return;

        LayoutKey key = new LayoutKey(flavor, checkId.get(), schema.version());
        Optional<VerboseSchema.Layout> effective = internSchema(stableKey, checkId.get(), schema);
        layoutCache.put(key, effective);

        VerboseFormatter formatter = formattersByStableKey.get(stableKey);
        if (formatter != null && isBinaryVersion(formatter.version())) {
            formattersByTuple.put(new FormatterKey(flavor, checkId.get(), formatter.version()), formatter);
        }
    }

    private @NotNull Optional<VerboseSchema.Layout> internSchema(
            @NotNull String stableKey,
            int checkId,
            @NotNull VerboseSchema schema) {
        if (!isBinaryVersion(schema.version())) return Optional.empty();
        String schemaKey = VerboseSchemaRecord.keyOf(flavor, checkId, schema.version());
        byte[] layoutBytes = layoutBytesFor(stableKey, schema);
        if (store == null) {
            return Optional.of(VerboseSchema.decodeLayout(layoutBytes));
        }
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
            return Optional.of(VerboseSchema.decodeLayout(layoutBytes));
        }

        long now = System.currentTimeMillis();
        VerboseSchemaRecord record = new VerboseSchemaRecord(
                schemaKey,
                flavor,
                checkId,
                schema.version(),
                layoutBytes,
                now);
        try {
            store.execute(new EntityOps.UpsertOp<>(category, record))
                    .toCompletableFuture()
                    .join();
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "failed to persist verbose schema " + schemaKey, e);
        }
        return Optional.of(VerboseSchema.decodeLayout(layoutBytes));
    }

    /** Template-bearing layout bytes when {@code stableKey} registered via template. */
    private byte @NotNull [] layoutBytesFor(@NotNull String stableKey, @NotNull VerboseSchema schema) {
        Verbose verbose = templatesByStableKey.get(stableKey);
        return verbose != null && verbose.version() == schema.version()
                ? verbose.layoutBytes()
                : schema.layoutBytes();
    }

    private @NotNull LayoutLookup loadLayout(@NotNull LayoutKey key) {
        if (key.flavor() == flavor) {
            Optional<String> stableKey = checks.stableKeyFor(key.checkId());
            if (stableKey.isPresent()) {
                VerboseSchema schema = schemasByStableKey.get(stableKey.get());
                if (schema != null && schema.version() == key.version()) {
                    return LayoutLookup.confirmed(Optional.of(
                            VerboseSchema.decodeLayout(layoutBytesFor(stableKey.get(), schema))));
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
        if (store == null) return RecordLookup.failure();
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

    private static boolean isBinaryVersion(int version) {
        return version >= 1;
    }

    private record LayoutKey(int flavor, int checkId, int version) {}

    private record FormatterKey(int flavor, int checkId, int version) {}
}
