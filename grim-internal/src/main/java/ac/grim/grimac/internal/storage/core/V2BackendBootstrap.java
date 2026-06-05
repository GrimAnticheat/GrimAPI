package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.MigrationContext;
import ac.grim.grimac.api.storage.registry.StoreId;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One-shot startup helper that {@code ensureStore}s + runs migrations for
 * a set of v2 {@link DataKind} bindings against one {@link BackendV2}, then
 * appends the wired routes into a caller-supplied {@link V2Routes.Builder}.
 *
 * <p>Intended to be called from the plugin's storage-lifecycle layer
 * (Grim's {@code DataStoreLifecycle}) once per backend that hosts v2
 * categories. For multi-backend cutovers, call {@link #install} once per
 * backend with the same builder; {@link V2Routes.Builder#build()} at the
 * end produces the merged routes ready for {@code DataStoreImpl.withV2Routes}.
 *
 * <p><strong>Failure semantics — fail-closed per binding.</strong>
 * <ul>
 *   <li>Adapter-lookup failures (backend doesn't host the kind) → record +
 *       skip binding (no migrations, no route).</li>
 *   <li>Type-compatibility check ({@code category.eventType() == kind.eventType()}
 *       and {@code category.queryResultType() == kind.recordType()}) → record +
 *       skip binding. Catches misrouted bindings at startup rather than at
 *       first dispatch.</li>
 *   <li>{@code ensureStore} failures → record + skip binding. ensureStore
 *       is a precondition for usable storage; without it no migrations
 *       should run and no reads will work.</li>
 *   <li>Migration failures → record, break out of the migration loop for
 *       this binding, AND do NOT register the route. Running 2→3 after
 *       1→2 failed is unsafe (later migrations assume earlier ones
 *       succeeded), and a route to un-migrated data silently dispatches
 *       reads against the wrong physical layout. Fail-closed is the only
 *       safe default.</li>
 * </ul>
 * The caller MAY still inspect {@link Result#failures} to decide whether
 * to abort startup entirely, but the bootstrap will not leave dangerous
 * routes installed regardless of the caller's policy.
 *
 * <p>Stateless; safe to call multiple times against the same backend (each
 * call appends new routes — ensureStore and migrations are idempotent by
 * contract).
 */
@ApiStatus.Internal
public final class V2BackendBootstrap {

    private V2BackendBootstrap() {}

    /**
     * One v2 wiring entry: which Category to register, which physical
     * store to target, and the Kind that defines the schema + migrations.
     * Held as a record so callers can build the input map declaratively.
     */
    public record Binding<K extends DataKind<?, ?>>(
        @NotNull StoreId storeId,
        @NotNull K kind) {
    }

    /**
     * Per-install summary. {@code failures} captures partial failures so
     * the caller can log + decide whether to abort. The caller-supplied
     * builder is mutated in-place with the successfully-wired routes;
     * {@link V2Routes.Builder#build()} is the caller's responsibility so
     * a multi-backend cutover can use one builder across many install
     * calls.
     */
    public record Result(@NotNull List<String> failures) {
        public boolean ok() { return failures.isEmpty(); }
    }

    /**
     * Run the bootstrap pass for one backend, appending routes into
     * {@code routesBuilder}.
     *
     * @param bindings       identity-keyed (Category → Binding) map. Caller is
     *                       responsible for using the SAME Category instances
     *                       downstream consumers will pass to
     *                       {@code DataStoreImpl.execute(Operation)} — v2
     *                       route lookup is identity-keyed.
     * @param backend        already-initialized backend (caller called
     *                       {@code backend.init(ctx)} before this).
     * @param migrationCtx   backend-specific MigrationContext (e.g.
     *                       {@code MongoMigrationContext} wrapping a
     *                       {@code MongoDatabase}).
     * @param routesBuilder  mutated in-place with the routes for this
     *                       backend's bindings.
     * @param logger         receives INFO per migration, WARNING on failure.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static @NotNull Result install(
        @NotNull Map<Category<?>, Binding<?>> bindings,
        @NotNull BackendV2 backend,
        @NotNull MigrationContext migrationCtx,
        @NotNull V2Routes.Builder routesBuilder,
        @NotNull Logger logger) {

        List<String> failures = new ArrayList<>();

        for (Map.Entry<Category<?>, Binding<?>> entry : bindings.entrySet()) {
            Category<?> category = entry.getKey();
            Binding<?> binding = entry.getValue();
            DataKind kind = binding.kind();
            StoreId storeId = binding.storeId();

            // Type-compatibility guard, layer 1: event + record DTO
            // identity. Catches typo'd bindings whose event or record
            // type doesn't match the category's declared interface at
            // startup, instead of at first dispatch where the failure
            // would be cryptic.
            //
            // Event check uses isAssignableFrom (kind's event accepts
            // the category's event as a subtype) so legacy Categories
            // can bind to a kind whose event is a supertype — e.g.
            // SETTING (Category<SettingEvent>) binds to KeyValueScoped
            // (eventType = KeyValueEvent) because SettingEvent extends
            // KeyValueEvent.
            //
            // Record check is strict for Entity / EventStream since
            // those adapters materialize records of the declared type.
            // KeyValueScoped is exempt: KV adapters return Optional<V>
            // (the value type, NOT the kind's nominal recordType),
            // and DataStoreImpl.translateV2Result bridges to the
            // category's queryResultType. A startup check on the
            // nominal recordType here would reject every legitimate
            // KV-backed binding.
            boolean eventOk = kind.eventType().isAssignableFrom(category.eventType());
            boolean recordOk = (kind instanceof KeyValueScoped<?, ?>)
                || category.queryResultType() == kind.recordType();
            if (!eventOk || !recordOk) {
                failures.add("type mismatch: category " + category.id() + " (event="
                    + category.eventType().getSimpleName() + ", queryResult="
                    + category.queryResultType().getSimpleName() + ") does not match kind "
                    + kind.name() + " (event=" + kind.eventType().getSimpleName()
                    + ", record=" + kind.recordType().getSimpleName() + ")");
                logger.warning(() -> "[v2-bootstrap] " + category.id() + " ↔ " + kind.name()
                    + " type mismatch — skipping binding");
                continue;
            }

            // Type-compatibility guard, layer 2: kind-family. The DTO
            // check above passes when the wrong-family kind happens to
            // share event + record types (e.g. binding VIOLATION to an
            // Entity<UUID, ViolationEvent, ViolationRecord> — same DTOs,
            // wrong family). Consumers would dispatch EventStreamOps to
            // a MongoEntityAdapter and hit a cryptic 'unsupported
            // operation' at runtime.
            //
            // EventStream is the only family with strong disambiguators
            // on Category today:
            //   - Legacy builtins declare TIMESERIES_APPEND.
            //   - v2-native (EventStreamCategoryImpl) declares
            //     KIND_EVENT_STREAM via kind.requiredCapabilities().
            // Accept either marker.
            //
            // Entity-vs-KeyValueScoped (both legacy categories declare
            // INDEXED_KV with no distinguisher today) and Counter (no
            // dedicated legacy capability) are NOT yet caught here —
            // those still fail at dispatch. KeyValueScoped is partly
            // covered indirectly because its DTO classes
            // (KeyValueEvent/KeyValueRecord) differ from Entity DTOs
            // so layer-1 catches most realistic mis-binds.
            //
            // TODO Phase 5+: when the legacy Categories migrate from
            // the deprecated TIMESERIES_APPEND/INDEXED_KV/BLOB enum
            // values to KIND_EVENT_STREAM/KIND_ENTITY/KIND_KV_SCOPED/
            // KIND_COUNTER/KIND_BLOB, drop the legacy suppression and
            // disambiguate every family here.
            @SuppressWarnings({"deprecation", "removal"})
            boolean expectsTimeseriesLegacy = category.requiredCapabilities().contains(Capability.TIMESERIES_APPEND);
            boolean expectsTimeseriesV2 = category.requiredCapabilities().contains(Capability.KIND_EVENT_STREAM);
            boolean expectsTimeseries = expectsTimeseriesLegacy || expectsTimeseriesV2;
            boolean isTimeseries = kind instanceof EventStream<?, ?>;
            if (expectsTimeseries != isTimeseries) {
                failures.add("kind family mismatch: category " + category.id()
                    + (expectsTimeseries
                        ? " requires an EventStream kind (TIMESERIES_APPEND) but got "
                        : " requires a non-EventStream kind but got an EventStream: ")
                    + kind.name());
                logger.warning(() -> "[v2-bootstrap] " + category.id() + " ↔ " + kind.name()
                    + " kind-family mismatch (expectsTimeseries=" + expectsTimeseries
                    + ", isTimeseries=" + isTimeseries + ") — skipping binding");
                continue;
            }

            KindAdapter adapter;
            try {
                adapter = (KindAdapter) backend.adapterFor(kind).orElseThrow(
                    () -> new IllegalStateException("backend " + backend.id()
                        + " does not advertise an adapter for kind " + kind.name()));
            } catch (Exception e) {
                failures.add("adapterFor " + kind.name() + " on " + backend.id() + ": " + e.getMessage());
                logger.log(Level.WARNING,
                    "[v2-bootstrap] adapterFor " + kind.name() + " on " + backend.id() + " failed", e);
                continue;
            }

            try {
                adapter.ensureStore(storeId, kind);
            } catch (Exception e) {
                failures.add("ensureStore " + kind.name() + " on " + storeId + ": " + e.getMessage());
                logger.log(Level.WARNING,
                    "[v2-bootstrap] ensureStore " + kind.name() + " on " + storeId + " failed", e);
                // No migrations + no route when ensureStore fails — the
                // store isn't usable.
                continue;
            }

            List<Migration> migs = adapter.migrations(kind);
            logger.info(() -> "[v2-bootstrap] " + storeId + " (" + kind.name() + "): "
                + migs.size() + " migration(s)");
            boolean migrationFailed = false;
            for (Migration m : migs) {
                try {
                    logger.fine(() -> "[v2-bootstrap]   apply " + m.getClass().getSimpleName()
                        + " " + m.fromVersion() + " → " + m.toVersion());
                    m.apply(migrationCtx, storeId, kind);
                    logger.fine(() -> "[v2-bootstrap]   OK    " + m.getClass().getSimpleName());
                } catch (Exception e) {
                    failures.add("migration " + m.getClass().getSimpleName()
                        + " on " + storeId + ": " + e.getMessage());
                    logger.log(Level.WARNING,
                        "[v2-bootstrap] migration " + m.getClass().getSimpleName()
                            + " on " + storeId + " FAILED — stopping migration chain for binding,"
                            + " not registering route", e);
                    // Stop running later migrations: 2→3 after 1→2
                    // failed is unsafe because later migrations assume
                    // earlier ones succeeded.
                    migrationFailed = true;
                    break;
                }
            }

            if (migrationFailed) {
                // Fail-closed: do NOT register a route to un-migrated
                // data. A registered route silently dispatches reads
                // against the wrong physical layout (subtype-0 vs -4
                // UUIDs; v6 flat fields vs v7 meta-nested fields).
                continue;
            }

            registerRoute(routesBuilder, category, storeId, kind, backend);
        }

        return new Result(failures);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void registerRoute(@NotNull V2Routes.Builder routes,
                                      @NotNull Category category,
                                      @NotNull StoreId storeId,
                                      @NotNull DataKind kind,
                                      @NotNull BackendV2 backend) {
        routes.register(category, storeId, kind, backend);
    }
}
