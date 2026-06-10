package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.AdminAdapter;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.SearchAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.backend.TxAdapter;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link V2Routes.Builder} duplicate-detection semantics
 * (the codex-flagged silent-overwrite bug from Phase 1.4c.1). Uses an
 * in-test stub backend / adapter so the test doesn't require live Mongo.
 */
@DisplayName("V2Routes.Builder duplicate registration")
class V2RoutesBuilderTest {

    @Test
    @DisplayName("duplicate category registration throws IllegalStateException")
    void duplicateRegistrationThrows() {
        StubBackend backend = new StubBackend("backend-a");
        V2Routes.Builder b = V2Routes.builder();

        // First register succeeds.
        b.register(Categories.SESSION, StoreId.grim("sessions"), V2BuiltinKinds.sessions(), backend);

        // Second register with the same Category instance throws.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> b.register(Categories.SESSION,
                StoreId.grim("sessions_alt"), V2BuiltinKinds.sessions(), new StubBackend("backend-b")));

        assertTrue(ex.getMessage().contains("duplicate v2 route registration"),
            "exception message should describe the duplicate: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("backend-a"),
            "exception message should name the existing backend: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("backend-b"),
            "exception message should name the attempted backend: " + ex.getMessage());
    }

    @Test
    @DisplayName("register different categories on the same builder is fine")
    void differentCategoriesCoexist() {
        StubBackend backend = new StubBackend("backend-a");
        V2Routes.Builder b = V2Routes.builder();
        b.register(Categories.SESSION, StoreId.grim("sessions"), V2BuiltinKinds.sessions(), backend);
        b.register(Categories.PLAYER_IDENTITY, StoreId.grim("players"), V2BuiltinKinds.players(), backend);
        V2Routes built = b.build();
        V2Routes.Route<?> sessionRoute = built.routeFor(Categories.SESSION);
        V2Routes.Route<?> playerRoute  = built.routeFor(Categories.PLAYER_IDENTITY);
        assertTrue(sessionRoute != null && playerRoute != null,
            "both routes should resolve");
        assertTrue(sessionRoute.storeId().equals(StoreId.grim("sessions")),
            "session route should target grim:sessions, got " + sessionRoute.storeId());
        assertTrue(playerRoute.storeId().equals(StoreId.grim("players")),
            "player route should target grim:players, got " + playerRoute.storeId());
    }

    @Test
    @DisplayName("identity-keyed: distinct Category instances with same id() do NOT collide")
    void identitySemanticsAllowDuplicateIds() {
        // Build two Category instances that .equals() each other but
        // are NOT the same identity reference. Per V2Routes javadoc, the
        // duplicate check is IDENTITY-based — these should coexist.
        Category<?> a = new SameIdCategory("custom-cat");
        Category<?> b = new SameIdCategory("custom-cat");
        assertTrue(a.equals(b), "the two categories must be .equals() to prove identity semantics");
        assertTrue(a != b, "the two categories must NOT be the same identity reference");

        StubBackend backend = new StubBackend("backend-a");
        V2Routes.Builder bld = V2Routes.builder();
        @SuppressWarnings({"unchecked", "rawtypes"})
        Category aErased = a;
        @SuppressWarnings({"unchecked", "rawtypes"})
        Category bErased = b;
        bld.register(aErased, StoreId.grim("store_a"), V2BuiltinKinds.sessions(), backend);
        // Second register with .equals()-equal but distinct identity → does NOT collide.
        bld.register(bErased, StoreId.grim("store_b"), V2BuiltinKinds.sessions(), backend);
        V2Routes built = bld.build();
        assertTrue(built.contains(a) && built.contains(b),
            "both identity-distinct categories should be registered");
        // Stronger proof: each route must resolve to its own storeId
        // (not collapsed into a single bucket by .equals() semantics).
        V2Routes.Route<?> routeA = built.routeFor(a);
        V2Routes.Route<?> routeB = built.routeFor(b);
        assertTrue(routeA != null && routeA.storeId().equals(StoreId.grim("store_a")),
            "route for identity a must target grim:store_a, got " + (routeA == null ? "null" : routeA.storeId()));
        assertTrue(routeB != null && routeB.storeId().equals(StoreId.grim("store_b")),
            "route for identity b must target grim:store_b, got " + (routeB == null ? "null" : routeB.storeId()));
    }

    /** Same-id Category that overrides equals/hashCode but lives at a distinct identity. */
    private record SameIdCategory(@NotNull String customId) implements Category<Object> {
        @Override public @NotNull String id() { return customId; }
        @Override public @NotNull Class<Object> eventType() { return Object.class; }
        @Override public @NotNull java.util.function.Supplier<Object> newEvent() { return Object::new; }
        @Override public @NotNull Class<?> queryResultType() { return Object.class; }
        @Override public @NotNull EnumSet<Capability> requiredCapabilities() { return EnumSet.noneOf(Capability.class); }
        @Override @SuppressWarnings({"deprecation", "removal"})
        public @NotNull ac.grim.grimac.api.storage.category.AccessPattern accessPattern() {
            return ac.grim.grimac.api.storage.category.AccessPattern.INDEXED_KV;
        }
    }

    // ---- Minimal stub backend ----
    private static final class StubBackend implements BackendV2 {
        private final String id;
        StubBackend(String id) { this.id = id; }

        @Override public @NotNull String id() { return id; }
        @Override public @NotNull ApiVersion apiVersion() { return ApiVersion.CURRENT; }
        @Override public @NotNull EnumSet<Capability> capabilities() { return EnumSet.noneOf(Capability.class); }
        @Override public void init(@NotNull BackendContext ctx) {}
        @Override public void flush() {}
        @Override public void close() {}
        @Override public @NotNull Optional<SearchAdapter> searchAdapter() { return Optional.empty(); }
        @Override public @NotNull Optional<TxAdapter> txAdapter() { return Optional.empty(); }
        @Override public @NotNull Optional<AdminAdapter> adminAdapter() { return Optional.empty(); }
        @Override public <X> @NotNull Optional<X> unwrap(@NotNull Class<X> type) { return Optional.empty(); }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public <K extends DataKind<?, ?>> @NotNull Optional<KindAdapter<K>> adapterFor(@NotNull K kind) {
            // Return a no-op adapter. The builder calls adapterFor()
            // during register() and stores the returned KindAdapter on
            // the Route — no other method on the adapter is exercised
            // by the duplicate-detection test.
            return Optional.of((KindAdapter<K>) new StubAdapter(kind.getClass()));
        }
    }

    @SuppressWarnings("rawtypes")
    private static final class StubAdapter implements KindAdapter {
        private final Class<?> kindCls;
        StubAdapter(Class<?> kindCls) { this.kindCls = kindCls; }

        @Override @SuppressWarnings({"unchecked", "rawtypes"})
        public @NotNull Class kindType() { return kindCls; }
        @Override public @NotNull EnumSet<Capability> subcapabilities() { return EnumSet.noneOf(Capability.class); }
        @Override public void ensureStore(@NotNull StoreId id, @NotNull DataKind kind) {}
        @Override public void dropStore(@NotNull StoreId id, @NotNull DataKind kind) {}
        @Override public StorageEventHandler writeHandler(
            @NotNull StoreId id, @NotNull DataKind kind, @NotNull Category category) {
            return (event, sequence, endOfBatch) -> {};
        }
        @Override public Object execute(@NotNull StoreId id, @NotNull DataKind kind, @NotNull Operation op)
            throws BackendException {
            throw new BackendException("StubAdapter.execute should not be reached in duplicate-detection test");
        }
        @Override public @NotNull List migrations(@NotNull DataKind kind) { return List.of(); }
    }
}
