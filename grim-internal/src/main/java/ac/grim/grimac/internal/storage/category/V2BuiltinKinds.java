package ac.grim.grimac.internal.storage.category;

import ac.grim.grimac.api.storage.event.CheckCatalogEvent;
import ac.grim.grimac.api.storage.event.PlayerIdentityEvent;
import ac.grim.grimac.api.storage.event.ServerInstanceEvent;
import ac.grim.grimac.api.storage.event.ServerStartupEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.event.VerboseSchemaEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.codec.Codec;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.codec.FieldKind;
import ac.grim.grimac.api.storage.codec.MergeMode;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.Granularity;
import ac.grim.grimac.api.storage.kind.IndexSpec;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.api.storage.model.SettingScope;
import ac.grim.grimac.api.storage.model.CheckCatalogRecord;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.ServerInstanceRecord;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.VerboseSchemaRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.internal.storage.codec.MethodHandleCodecFactory;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Static {@link ac.grim.grimac.api.storage.kind.DataKind} declarations for
 * each builtin store. Stateless — Kinds are immutable value records, safe
 * to hold as constants and reuse across registrations.
 * <p>
 * The matching {@link EventStreamCategoryImpl} / {@code EntityCategoryImpl} /
 * etc. instances are constructed lazily by the wiring layer (Phase 1.4c
 * adds the install hook that builds these against the active backend +
 * StoreId).
 */
@ApiStatus.Internal
public final class V2BuiltinKinds {

    static {
        // Install the default codec provider eagerly at class load time.
        // Previously this was lazy on violations() and could leave a hole
        // where any other Codecs.of(...) call (e.g. an extension built
        // a Kind before violations() was first invoked) saw the throwing
        // default provider. Idempotent — safe across reloads.
        MethodHandleCodecFactory.installAsDefaultProvider();
    }

    private V2BuiltinKinds() {}

    /** The violations EventStream Kind. */
    public static @NotNull EventStream<ViolationEvent, ViolationRecord> violations() {
        return EventStream.<ViolationEvent, ViolationRecord>builder()
            .name("violations")
            .event(ViolationEvent.class, ViolationEvent::new)
            .record(ViolationRecord.class)
            .codec(MethodHandleCodecFactory.get().create(ViolationRecord.class))
            .eventToRecord(V2BuiltinKinds::violationEventToRecord)
            .timestamp("occurred_at")
            .partition("session_id", "player_uuid", "check_id")
            // Binary verbose rows are ~70-90 bytes all-in, so half a year of
            // history is cheap; deployments override (or disable) sweeping
            // via the storage config's retention.violations rule.
            .retention(Duration.ofDays(180))
            .granularity(Granularity.SECONDS)
            .build();
    }

    /**
     * Convert a mutable ring-slot {@link ViolationEvent} into the immutable
     * {@link ViolationRecord} the codec encodes. Called once per write in
     * the handler thread.
     */
    static @NotNull ViolationRecord violationEventToRecord(@NotNull ViolationEvent e) {
        return new ViolationRecord(
            java.util.Objects.requireNonNull(e.id(), "ViolationEvent.id must be set before submit"),
            e.sessionId(),
            e.playerUuid(),
            e.checkId(),
            e.vl(),
            e.occurredEpochMs(),
            e.verboseData());
    }

    /**
     * The sessions Entity Kind. {@code _id = session_id} (UUID).
     * Secondary indexes:
     * <ul>
     *   <li>{@code by_player_started} → compound
     *       {@code (player_uuid ASC, started_at DESC)} — drives the
     *       {@code /grim history} session list ordering for one player.</li>
     *   <li>{@code by_started} → {@code started_at DESC} for cross-player
     *       global recency queries.</li>
     *   <li>{@code by_startup_open} → compound
     *       {@code (startup_id ASC, closed_at ASC)} so crash recovery can
     *       scan open rows for one JVM startup before closed historical
     *       rows.</li>
     * </ul>
     * No TTL (sessions are durable across the server's lifetime).
     */
    public static @NotNull Entity<UUID, SessionEvent, SessionRecord> sessions() {
        return Entity.<UUID, SessionEvent, SessionRecord>builder()
            .name("sessions")
            .event(SessionEvent.class, SessionEvent::new)
            .record(SessionRecord.class)
            .codec(MethodHandleCodecFactory.get().create(SessionRecord.class))
            .eventToRecord(V2BuiltinKinds::sessionEventToRecord)
            .id(UUID.class, SessionRecord::sessionId)
            .secondaryIndex(IndexSpec.of("by_player_started", "player_uuid", "-started_at"))
            .secondaryIndex(IndexSpec.of("by_startup_open", "startup_id", "closed_at"))
            .secondaryIndex(IndexSpec.of("by_started", "-started_at"))
            .build();
    }

    /**
     * Convert a mutable ring-slot {@link SessionEvent} into the immutable
     * {@link SessionRecord} the codec encodes. {@code closedAtEpochMs == 0L}
     * is the {@link SessionRecord#OPEN} sentinel — heartbeats leave it at 0,
     * graceful close stamps a real timestamp.
     */
    static @NotNull SessionRecord sessionEventToRecord(@NotNull SessionEvent e) {
        return new SessionRecord(
            e.sessionId(),
            e.playerUuid(),
            e.serverName(),
            e.startedEpochMs(),
            e.lastActivityEpochMs(),
            e.closedAtEpochMs(),
            e.grimVersion(),
            e.clientBrand(),
            e.clientVersion(),
            e.serverVersionString(),
            e.instanceId(),
            e.startupId(),
            List.copyOf(e.sessionBlobs()));
    }

    /**
     * The players Entity Kind. {@code _id = uuid}. Secondary index on
     * {@code current_name} with {@code caseInsensitivePrefix} so the
     * MongoEntityAdapter additionally provisions a companion
     * {@code current_name_lower} index for /grim history's by-name lookup.
     * Application code is responsible for writing the lowercased variant
     * alongside the canonical name on identity upserts (the codec doesn't
     * derive it).
     */
    public static @NotNull Entity<UUID, PlayerIdentityEvent, PlayerIdentity> players() {
        return Entity.<UUID, PlayerIdentityEvent, PlayerIdentity>builder()
            .name("players")
            .event(PlayerIdentityEvent.class, PlayerIdentityEvent::new)
            .record(PlayerIdentity.class)
            .codec(MethodHandleCodecFactory.get().create(PlayerIdentity.class))
            .eventToRecord(V2BuiltinKinds::playerEventToRecord)
            .id(UUID.class, PlayerIdentity::uuid)
            .secondaryIndex(new IndexSpec("by_name", List.of("current_name"), false, true))
            .build();
    }

    static @NotNull PlayerIdentity playerEventToRecord(@NotNull PlayerIdentityEvent e) {
        return new PlayerIdentity(
            e.uuid(),
            e.currentName(),
            e.firstSeenEpochMs(),
            e.lastSeenEpochMs());
    }

    /**
     * The check catalog Entity Kind. {@code _id = stable_key} (String).
     * Secondary index:
     * <ul>
     *   <li>{@code by_check_id} → unique on {@code check_id} for legacy
     *       lookup paths that still reference the surrogate integer
     *       check id.</li>
     * </ul>
     * No TTL; the catalog is durable until a check is renamed/removed.
     * Producers upsert on plugin load; ring-buffer slot is
     * {@link CheckCatalogEvent}.
     *
     * <p><strong>check_id immutability:</strong> the V2 Mongo entity
     * adapter {@code $set}s the entire encoded record on every upsert,
     * so {@code checkId} could be mutated by a stale producer republishing
     * the same {@code stableKey} with a different id — orphaning
     * historical violation rows that reference the old id. Until an
     * {@code @InsertOnly} codec annotation lands (deferred from the
     * Phase 3c review), the contract is: <em>consumers MUST reuse the
     * existing {@code checkId} for a given {@code stableKey}</em>.
     * Allocate a new id only on first registration. See
     * {@code MongoCheckCatalogPersistence.upsert} for the v1 reference
     * implementation of conflict-check semantics.
     */
    public static @NotNull Entity<String, CheckCatalogEvent, CheckCatalogRecord> checks() {
        return Entity.<String, CheckCatalogEvent, CheckCatalogRecord>builder()
            .name("checks")
            .event(CheckCatalogEvent.class, CheckCatalogEvent::new)
            .record(CheckCatalogRecord.class)
            .codec(MethodHandleCodecFactory.get().create(CheckCatalogRecord.class))
            .eventToRecord(V2BuiltinKinds::checkCatalogEventToRecord)
            .id(String.class, CheckCatalogRecord::stableKey)
            .secondaryIndex(IndexSpec.unique("by_check_id", "check_id"))
            .build();
    }

    static @NotNull CheckCatalogRecord checkCatalogEventToRecord(@NotNull CheckCatalogEvent e) {
        String key = e.stableKey();
        if (key == null) {
            throw new IllegalStateException("CheckCatalogEvent.stableKey must be set before submit");
        }
        return new CheckCatalogRecord(
            key,
            e.checkId(),
            e.display(),
            e.description(),
            e.introducedVersion(),
            e.introducedAt());
    }

    /**
     * The binary verbose schema dictionary. {@code _id = schema_key}
     * ({@code "<flavor>:<checkId>:<version>"}). No TTL; layouts must remain
     * available for old history rows after checks or builds disappear.
     */
    public static @NotNull Entity<String, VerboseSchemaEvent, VerboseSchemaRecord> verboseSchemas() {
        return Entity.<String, VerboseSchemaEvent, VerboseSchemaRecord>builder()
            .name("verbose_schemas")
            .event(VerboseSchemaEvent.class, VerboseSchemaEvent::new)
            .record(VerboseSchemaRecord.class)
            .codec(MethodHandleCodecFactory.get().create(VerboseSchemaRecord.class))
            .eventToRecord(V2BuiltinKinds::verboseSchemaEventToRecord)
            .id(String.class, VerboseSchemaRecord::schemaKey)
            .secondaryIndex(IndexSpec.of("by_flavor", "flavor"))
            .secondaryIndex(IndexSpec.of("by_check_id", "check_id"))
            .build();
    }

    static @NotNull VerboseSchemaRecord verboseSchemaEventToRecord(@NotNull VerboseSchemaEvent e) {
        String key = e.schemaKey();
        byte[] layout = e.layout();
        if (key == null) throw new IllegalStateException("VerboseSchemaEvent.schemaKey must be set before submit");
        if (e.version() < 1) throw new IllegalStateException("VerboseSchemaEvent.version must be positive before submit");
        if (layout == null) throw new IllegalStateException("VerboseSchemaEvent.layout must be set before submit");
        return new VerboseSchemaRecord(
            key,
            e.flavor(),
            e.checkId(),
            e.version(),
            layout,
            e.introducedAt());
    }

    /**
     * The server instance registry Entity Kind — one row per live JVM
     * writing against the shared backend, used by the multi-server
     * crash sweep to distinguish "live but quiet" sessions from
     * "dead instance, sweep eligible" sessions. See
     * {@code .docs/storage-redesign/07-crash-recovery.md}.
     * <p>
     * Secondary index:
     * <ul>
     *   <li>{@code by_server_name} → {@code server_name} for the
     *       {@code /grim instances} admin listing grouped by display
     *       label.</li>
     * </ul>
     * No TTL declared at the Kind level yet — backends that support
     * native TTL (Mongo {@code expireAfterSeconds}, Redis {@code EX})
     * wire it up at {@code ensureStore} time keyed off the
     * {@code last_heartbeat} field. SQL backends run a scheduled
     * DELETE on the same cadence as the sweep.
     */
    public static @NotNull Entity<UUID, ServerInstanceEvent, ServerInstanceRecord> instances() {
        return Entity.<UUID, ServerInstanceEvent, ServerInstanceRecord>builder()
            .name("server_instances")
            .event(ServerInstanceEvent.class, ServerInstanceEvent::new)
            .record(ServerInstanceRecord.class)
            .codec(MethodHandleCodecFactory.get().create(ServerInstanceRecord.class))
            .eventToRecord(V2BuiltinKinds::instanceEventToRecord)
            .id(UUID.class, ServerInstanceRecord::instanceId)
            .secondaryIndex(IndexSpec.of("by_server_name", "server_name"))
            .build();
    }

    /**
     * Durable metadata for one Grim JVM startup. Sessions store only
     * {@code startup_id}; history display resolves server/version metadata
     * through this row, and crash recovery uses its heartbeat.
     */
    public static @NotNull Entity<UUID, ServerStartupEvent, ServerStartupRecord> serverStartups() {
        return Entity.<UUID, ServerStartupEvent, ServerStartupRecord>builder()
            .name("server_startups")
            .event(ServerStartupEvent.class, ServerStartupEvent::new)
            .record(ServerStartupRecord.class)
            .codec(MethodHandleCodecFactory.get().create(ServerStartupRecord.class))
            .eventToRecord(V2BuiltinKinds::startupEventToRecord)
            .id(UUID.class, ServerStartupRecord::startupId)
            .secondaryIndex(IndexSpec.of("by_instance_open", "instance_id", "closed_at"))
            .secondaryIndex(IndexSpec.of("by_open_heartbeat", "closed_at", "last_heartbeat"))
            .secondaryIndex(IndexSpec.of("by_server_name", "server_name"))
            .build();
    }

    /**
     * The settings KeyValueScoped Kind. One document/row per
     * {@code (scope, scope_key)} tenant, with each player toggle, server
     * config flag, or extension preference embedded under the per-tenant
     * envelope's {@code values} sub-document. Replaces the v6
     * one-document-per-setting layout — see
     * {@code .docs/storage-redesign/01-data-kinds.md}.
     * <p>
     * Value type is opaque {@code byte[]} so the bridge from
     * {@code SettingEvent.value()} round-trips losslessly across every
     * backend without per-value codec generation; callers encode/decode
     * their own value shapes (e.g. {@code PlayerToggleStoreImpl}
     * encodes booleans as a single byte).
     */
    public static @NotNull KeyValueScoped<SettingScope, byte[]> settings() {
        return KeyValueScoped.<SettingScope, byte[]>builder()
            .name("settings")
            .scope(SettingScope.class)
            .value(byte[].class, OPAQUE_BYTES_CODEC)
            .build();
    }

    /**
     * Stub codec for opaque {@code byte[]} KV values. KV adapters store
     * the raw bytes via backend-native binary types (Mongo {@code BsonBinary},
     * Postgres {@code BYTEA}, etc.) and never invoke this codec on the
     * value path; the {@link KeyValueScoped} builder only requires a
     * non-null reference. Methods throw to make accidental codegen
     * routing through this codec a hard error rather than silent
     * mis-encoding.
     */
    private static final Codec<byte[]> OPAQUE_BYTES_CODEC = new Codec<byte[]>() {
        private final EncodeShape shape = new EncodeShape(
            "value",
            null,
            List.of(),
            List.of(),
            List.of(),
            List.of(new EncodeShape.FieldDef("value", byte[].class, FieldKind.VALUE,
                false, null, 0, 0, MergeMode.OVERWRITE, 0L)),
            1);
        @Override public @NotNull Class<byte[]> recordType() { return byte[].class; }
        @Override public @NotNull EncodeShape shape()        { return shape; }
        @Override public int version()                       { return 1; }
        @Override public Object indexField(@NotNull byte[] record, @NotNull String fieldName) {
            throw new UnsupportedOperationException(
                "opaque-bytes codec has no introspectable fields; KV adapters write raw bytes");
        }
    };

    static @NotNull ServerInstanceRecord instanceEventToRecord(@NotNull ServerInstanceEvent e) {
        UUID id = e.instanceId();
        String name = e.serverName();
        if (id == null)   throw new IllegalStateException("ServerInstanceEvent.instanceId must be set before submit");
        if (name == null) throw new IllegalStateException("ServerInstanceEvent.serverName must be set before submit");
        return new ServerInstanceRecord(
            id,
            name,
            e.startedEpochMs(),
            e.lastHeartbeatEpochMs(),
            e.hostname(),
            e.grimVersion(),
            e.drainMode());
    }

    static @NotNull ServerStartupRecord startupEventToRecord(@NotNull ServerStartupEvent e) {
        UUID startupId = e.startupId();
        UUID instanceId = e.instanceId();
        String name = e.serverName();
        if (startupId == null) throw new IllegalStateException("ServerStartupEvent.startupId must be set before submit");
        if (instanceId == null) throw new IllegalStateException("ServerStartupEvent.instanceId must be set before submit");
        if (name == null)      throw new IllegalStateException("ServerStartupEvent.serverName must be set before submit");
        return new ServerStartupRecord(
            startupId,
            instanceId,
            name,
            e.grimVersion(),
            e.serverVersionString(),
            e.hostname(),
            e.startedEpochMs(),
            e.lastHeartbeatEpochMs(),
            e.closedAtEpochMs(),
            e.closeReason(),
            e.verboseManifest());
    }
}
