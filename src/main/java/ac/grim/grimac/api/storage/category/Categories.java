package ac.grim.grimac.api.storage.category;

import ac.grim.grimac.api.storage.event.BlobEvent;
import ac.grim.grimac.api.storage.event.PlayerIdentityEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.event.SettingEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionBlobRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.function.Supplier;

@ApiStatus.Experimental
public final class Categories {

    public static final Category<ViolationEvent> VIOLATION = new Builtin<>(
            "violation",
            ViolationEvent.class,
            ViolationEvent::new,
            ViolationRecord.class,
            EnumSet.of(Capability.INDEXED_KV, Capability.TIMESERIES_APPEND, Capability.HISTORY),
            AccessPattern.TIMESERIES);

    public static final Category<SessionEvent> SESSION = new Builtin<>(
            "session",
            SessionEvent.class,
            SessionEvent::new,
            SessionRecord.class,
            EnumSet.of(Capability.INDEXED_KV, Capability.HISTORY),
            AccessPattern.INDEXED_KV);

    public static final Category<PlayerIdentityEvent> PLAYER_IDENTITY = new Builtin<>(
            "player-identity",
            PlayerIdentityEvent.class,
            PlayerIdentityEvent::new,
            PlayerIdentity.class,
            EnumSet.of(Capability.INDEXED_KV, Capability.PLAYER_IDENTITY),
            AccessPattern.INDEXED_KV);

    public static final Category<SettingEvent> SETTING = new Builtin<>(
            "setting",
            SettingEvent.class,
            SettingEvent::new,
            SettingRecord.class,
            EnumSet.of(Capability.INDEXED_KV, Capability.SETTINGS),
            AccessPattern.INDEXED_KV);

    /**
     * Session-attached blob metadata. Blob bytes themselves live in the
     * configured {@code BlobStore}; this category only stores the small
     * session/player/timeline attachment record.
     */
    public static final Category<BlobEvent> BLOB = new Builtin<>(
            "blob",
            BlobEvent.class,
            BlobEvent::new,
            SessionBlobRecord.class,
            EnumSet.of(Capability.BLOB),
            AccessPattern.BLOB_REF);

    private Categories() {}

    private record Builtin<E>(
            @NotNull String id,
            @NotNull Class<E> eventType,
            @NotNull Supplier<E> newEvent,
            @NotNull Class<?> queryResultType,
            @NotNull EnumSet<Capability> requiredCapabilities,
            @NotNull AccessPattern accessPattern) implements Category<E> {}
}
