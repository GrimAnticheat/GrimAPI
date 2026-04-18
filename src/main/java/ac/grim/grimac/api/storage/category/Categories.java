package ac.grim.grimac.api.storage.category;

import ac.grim.grimac.api.storage.model.BlobRef;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import org.jetbrains.annotations.ApiStatus;

import java.util.EnumSet;

@ApiStatus.Experimental
public final class Categories {

    public static final Category<ViolationRecord> VIOLATION = new Builtin<>(
            "violation",
            ViolationRecord.class,
            EnumSet.of(Capability.INDEXED_KV, Capability.TIMESERIES_APPEND, Capability.HISTORY),
            AccessPattern.TIMESERIES);

    public static final Category<SessionRecord> SESSION = new Builtin<>(
            "session",
            SessionRecord.class,
            EnumSet.of(Capability.INDEXED_KV, Capability.HISTORY),
            AccessPattern.INDEXED_KV);

    public static final Category<PlayerIdentity> PLAYER_IDENTITY = new Builtin<>(
            "player-identity",
            PlayerIdentity.class,
            EnumSet.of(Capability.INDEXED_KV, Capability.PLAYER_IDENTITY),
            AccessPattern.INDEXED_KV);

    public static final Category<SettingRecord> SETTING = new Builtin<>(
            "setting",
            SettingRecord.class,
            EnumSet.of(Capability.INDEXED_KV, Capability.SETTINGS),
            AccessPattern.INDEXED_KV);

    public static final Category<BlobRef> BLOB = new Builtin<>(
            "blob",
            BlobRef.class,
            EnumSet.of(Capability.BLOB),
            AccessPattern.BLOB_REF);

    private Categories() {}

    private record Builtin<R>(
            String id,
            Class<R> recordType,
            EnumSet<Capability> requiredCapabilities,
            AccessPattern accessPattern) implements Category<R> {}
}
