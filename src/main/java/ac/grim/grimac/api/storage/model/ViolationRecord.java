package ac.grim.grimac.api.storage.model;

import ac.grim.grimac.api.storage.codec.Id;
import ac.grim.grimac.api.storage.codec.Name;
import ac.grim.grimac.api.storage.codec.Nullable;
import ac.grim.grimac.api.storage.codec.Partition;
import ac.grim.grimac.api.storage.codec.Persistent;
import ac.grim.grimac.api.storage.codec.Timestamp;
import ac.grim.grimac.api.storage.codec.Value;
import org.jetbrains.annotations.ApiStatus;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Immutable read-side violation row. The {@code id} is a UUIDv7 generated on
 * the write path; its timestamp prefix gives storage engines a sortable,
 * globally unique tie-breaker while {@code occurredEpochMs} remains the event
 * time used for history ordering.
 * <p>
 * Field roles (see {@code .docs/storage-redesign/02-codec-and-codegen.md}):
 * id is the primary key; sessionId / playerUuid / checkId are partition keys
 * the EventStream's KindAdapter projects into Mongo timeseries {@code meta},
 * SQL partition columns, or Redis hash-tag fragments; occurredEpochMs is the
 * event time (encoded on disk as {@code occurred_at} to match the v6 schema
 * and the Mongo timeseries {@code timeField} contract); verbose is the
 * opaque, versioned-by-startup payload.
 */
@ApiStatus.Experimental
@Persistent
public record ViolationRecord(
        @Id                                                    UUID id,
        @Partition                                             UUID sessionId,
        @Partition                                             UUID playerUuid,
        @Partition                                             int checkId,
        @Value                                                 double vl,
        @Timestamp @Name("occurred_at")                        long occurredEpochMs,
        @Value @Name("verbose") @Nullable                      byte[] verboseData,
        @Value @Name("verbose_format")                         VerboseFormat verboseFormat) {

    public ViolationRecord {
        if (id == null) throw new IllegalArgumentException("id");
        if (sessionId == null) throw new IllegalArgumentException("sessionId");
        if (playerUuid == null) throw new IllegalArgumentException("playerUuid");
        if (verboseFormat == null) verboseFormat = VerboseFormat.TEXT;
    }

    public ViolationRecord(
            UUID id,
            UUID sessionId,
            UUID playerUuid,
            int checkId,
            double vl,
            long occurredEpochMs,
            byte[] verboseData) {
        this(id, sessionId, playerUuid, checkId, vl, occurredEpochMs, verboseData, VerboseFormat.TEXT);
    }

    public ViolationRecord(
            UUID id,
            UUID sessionId,
            UUID playerUuid,
            int checkId,
            double vl,
            long occurredEpochMs,
            @org.jetbrains.annotations.Nullable String verbose,
            VerboseFormat verboseFormat) {
        this(id, sessionId, playerUuid, checkId, vl, occurredEpochMs,
                verbose == null ? null : verbose.getBytes(StandardCharsets.UTF_8),
                verboseFormat);
    }

    public @org.jetbrains.annotations.Nullable String verbose() {
        return verboseData == null ? null : new String(verboseData, StandardCharsets.UTF_8);
    }

}
