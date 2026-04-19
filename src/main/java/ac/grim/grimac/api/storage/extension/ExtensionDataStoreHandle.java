package ac.grim.grimac.api.storage.extension;

import ac.grim.grimac.api.storage.category.AccessPattern;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Scoped handle an extension receives instead of the raw {@code DataStore}.
 * Every category declared through this handle is auto-namespaced; all reads and
 * writes can only touch the extension's namespace.
 */
@ApiStatus.Experimental
public interface ExtensionDataStoreHandle {

    @NotNull String extensionId();

    @NotNull <R> Category<R> declareCategory(
            @NotNull String localId,
            @NotNull Class<R> type,
            @NotNull AccessPattern ap,
            @NotNull EnumSet<Capability> required);

    <R> void submit(@NotNull Category<R> cat, @NotNull R record);

    @NotNull <R> CompletionStage<Page<R>> query(@NotNull Category<R> cat, @NotNull Query<R> q);

    @NotNull <R> CompletionStage<Void> delete(@NotNull Category<R> cat, @NotNull DeleteCriteria c);

    void putSetting(@NotNull String key, byte @NotNull [] value);

    @NotNull Optional<byte[]> getSetting(@NotNull String key);

    @NotNull BlobStoreHandle blobs();
}
