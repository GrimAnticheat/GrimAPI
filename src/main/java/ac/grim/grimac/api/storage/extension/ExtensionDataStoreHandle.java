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
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Scoped handle an extension receives instead of the raw {@code DataStore}.
 * Every category declared through this handle is auto-namespaced; all reads and
 * writes can only touch the extension's namespace.
 */
@ApiStatus.Experimental
public interface ExtensionDataStoreHandle {

    @NotNull String extensionId();

    /**
     * Declare a new extension-namespaced category. Extensions provide a mutable
     * event type for the hot path and an immutable result type for reads; the
     * shape mirrors the Layer 1 {@link Category} contract.
     */
    @NotNull <E> Category<E> declareCategory(
            @NotNull String localId,
            @NotNull Class<E> eventType,
            @NotNull Supplier<E> eventFactory,
            @NotNull Class<?> queryResultType,
            @NotNull AccessPattern ap,
            @NotNull EnumSet<Capability> required);

    <E> void submit(@NotNull Category<E> cat, @NotNull Consumer<E> configurer);

    @NotNull <R> CompletionStage<Page<R>> query(@NotNull Category<?> cat, @NotNull Query<R> q);

    @NotNull <E> CompletionStage<Void> delete(@NotNull Category<E> cat, @NotNull DeleteCriteria c);

    void putSetting(@NotNull String key, byte @NotNull [] value);

    @NotNull Optional<byte[]> getSetting(@NotNull String key);

    @NotNull BlobStoreHandle blobs();
}
