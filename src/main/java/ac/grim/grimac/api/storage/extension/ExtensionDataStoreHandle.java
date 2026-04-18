package ac.grim.grimac.api.storage.extension;

import ac.grim.grimac.api.storage.category.AccessPattern;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;

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

    String extensionId();

    <R> Category<R> declareCategory(
            String localId,
            Class<R> type,
            AccessPattern ap,
            EnumSet<Capability> required);

    <R> void submit(Category<R> cat, R record);

    <R> CompletionStage<Page<R>> query(Category<R> cat, Query<R> q);

    <R> CompletionStage<Void> delete(Category<R> cat, DeleteCriteria c);

    void putSetting(String key, byte[] value);

    Optional<byte[]> getSetting(String key);

    BlobStoreHandle blobs();
}
