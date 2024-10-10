package ac.grim.grimac.api.common;

import java.util.concurrent.CompletableFuture;

public interface GenericReloadable<T> {

    /**
     * Reloads using the specified configuration object.
     *
     * @param object the object to reload with
     */
    void reload(T object);

    /**
     * Reloads using the specified configuration asynchronously if supported.
     * Otherwise, it will run on the current thread.
     *
     * @param config the configuration to reload with
     * @return a future that completes when the reload is done
     */
    default CompletableFuture<Boolean> reloadAsync(T config) {
        try {
            reload(config);
            return CompletableFuture.completedFuture(true);
        } catch (Exception e) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

}
