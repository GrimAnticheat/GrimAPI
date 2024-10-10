package ac.grim.grimac.api.common;

import java.util.concurrent.CompletableFuture;

public interface BasicReloadable {

    /**
     * Reloads the object.
     */
    void reload();

    /**
     * If the object is reloaded asynchronously.
     * @return boolean
     */
    default boolean isLoadedAsync() {
        return false;
    }

    /**
     * Reloads the object asynchronously if supported.
     * Otherwise, it will run on the current thread.
     * @return a future that completes when the reload is done
     */
    default CompletableFuture<Boolean> reloadAsync() {
        try {
            reload();
            return CompletableFuture.completedFuture(true);
        } catch (Exception e) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

}
