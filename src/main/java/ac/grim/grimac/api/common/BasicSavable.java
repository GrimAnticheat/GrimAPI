package ac.grim.grimac.api.common;

import java.util.concurrent.CompletableFuture;

public interface BasicSavable {

    /**
     * Saves the object.
     */
    void save();

    /**
     * Saves the object asynchronously if supported.
     * Otherwise, it will run on the current thread.
     * @return boolean
     */
    default CompletableFuture<Boolean> saveAsync() {
        try {
            save();
            return CompletableFuture.completedFuture(true);
        } catch (Exception e) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    /**
     * If the object is saved asynchronously.
     *
     * @return boolean
     */
    default boolean isSavedAsync() {
        return false;
    }

}
