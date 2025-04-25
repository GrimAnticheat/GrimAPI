package ac.grim.grimac.api;

import java.util.concurrent.CompletableFuture;

public final class GrimAPIProvider {
    private static GrimAbstractAPI instance;
    private static final CompletableFuture<GrimAbstractAPI> futureInstance = new CompletableFuture<>();

    private GrimAPIProvider() {
        // Private constructor to prevent instantiation
    }

    /**
     * Initializes the GrimAPI instance during mod loading.
     * This method should only be called once by the mod initializer.
     *
     * @param api The GrimAbstractAPI instance to initialize.
     * @throws IllegalStateException If the API is already initialized.
     */
    public static void init(GrimAbstractAPI api) {
        if (instance != null || futureInstance.isDone()) {
            throw new IllegalStateException("GrimAPI is already initialized");
        }
        instance = api;
        futureInstance.complete(api); // Complete the future with the API instance
    }

    /**
     * Gets the GrimAPI instance synchronously.
     *
     * @return The GrimAbstractAPI instance.
     * @throws IllegalStateException If the API is not loaded.
     */
    public static GrimAbstractAPI get() {
        if (instance == null) {
            throw new IllegalStateException("GrimAPI is not loaded. Ensure the Grim mod is installed and initialized.");
        }
        return instance;
    }

    /**
     * Gets the GrimAPI instance asynchronously.
     * The returned CompletableFuture will complete when the GrimAPI instance is available.
     * If the API is already loaded, the future will complete immediately.
     * If the API fails to load (e.g., the mod is not installed), the future will complete exceptionally.
     *
     * @return A CompletableFuture that completes with the GrimAbstractAPI instance.
     */
    public static CompletableFuture<GrimAbstractAPI> getAsync() {
        if (instance != null) {
            // If the instance is already loaded, return a completed future
            return CompletableFuture.completedFuture(instance);
        }
        return futureInstance;
    }
}