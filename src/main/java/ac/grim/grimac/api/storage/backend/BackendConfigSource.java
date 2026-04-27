package ac.grim.grimac.api.storage.backend;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Read-only view of a single backend's settings.
 * <p>
 * Platforms that host the DataStore (a Minecraft plugin, a test harness, a
 * standalone tool, …) open one source per backend id — from a YAML file, a
 * code-built map, a database, whatever — and hand it to
 * {@link BackendProvider#readConfig(BackendConfigSource)} so each provider can
 * materialise its own {@link BackendConfig} without the platform needing to
 * know which keys the backend cares about.
 * <p>
 * Every getter takes a default returned when the key is absent; there is no
 * exception-throwing variant on purpose. A backend that cannot accept a
 * default should validate in its constructor or {@link Backend#init}.
 */
@ApiStatus.Experimental
public interface BackendConfigSource {

    @NotNull String getString(@NotNull String key, @NotNull String defaultValue);

    int getInt(@NotNull String key, int defaultValue);

    long getLong(@NotNull String key, long defaultValue);

    boolean getBoolean(@NotNull String key, boolean defaultValue);

    @NotNull List<String> getStringList(@NotNull String key, @NotNull List<String> defaultValue);
}
