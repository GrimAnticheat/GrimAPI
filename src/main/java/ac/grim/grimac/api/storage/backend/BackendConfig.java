package ac.grim.grimac.api.storage.backend;

import org.jetbrains.annotations.ApiStatus;

/**
 * Marker for per-backend configuration. Each {@link Backend} impl supplies its own
 * subtype (e.g. {@code SqliteBackendConfig}). The shared-impl facade holds
 * {@code Map<String, BackendConfig>} keyed by backend id.
 */
@ApiStatus.Experimental
public interface BackendConfig {}
