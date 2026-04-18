package ac.grim.grimac.api.storage.backend;

import org.jetbrains.annotations.ApiStatus;

/**
 * SPI for constructing a {@link Backend} from its {@link BackendConfig}.
 * Each backend id maps to exactly one provider.
 */
@ApiStatus.Experimental
public interface BackendProvider {

    String id();

    Class<? extends BackendConfig> configType();

    Backend create(BackendConfig config);
}
