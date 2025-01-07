package ac.grim.grimac.api.feature;

import ac.grim.grimac.api.common.BasicReloadable;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

public interface FeatureManager extends BasicReloadable {

    /**
     * Retrieves a collection of all feature keys.
     * @return Collection of feature keys
     */
    Collection<String> getFeatureKeys();

    /**
     * Retrieves the state of a feature. Null if the feature does not exist.
     * @param key Feature key
     * @return Feature state
     */
    @Nullable FeatureState getFeatureState(String key);

    /**
     * Checks if a feature is enabled.
     * @param key Feature key
     * @return True if enabled
     */
    boolean isFeatureEnabled(String key);

    /**
     *  Sets the state of a feature.
     * @param key Feature key
     * @param state State to set the feature to.
     * @return True if the state was successfully set.
     */
    boolean setFeatureState(String key, FeatureState state);

}
