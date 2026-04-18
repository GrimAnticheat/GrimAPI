package ac.grim.grimac.api.storage.category;

import org.jetbrains.annotations.ApiStatus;

import java.util.EnumSet;

@ApiStatus.Experimental
public interface Category<R> {

    String id();

    Class<R> recordType();

    EnumSet<Capability> requiredCapabilities();

    AccessPattern accessPattern();
}
