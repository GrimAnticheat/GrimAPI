package ac.grim.grimac.api.event;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface GrimEventHandler {
    int priority() default 0; // Same as your current system (higher = earlier)
    boolean ignoreCancelled() default false; // Support for ignoring cancelled events
}
