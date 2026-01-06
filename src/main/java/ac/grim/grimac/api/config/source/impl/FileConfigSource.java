package ac.grim.grimac.api.config.source.impl;

import ac.grim.grimac.api.config.source.ConfigContext;
import ac.grim.grimac.api.config.source.ConfigSource;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Objects;

/**
 * Standard implementation of a file-based config source.
 * <p>
 * Internal use only. Use {@link ConfigSource#file(String, File, Class)} instead.
 */
@ApiStatus.Internal
public final class FileConfigSource implements ConfigSource {

    private final String id;
    private final File file;
    private final Class<?> resourceOwner;

    public FileConfigSource(@NotNull String id, @NotNull File file, @NotNull Class<?> resourceOwner) {
        this.id = id;
        this.file = file;
        this.resourceOwner = resourceOwner;
    }

    @Override
    public @NotNull String getId() {
        return id;
    }

    @Override
    public void load(@NotNull ConfigContext context) {
        // The Visitor Pattern:
        // We tell the context "I am a file source, please handle me as a file."
        context.addFileSource(this.id, this.file, this.resourceOwner);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileConfigSource that = (FileConfigSource) o;
        return Objects.equals(id, that.id) && 
               Objects.equals(file, that.file) && 
               Objects.equals(resourceOwner, that.resourceOwner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, file, resourceOwner);
    }
}