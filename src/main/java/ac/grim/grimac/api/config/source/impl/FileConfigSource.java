package ac.grim.grimac.api.config.source.impl;

import ac.grim.grimac.api.config.source.ConfigSource;
import org.jetbrains.annotations.NotNull;
import java.io.File;

public final class FileConfigSource implements ConfigSource {
    private final String id;
    private final File file;
    private final Class<?> resourceOwner;

    public FileConfigSource(String id, File file, Class<?> resourceOwner) {
        this.id = id;
        this.file = file;
        this.resourceOwner = resourceOwner;
    }

    @Override public @NotNull String getId() { return id; }
    public File getFile() { return file; }
    public Class<?> getResourceOwner() { return resourceOwner; }
}