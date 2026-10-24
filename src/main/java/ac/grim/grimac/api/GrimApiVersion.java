package ac.grim.grimac.api;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class GrimApiVersion {
    private static final String VERSION = loadVersion();

    private GrimApiVersion() {
    }

    public static @NotNull String get() {
        return VERSION;
    }

    private static String loadVersion() {
        try (InputStream stream = GrimApiVersion.class.getResourceAsStream("version.properties")) {
            if (stream == null) {
                throw new IllegalStateException("GrimAPI version resource is missing");
            }
            Properties properties = new Properties();
            properties.load(stream);
            String version = properties.getProperty("version");
            if (version == null || version.isBlank()) {
                throw new IllegalStateException("GrimAPI version resource is invalid");
            }
            return version;
        } catch (IOException e) {
            throw new IllegalStateException("Could not read GrimAPI version", e);
        }
    }
}
