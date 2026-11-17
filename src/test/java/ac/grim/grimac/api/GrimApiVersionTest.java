package ac.grim.grimac.api;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class GrimApiVersionTest {

    @Test
    void reportsPackagedApiVersion() throws IOException {
        Properties properties = new Properties();
        try (InputStream stream = GrimApiVersion.class.getResourceAsStream("version.properties")) {
            properties.load(stream);
        }

        String version = GrimApiVersion.get();
        assertFalse(version.isBlank());
        assertFalse(version.contains("${"));
        assertEquals(properties.getProperty("version"), version);
    }
}
