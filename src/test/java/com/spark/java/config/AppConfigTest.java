package com.spark.java.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class AppConfigTest {

    @Test
    public void testGetValidKey() {
        // Ensure the value for a valid key matches the expected value in application.conf
        String appName = AppConfig.get("spark.app.name");
        assertEquals("IMDB Batch Job", appName, "The app name should match the value in the configuration file.");
    }

    @Test
    public void testGetValidKeyMaster() {
        // Test another key
        String master = AppConfig.get("spark.master");
        assertEquals("local[*]", master, "The spark master should match the value in the configuration file.");
    }

    @Test
    public void testGetInvalidKey() {
        // Test behavior for an invalid key
        Exception exception = assertThrows(Exception.class, () -> {
            AppConfig.get("invalid.key");
        });

        String expectedMessage = "No configuration setting found for key 'invalid'";
        String actualMessage = exception.getMessage();
        System.out.println(actualMessage);
        assertTrue(actualMessage.contains(expectedMessage), "Exception message should indicate the missing key.");
    }
}
