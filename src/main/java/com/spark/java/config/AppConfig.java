package com.spark.java.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AppConfig {
    private static final Config config = ConfigFactory.load();

    public static String get(String key) {
        return config.getString(key);
    }
}