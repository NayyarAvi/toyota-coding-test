package com.spark.java.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileUtilsTest {
    private SparkSession spark;

    @BeforeEach
    public void setUp() {
        spark = SparkSession.builder()
                .appName("IMDBMoviesJobTest")
                .master("local[*]")
                .getOrCreate();
    }

    @Test
    public void testReadFile() {
        String filePath = "src/test/resources/mock_title_ratings.tsv";
        String delimiter = "\t";
        Dataset<Row> result = FileUtils.readFile(spark, filePath, delimiter);

        // Verify dataset is not null
        assert(result != null);

        // Verify the row count in the Dataset
        assertEquals(12, result.count(), "Title Ratings dataset should have exactly 12 records");
    }

    @Test
    public void writeReadFile() {
        String filePath = "src/test/resources/mock_title_ratings.tsv";
        String delimiter = "\t";
        Dataset<Row> result = FileUtils.readFile(spark, filePath, delimiter);
        FileUtils.writeJsonFile(result, "overwrite", "output/test");

        // Verify dataset is not null
        assert(result != null);

        assertTrue(new java.io.File("output/test").exists(), "output/test should exist");
    }
}
