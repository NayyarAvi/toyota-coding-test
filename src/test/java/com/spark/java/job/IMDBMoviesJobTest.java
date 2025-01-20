package com.spark.java.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class IMDBMoviesJobTest {
    private static SparkSession spark;
    private static IMDBMoviesJob job;
    private static Dataset<Row> mockTitleRatingsDf;
    private static Dataset<Row> mockTitleBasicsDf;
    private static Dataset<Row> mockNameBasicsDf;
    private static Dataset<Row> mockDiffTitlesDf;

    @BeforeAll
    public static void setUp() {
        spark = SparkSession.builder()
                .appName("IMDBMoviesJobTest")
                .master("local[*]")
                .getOrCreate();

        job = new IMDBMoviesJob(spark);

        // Creating mock datasets
        mockTitleRatingsDf = spark.read()
                .option("header", "true")
                .option("sep", "\t")
                .csv("src/test/resources/mock_title_ratings.tsv");

        mockTitleBasicsDf = spark.read()
                .option("header", "true")
                .option("sep", "\t")
                .csv("src/test/resources/mock_title_basics.tsv");

        mockNameBasicsDf = spark.read()
                .option("header", "true")
                .option("sep", "\t")
                .csv("src/test/resources/mock_name_basics.tsv");

        mockDiffTitlesDf = spark.read()
                .option("header", "true")
                .option("sep", "\t")
                .csv("src/test/resources/mock_title_akas.tsv");
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testExtractWithMockData() {
        // Inject mock data
        job.setMockData(mockTitleRatingsDf, mockTitleBasicsDf, mockNameBasicsDf, mockDiffTitlesDf);

        // Run transform phase
        job.extract();

        // Validate Title ratings dataset
        Dataset<Row> titleRatingsDf = job.getTitleRatingsDf();
        assertNotNull(titleRatingsDf);
        assertEquals(12, titleRatingsDf.count(), "Title Ratings dataset should have exactly 12 records");

        // Validate Title Basics dataset
        Dataset<Row> titleBasicsDf = job.getTitleBasicsDf();
        assertNotNull(titleBasicsDf);
        assertEquals(12, titleBasicsDf.count(), "Title Basics dataset should have exactly 12 records");

        // Validate Name Basics dataset
        Dataset<Row> nameBasicsDf = job.getNameBasicsDf();
        assertNotNull(nameBasicsDf);
        assertEquals(10, nameBasicsDf.count(), "Name Basics dataset should have exactly 10 records");

        // Validate Different Titles dataset
        Dataset<Row> diffTitlesDf = job.getDiffTitlesDf();
        assertNotNull(diffTitlesDf);
        assertEquals(139, diffTitlesDf.count(), "Different Titles dataset should have exactly 10 records");
    }

    @Test
    public void testTransformWithMockData() {
        // Inject mock data
        job.setMockData(mockTitleRatingsDf, mockTitleBasicsDf, mockNameBasicsDf, mockDiffTitlesDf);

        // Run transform phase
        job.extract();
        job.transform();

        // Validate top 10 movies dataset
        Dataset<Row> top10MoviesDf = job.getTop10MoviesDf();
        assertNotNull(top10MoviesDf);
        assertEquals(10, top10MoviesDf.count(), "Top 10 movies dataset should have exactly 10 records");
    }

    @Test
    public void testLoadWithMockData() {
        // Inject mock data
        job.setMockData(mockTitleRatingsDf, mockTitleBasicsDf, mockNameBasicsDf, mockDiffTitlesDf);

        // Run full job
        job.run();

        // Validate output paths
        assertTrue(new java.io.File("output/top10MoviesTitle").exists(), "Top 10 movies output should exist");
        assertTrue(new java.io.File("output/creditedPersons").exists(), "Credited persons output should exist");
        assertTrue(new java.io.File("output/differentTitles").exists(), "Different titles output should exist");
    }
}
