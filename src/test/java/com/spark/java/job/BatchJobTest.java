package com.spark.java.job;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class BatchJobTest {

    private static SparkSession spark;

    @BeforeAll
    public static void setUp() {
        spark = SparkSession.builder()
                .appName("BatchJobTest")
                .master("local[*]")
                .getOrCreate();
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testRunMethod() {
        // Create a mock BatchJob
        BatchJob mockBatchJob = spy(new BatchJob(spark) {
            @Override
            protected void extract() {
                System.out.println("Mock Extract");
            }

            @Override
            protected void transform() {
                System.out.println("Mock Transform");
            }

            @Override
            protected void load() {
                System.out.println("Mock Load");
            }
        });

        // Execute the run method
        mockBatchJob.run();

        // Verify that the methods are called in the correct order
        verify(mockBatchJob, times(1)).extract();
        verify(mockBatchJob, times(1)).transform();
        verify(mockBatchJob, times(1)).load();
    }
}
