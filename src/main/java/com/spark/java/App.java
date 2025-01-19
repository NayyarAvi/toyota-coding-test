package com.spark.java;

import com.spark.java.config.AppConfig;
import com.spark.java.job.IMDBMoviesJob;
import org.apache.spark.sql.SparkSession;

/**
 * The App class is the entry point of the Spark application.
 * It initializes the SparkSession, creates an instance of IMDBMoviesJob, and executes the job.
 * The application handles the setup, execution, and teardown of the Spark session.
 */
public abstract class App {
    /**
     * The main method is the entry point for running the Spark job.
     *
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        SparkSession spark = null;
        try {
            // fetching the master value
            String master = (args.length > 0) ? args[0] : AppConfig.get("spark.master");

            // Initializing the SparkSession
            spark = SparkSession.builder()
                    .appName(AppConfig.get("spark.app.name"))
                    .master(master)
                    .getOrCreate();
            System.out.println("Spark Session initialized successfully.");
            IMDBMoviesJob job = new IMDBMoviesJob(spark);

            // Running the job to execute the ETL process.
            job.run();
        } catch (IllegalArgumentException e) {
            System.err.println("Input Validation Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (spark != null) {
                // stopping the spark session to release resources
                spark.stop();
                System.out.println("Spark Session stopped.");
            }
        }
    }
}