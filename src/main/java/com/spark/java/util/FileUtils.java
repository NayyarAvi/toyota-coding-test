package com.spark.java.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FileUtils {
    /**
     * The readFile method reads a CSV file into a Spark Dataframe.
     *
     * @param spark The SparkSession to interact with Spark.
     * @param filePath The path of the CSV file to read.
     * @param delimiter The delimiter used in the CSV file (e.g., comma, tab).
     * @return A Dataset<Row> representing the contents of the CSV file.
     */
    public static Dataset<Row> readFile(SparkSession spark, String filePath, String delimiter) {
        return spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("sep", delimiter)
                .csv(filePath);
    }

    /**
     * The writeJsonFile method writes a Spark Dataset to a JSON file.
     *
     * @param df The Dataset<Row> to be written to a file.
     * @param mode The write mode (e.g., "overwrite" or "append") for the output file.
     * @param filePath The path where the JSON file should be saved.
     */
    public static void writeJsonFile(Dataset<Row> df, String mode, String filePath) {
        df.write().mode(mode).json(filePath);
    }
}
