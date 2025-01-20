package com.spark.java.job;

import org.apache.spark.sql.SparkSession;

public abstract class BatchJob {
    protected final SparkSession spark;

    /**
     * Constructor for BatchJob. Initializes the SparkSession to be used for the job.
     *
     * @param spark The SparkSession that will be used to interact with Spark.
     */
    public BatchJob(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * The run method orchestrates the execution of the ETL process.
     * It sequentially calls the extract, transform, and load methods,
     * which must be implemented by the derived classes.
     */
    public void run() {
        extract();
        transform();
        load();
    }

    /**
     * The extract method is an abstract method that must be implemented by subclasses.
     * It defines the data extraction logic, such as reading data from files or databases.
     */
    protected abstract void extract();
    protected abstract void transform();
    protected abstract void load();
}
