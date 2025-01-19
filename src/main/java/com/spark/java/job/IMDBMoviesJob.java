package com.spark.java.job;

import com.spark.java.config.AppConfig;
import com.spark.java.util.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * IMDBMoviesJob is responsible for extracting, transforming, and loading IMDB movie data.
 * The job processes datasets related to movie ratings, titles, and credited persons,
 * and outputs insights like top 10 movies, credited persons, and different title names.
 */
public class IMDBMoviesJob extends BatchJob {
    private Dataset<Row> titleRatingsDf;
    private Dataset<Row> titleBasicsDf;
    private Dataset<Row> nameBasicsDf;
    private Dataset<Row> diffTitlesDf;
    private Dataset<Row> top10MoviesDf;

    private final String titleRatingsPath;
    private final String titleBasicsPath;
    private final String nameBasicsPath;
    private final String diffTitlesPath;

    /**
     * Constructor for initializing IMDBMoviesJob with a SparkSession.
     * It reads file paths for datasets from the AppConfig.
     *
     * @param spark The SparkSession to be used in the job.
     */
    public IMDBMoviesJob(SparkSession spark) {
        super(spark);
        titleRatingsPath = AppConfig.get("input.titleRatingsFilePath");
        titleBasicsPath = AppConfig.get("input.titleBasicsFilePath");
        nameBasicsPath = AppConfig.get("input.nameBasicsFilePath");
        diffTitlesPath = AppConfig.get("input.diffTitlesFilePath");
    }

    /**
     * The extract method reads the datasets from their respective file paths.
     */
    @Override
    protected void extract() {
        if (titleRatingsDf == null || titleBasicsDf == null || nameBasicsDf == null || diffTitlesDf == null) {
            titleRatingsDf = FileUtils.readFile(spark, titleRatingsPath, "\t");
            titleBasicsDf = FileUtils.readFile(spark, titleBasicsPath, "\t");
            nameBasicsDf = FileUtils.readFile(spark, nameBasicsPath, "\t");
            diffTitlesDf = FileUtils.readFile(spark, diffTitlesPath, "\t");
        }
    }

    /**
     * The transform method processes and transforms the data:
     * - Filters movies with numVotes >= 500.
     * - Calculates average number of votes.
     * - Adds a "ranking" column based on a formula using numVotes and averageRating.
     * - Retrieves the top 10 movies based on the ranking.
     */
    @Override
    protected void transform() {
        // Filtering movies with numVotes >= 500
        titleRatingsDf = titleRatingsDf.filter("numVotes > 499");

        // Calculating average numVotes
        double avgNumVotes = titleRatingsDf.agg(functions.avg("numVotes")).first().getDouble(0);

        // Adding a ranking column
        titleRatingsDf = titleRatingsDf.withColumn(
                "ranking",
                functions.col("numVotes").divide(avgNumVotes).multiply(functions.col("averageRating"))
        ).orderBy(functions.col("ranking").desc());

        // Retrieving top 10 movies id
        Dataset<Row> top10MoviesIdDf = titleRatingsDf
                .select(functions.col("tconst").alias("movieId"))
                .limit(10);

        // Retrieving top 10 moviesId and their corresponding title
        top10MoviesDf = top10MoviesIdDf.join(
                titleBasicsDf,
                top10MoviesIdDf.col("movieId").equalTo(titleBasicsDf.col("tconst")),
                "inner"
        ).select(functions.col("movieId"), functions.col("primaryTitle"));
    }

    /**
     * The load method orchestrates the loading process:
     * - It calls methods for top 10 movies, credited persons, and different titles.
     */
    @Override
    protected void load() {
        top10Movies();
        creditedPersons();
        differentTitles();
    }

    /**
     * Use Case 1: Retrieve the top 10 movies
     *
     * The top10Movies method outputs the top 10 movies to a JSON file and displays them.
     */
    private void top10Movies() {
        Dataset<Row> top10MoviesTitleDf = top10MoviesDf.select(functions.col("primaryTitle").alias("top10MoviesTitle"));
        FileUtils.writeJsonFile(top10MoviesTitleDf, "overwrite", AppConfig.get("output.top10MoviesTitlePath"));
        top10MoviesTitleDf.show(10);
    }

    /**
     * Use Case 2: list the persons who are most often credited for top 10 movies
     *
     * The creditedPersons method identifies and outputs the credited persons for the top 10 movies.
     * The resulting dataset of credited persons is written to a JSON file.
     */
    private void creditedPersons() {
        /**
         * Splitting the 'knownForTitles' column into an array to handle multiple movie titles listed in a single cell,
         * and then exploding the array to create individual rows for each title.
        */
        nameBasicsDf = nameBasicsDf
                .withColumn("knownForTitlesArray", functions.split(functions.col("knownForTitles"), ","))
                .withColumn("title", functions.explode(functions.col("knownForTitlesArray")));

        Dataset<Row> creditedPersonsDf = top10MoviesDf.join(
                nameBasicsDf,
                top10MoviesDf.col("movieId").equalTo(nameBasicsDf.col("title")),
                "inner"
        ).select(functions.col("primaryName").alias("topPersonCredited")).distinct();

        FileUtils.writeJsonFile(creditedPersonsDf, "overwrite", AppConfig.get("output.creditedPersonsPath"));
        creditedPersonsDf.show(10);
        System.out.println("creditedPersons Count: " + creditedPersonsDf.count());
    }

    /**
     * Use Case 3: list the different titles of the top 10 movies
     *
     * The differentTitles method identifies and outputs different titles for the top 10 movies.
     */
    private void differentTitles() {
        Dataset<Row> differentTitlesDf = top10MoviesDf.join(
                diffTitlesDf,
                top10MoviesDf.col("movieId").equalTo(diffTitlesDf.col("titleId")),
                "inner"
        ).select(
                functions.col("primaryTitle"),
                functions.col("title").alias("differentTitleNames")
        ).distinct();

        FileUtils.writeJsonFile(differentTitlesDf, "overwrite", AppConfig.get("output.differentTitlesPath"));
        differentTitlesDf.show(50);
        System.out.println("differentTitles Count: " + differentTitlesDf.count());
    }

    /**
     * The setMockData method allows for injecting mock data into the job for testing purposes.
     */
    public void setMockData(Dataset<Row> titleRatingsDf,
                            Dataset<Row> titleBasicsDf,
                            Dataset<Row> nameBasicsDf,
                            Dataset<Row> diffTitlesDf) {
        this.titleRatingsDf = titleRatingsDf;
        this.titleBasicsDf = titleBasicsDf;
        this.nameBasicsDf = nameBasicsDf;
        this.diffTitlesDf = diffTitlesDf;
    }

    /**
     * Getter methods for testing
     */
    public Dataset<Row> getTop10MoviesDf() {
        return top10MoviesDf;
    }

    public Dataset<Row> getTitleRatingsDf() {
        return titleRatingsDf;
    }

    public Dataset<Row> getTitleBasicsDf() {
        return titleBasicsDf;
    }

    public Dataset<Row> getNameBasicsDf() {
        return nameBasicsDf;
    }

    public Dataset<Row> getDiffTitlesDf() {
        return diffTitlesDf;
    }
}