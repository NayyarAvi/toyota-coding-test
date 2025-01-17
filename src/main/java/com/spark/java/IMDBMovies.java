package com.spark.java;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class IMDBMovies {
    private static Dataset<Row> top10MoviesDf = null;
    private final String titleRatingsFilePath = "/Users/nayyar/Downloads/title_ratings.tsv";
    private static final String nameBasicsFilePath = "/Users/nayyar/Downloads/name_basics.tsv";
    private static final String titleBasicsFilePath = "/Users/nayyar/Downloads/title_basics.tsv";
    private static final String diffTitlesFilePath = "/Users/nayyar/Downloads/title_akas.tsv";

    private void printTop10Movies(SparkSession spark) {
        Dataset<Row> titleRatingsDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("sep", "\t")
                .csv(titleRatingsFilePath);
        Dataset<Row> titleBasicsDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("sep", "\t")
                .csv(titleBasicsFilePath);

        // Filtering movies with numVotes >= 500
        titleRatingsDf = titleRatingsDf.filter("numVotes > 499");

        // Calculating average numVotes
        double avgNumVotes = titleRatingsDf.agg(functions.avg("numVotes")).first().getDouble(0);

        // Adding a ranking column
        titleRatingsDf = titleRatingsDf.withColumn(
                "ranking",
                functions.col("numVotes").divide(avgNumVotes).multiply(functions.col("averageRating"))
        ).orderBy(functions.col("ranking").desc());

        // Retrieving the top 10 movies id
        Dataset<Row> top10MoviesIdDf = titleRatingsDf
                .select(functions.col("tconst").alias("movieId"))
                .limit(10);

        // Joining top 10 movies ids with title_basics to find movies title
        Dataset<Row> filteredTitleBasicsDf = top10MoviesIdDf.join(
                titleBasicsDf,
                top10MoviesIdDf.col("movieId").equalTo(titleBasicsDf.col("tconst")),
                "inner"
        );

        top10MoviesDf = filteredTitleBasicsDf.select("movieId", "primaryTitle");
        top10MoviesDf.show();

        // Selecting top 10 movies title
        Dataset<Row> top10MoviesTitleDf = top10MoviesDf
                .select(functions.col("primaryTitle")
                        .alias("top10MoviesTitle"));
        top10MoviesTitleDf.show(10);
    }

    private void printPersonCredited(SparkSession spark) {
        Dataset<Row> nameBasicsDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("sep", "\t")
                .csv(nameBasicsFilePath);

        // Split knownForTitles into an array and explode it into individual rows
        nameBasicsDf = nameBasicsDf
                .withColumn("knownForTitlesArray", functions.split(functions.col("knownForTitles"), ","))
                .withColumn("title", functions.explode(functions.col("knownForTitlesArray")));

        // Join top 10 movies with name_basics to find credited persons
        Dataset<Row> filteredNameBasicsDf = top10MoviesDf.join(
                nameBasicsDf,
                top10MoviesDf.col("movieId").equalTo(nameBasicsDf.col("title")),
                "inner"
        );

        // Select distinct primary names
        Dataset<Row> personsCreditedDf = filteredNameBasicsDf
                .select(functions.col("primaryName").alias("topPersonCredited"))
                .distinct();
        personsCreditedDf.show(10);
        System.out.println("Count: " + personsCreditedDf.count());
    }

    private void printDifferentTitles(SparkSession spark) {
        Dataset<Row> diffTitlesDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("sep", "\t")
                .csv(diffTitlesFilePath);

        // Join top 10 movies id with title_aks to find different titles of the top 10 movies
        Dataset<Row> filteredDiffTitlesDf = top10MoviesDf.join(
                diffTitlesDf,
                top10MoviesDf.col("movieId").equalTo(diffTitlesDf.col("titleId")),
                "inner"
        );

        // Select different titles of the top 10 movies
        Dataset<Row> top10MoviesDiffTitlesDf = filteredDiffTitlesDf.select(
                functions.col("primaryTitle"),
                functions.col("title").alias("differentTitleNames")
        ).distinct();
        top10MoviesDiffTitlesDf.show(50);
        System.out.println("Count: " + top10MoviesDiffTitlesDf.count());
    }

    public static void main(String[] args) {
        SparkSession spark = null;
        try {
            // Initialize Spark session
            spark = SparkSession.builder()
                    .appName("IMDB Movies Rating")
                    .master("local[*]")
                    .getOrCreate();
            System.out.println("Spark Session initialized successfully.");

            IMDBMovies obj = new IMDBMovies();

            // Part 1: Retrieve the top 10 movies based on ranking
            obj.printTop10Movies(spark);

            // Part 2: list the persons who are most often credited
            obj.printPersonCredited(spark);

            // Part 3: list the different titles of the top 10 movies
            obj.printDifferentTitles(spark);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            // Stop the Spark session
            spark.stop();
            System.out.println("Spark Session stopped.");
        }
    }
}