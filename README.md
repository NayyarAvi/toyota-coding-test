
# Spark Batch Job Framework

This project is a Spark-based batch processing framework that performs an ETL (Extract, Transform, Load) pipeline for processing IMDB movie data. It extracts data from TSV files, performs transformations to rank movies, identifies credited persons, and outputs the results to JSON files.

## Project Structure

```
.
├── src
│   ├── main
│   │   ├── java
│   │   │   ├── com
│   │   │   │   ├── spark
│   │   │   │   │   ├── java
│   │   │   │   │   │   ├── config
│   │   │   │   │   │   │   └── AppConfig.java
│   │   │   │   │   │   ├── job
│   │   │   │   │   │   │   ├── BatchJob.java
│   │   │   │   │   │   │   └── IMDBMoviesJob.java
│   │   │   │   │   │   ├── util
│   │   │   │   │   │   │   └── FileUtils.java
│   │   │   │   │   │   ├── App.java
│   ├── test
│   │   ├── java
│   │   │   ├── com
│   │   │   │   ├── spark
│   │   │   │   │   ├── java
│   │   │   │   │   │   ├── config
│   │   │   │   │   │   │   └── AppConfigTest.java
│   │   │   │   │   │   ├── job
│   │   │   │   │   │   │   └── BatchJobTest.java
│   │   │   │   │   │   │   └── IMDBMoviesJobTest.java
│   │   │   │   │   │   ├── util
│   │   │   │   │   │   │   └── FileUtilsTest.java
├── pom.xml
├── application.conf
├── README.md
```

## Prerequisites

1. **Java**: Ensure Java JDK (version 11 or above) is installed.
   ```bash
   java --version
   ```

2. **Apache Spark**: Install Apache Spark and ensure it is added to your `PATH`.
   ```bash
   spark-submit --version
   ```

3. **Maven**: Ensure Maven is installed for building the project.
   ```bash
   mvn --version
   ```

---

## Configuration

The application configuration is managed in the `application.conf` file located in the `src/main/resources/` directory.

### **Sample Configuration**
```hocon
spark {
    app {
        name = "IMDB Batch Job"
    }
    master = "local[*]"
}

input {
    titleRatingsFilePath = "src/main/resources/title_ratings.tsv"
}

output {
    top10MoviesPath = "output/top10Movies.json"
}
```

---

## Building the Project

Use Maven to build the project and generate a JAR file with dependencies.

### **Build Command**
```bash
mvn clean package
```

The resulting JAR file will be located in the `target/` directory with the name:
```
spark-java-test-1.0-jar-with-dependencies.jar
```

---

## Running the Application

### **Using `spark-submit`**
Run the application using the `spark-submit` command. Pass the `master` value either through the `application.conf` file or directly as a command-line argument.

#### **Command Example (Local Mode)**
```bash
spark-submit \
  --class com.spark.java.App \
  --master local[*] \
  target/spark-java-test-1.0-jar-with-dependencies.jar
```

#### **Override Master Value**
You can override the `spark.master` value by passing it as a command-line argument:
```bash
spark-submit \
  --class com.spark.java.App \
  --master local[4] \
  target/spark-java-test-1.0-jar-with-dependencies.jar local[4]
```

---

## Outputs

The application generates three output files in JSON format:

1. **Top 10 Movies**:
   - Path: `output/top10Movies.json`
   - Description: Contains the top 10 ranked movies based on the ranking algorithm.

2. **Credited Persons**:
   - Path: `output/creditedPersons.json`
   - Description: Contains a list of persons credited for the top 10 movies.

3. **Different Titles**:
   - Path: `output/differentTitles.json`
   - Description: Contains alternate titles for the top 10 movies.

---

## Testing the Application

### **Run Unit Tests**
Unit tests are written using JUnit 5 and Mockito. Run them using Maven:
```bash
mvn test
```

---

## Troubleshooting

1. **Error: `zsh: no matches found: local[*]`**
   - Solution: Quote or escape the `[*]` in the `spark-submit` command:
     ```bash
     --master "local[*]"
     ```

2. **Spark Not Found**:
   - Ensure Spark’s `bin/` directory is added to your `PATH`.
   - Verify installation with:
     ```bash
     spark-submit --version
     ```

3. **Missing Configuration Key**:
   - Ensure `application.conf` is present in `src/main/resources/`.

---
