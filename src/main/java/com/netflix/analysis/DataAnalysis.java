package com.netflix.analysis;

import com.netflix.util.Constants;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jainikkumar on 5/6/16.
 */
public class DataAnalysis {
    public static void getTop10Movies(SQLContext sqlContext, String outputDir) {
        DataFrame movieRatings = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_RATING);
        DataFrame movieTitles = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_TITLES);
        movieRatings.registerTempTable("movie_ratings");
        movieTitles.registerTempTable("movie_titles");

        DataFrame topTen = sqlContext.sql(
                "SELECT movie_id, count(*) as num_of_ratings " +
                "FROM movie_ratings " +
                "GROUP BY movie_id ORDER BY num_of_ratings DESC LIMIT 10"
        );
        topTen.join(movieTitles, "movie_id").write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save(outputDir + "/top10movies.csv");
    }

    public static void getHighestAndLowestRatedMovies(SQLContext sqlContext, String outputDir) {
        DataFrame movieRatings = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_RATING);
        DataFrame movieTitles = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_TITLES);
        movieRatings.registerTempTable("movie_ratings");
        movieTitles.registerTempTable("movie_titles");

        DataFrame highRated = sqlContext.sql(
                "SELECT movie_id, AVG(rating) as avg_rating " +
                "FROM movie_ratings " +
                "GROUP BY movie_id " +
                "ORDER BY avg_rating DESC LIMIT 10 "
        );

        DataFrame lowRated = sqlContext.sql(
                "SELECT movie_id, AVG(rating) as avg_rating " +
                "FROM movie_ratings " +
                "GROUP BY movie_id " +
                "ORDER BY avg_rating ASC LIMIT 10 "
        );
        highRated.join(movieTitles, "movie_id").write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save(outputDir + "/highest_rated.csv");

        lowRated.join(movieTitles, "movie_id").write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save(outputDir + "/lowest_rated.csv");
    }


    public static void getRatingDistribution(SQLContext sqlContext, String outputDir) {
        DataFrame movieRatings = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_RATING);
        movieRatings.registerTempTable("movie_ratings");

        DataFrame ratingDistribution = sqlContext.sql(
                "SELECT DISTINCT rating, count(*) as num_of_ratings "+
                "FROM movie_ratings "+
                "GROUP BY rating "
        );
        ratingDistribution.coalesce(1).write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save(outputDir + "/rating_distribution.csv");
    }

    public static void getTopUsers(SQLContext sqlContext, String outputDir) {
        DataFrame movieRatings = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_RATING);
        movieRatings.registerTempTable("movie_ratings");

        DataFrame ratingDistribution = sqlContext.sql(
                "SELECT user_id, count(*) as num_of_ratings "+
                "FROM movie_ratings "+
                "GROUP BY user_id "+
                "ORDER BY num_of_ratings DESC LIMIT 100"
        );
        ratingDistribution.write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save(outputDir + "/top_100_users.csv");
    }

    public static void getYearWiseRatings(SQLContext sqlContext, String outputDir) {
        DataFrame movieRatings = sqlContext.read().parquet(Constants.PARQUET_FILE_COMBINED_DATA);
        movieRatings.registerTempTable("movie_ratings");
        DataFrame ratingDistribution = sqlContext.sql(
                "SELECT year_released, count(*) as num_of_ratings "+
                "FROM movie_ratings mr "+
                "INNER JOIN movie_titles mt ON (mr.movie_id = mt.movie_id) "+
                "GROUP BY year_released "+
                "ORDER BY num_of_ratings DESC"
        );
        ratingDistribution.coalesce(1).write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save(outputDir + "/year_wise_rating.csv");
    }

    public static void getYearWiseActiveUsers(SQLContext sqlContext, String outputDir) {
        DataFrame movieRatings = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_RATING);
        DataFrame movieTitles = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_TITLES);
        movieRatings.registerTempTable("movie_ratings");
        movieTitles.registerTempTable("movie_titles");

        DataFrame ratingDistribution = sqlContext.sql(
                "SELECT year_released, count(DISTINCT mr.user_id) as num_of_users "+
                "FROM movie_ratings mr "+
                "INNER JOIN movie_titles mt ON (mr.movie_id = mt.movie_id) "+
                "GROUP BY year_released "+
                "ORDER BY num_of_users DESC"
        );
        ratingDistribution.coalesce(1).write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save(outputDir + "/year_wise_active_users.csv");
    }

    public static void getLeastPopularMovies(SQLContext sqlContext, String outputDir) {
        DataFrame movieRatings = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_RATING);
        DataFrame movieTitles = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_TITLES);
        movieRatings.registerTempTable("movie_ratings");
        movieTitles.registerTempTable("movie_titles");

        DataFrame topTen = sqlContext.sql(
                "SELECT movie_id, count(*) as num_of_ratings "+
                "FROM movie_ratings "+
                "GROUP BY movie_id "+
                "ORDER BY num_of_ratings ASC LIMIT 10"
        );
        topTen.join(movieTitles, "movie_id").write().format("com.databricks.spark.csv")
                .option("header", "true")
                .save(outputDir + "/least_popular_movies.csv");
    }
}
