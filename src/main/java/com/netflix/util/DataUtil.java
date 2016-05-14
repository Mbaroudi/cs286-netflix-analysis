package com.netflix.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jainikkumar on 5/5/16.
 */
public class DataUtil {
    public static void convertCSVToParquet(SQLContext sqlContext, String fileName, String parquetFileLoc) {
        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema","true")
                .option("nullValue","")
                .option("treatEmptyValuesAsNulls","true")
                .load(fileName);
        System.out.println("Converting Movie Ratings to Parquet");
        System.out.println(df.count());
        df.write().parquet(parquetFileLoc);
    }

    public static void convertTxtToParquet(SQLContext sqlContext, String filename, String parquet_file_loc) {
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("movie_id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("year_released", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType movieTitleSchema = DataTypes.createStructType(fields);
        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .schema(movieTitleSchema)
                .option("delimiter","|")
                .option("nullValue", "")
                .option("treatEmptyValuesAsNulls","true")
                .load(filename);
        // now simply write to a parquet file
        System.out.println("Converting Movie Titles to Parquet");
        System.out.println(df.count());
        df.write().parquet(parquet_file_loc);
    }


    public static void joinParequetFile(SQLContext sqlContext, String outputDir) {
        DataFrame movieRatings = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_RATING);
        DataFrame movieTitles = sqlContext.read().parquet(Constants.PARQUET_FILE_LOC_MOVIE_TITLES);
        movieRatings.registerTempTable("movie_ratings");
        movieTitles.registerTempTable("movie_titles");
//        movieRatings.join(movieTitles, "movie_id").write().parquet(outputDir + "combined_data_parquet");
        movieRatings.join(movieTitles, "movie_id").coalesce(1).write()
                .format("com.databricks.spark.csv")
                .option("delimiter","|")
                .option("header", "true")
                .save(outputDir + "combined_data.csv");
    }
}
