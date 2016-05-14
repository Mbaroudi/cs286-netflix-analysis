package com.netflix.driver;

import com.netflix.util.Constants;
import com.netflix.util.DataUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jainikkumar on 5/5/16.
 */
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("NetflixAnalysis").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
//        DataUtil.convertCSVToParquet(sqlContext, Constants.MOVIE_RATING_CSV_FILE_LOC, Constants.PARQUET_FILE_LOC_MOVIE_RATING);
//        DataUtil.convertCSVToParquet(sqlContext, Constants.MOVIE_RATING_CSV_FILE_LOC, Constants.PARQUET_FILE_LOC_MOVIE_RATING);
//        DataUtil.convertTxtToParquet(sqlContext, Constants.MOVIE_TITLES_TXT_FILE_LOC, Constants.PARQUET_FILE_LOC_MOVIE_TITLES);
        DataUtil.joinParequetFile(sqlContext, "/Users/jainikkumar/Work/netfix-analysis/dataset/output/");

    }
}
