package com.asos.transformation

import java.io.FileNotFoundException
import com.asos.{Constants, ReadFile, WriteFile}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object Top10Movies {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Top 10 movies")
      .master("local")
      .getOrCreate()

    val url = getClass.getResource("/application.properties")

    if (url != null) {
      val source = Source.fromURL(url)
      Constants.prop.load(source.bufferedReader())
    }
    else{
      throw new FileNotFoundException("Properties file cannot be loaded")
    }

    val ratingsStagingLoc = Constants.prop.getProperty("ratings_stagging_location")
    val outputLoc = Constants.prop.getProperty("top10_movies_transformation_location")

    val ratingDF = ReadFile.readParquetFile(spark, ratingsStagingLoc)

    val outputDF = getTop10Movies(ratingDF, 10)

    WriteFile.writeParquetFile(outputDF.coalesce(1), outputLoc)

  }

  def getTop10Movies(ratingDF: DataFrame, topN: Int) = {

    val aggRatingDF = ratingDF.groupBy("movie_id")
      .agg(avg(col("ratings")).as("avg_rating"),
        count("movie_id").as("rating_count")
      )

    val filteredAggRatingDF = aggRatingDF
      .filter(col("rating_count") >= 5)
      .orderBy(col("avg_rating").desc).limit(topN)

    filteredAggRatingDF

  }

}
