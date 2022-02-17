package com.asos.stagging

import java.io.FileNotFoundException
import com.asos.{Constants, ReadFile, WriteFile}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import scala.io.Source


object LoadRatingsData extends App {

  val spark = SparkSession.builder()
    .appName("Movie Rating")
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

  val ratingsColumns = Constants.prop.getProperty("ratings_file_columns").split(",")
  val stagingLoc = Constants.prop.getProperty("ratings_stagging_location")

  val ratingSchema = StructType(List(
    StructField("user_id", StringType),
    StructField("movie_id", StringType),
    StructField("ratings", IntegerType),
    StructField("timestamp", StringType)
  ))

  val ratingsLatest = ReadFile.readCSVFileWithSchema(spark,
    Constants.prop.getProperty("ratings_file_delimiter"), ratingSchema,
    Constants.prop.getProperty("ratings_file_location"))
    .toDF(ratingsColumns:_*)

  val ratingsPrevious = ReadFile.readParquetFileWithSchema(spark, stagingLoc, ratingSchema)

  val finalDF = RatingsStagTransformation.matchAndMergeRatings(ratingsLatest, ratingsPrevious)

  WriteFile.writeParquetFile(finalDF, stagingLoc)

}
