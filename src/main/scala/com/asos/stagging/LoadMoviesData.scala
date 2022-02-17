package com.asos.stagging

import java.io.FileNotFoundException
import com.asos.{Constants, ReadFile, WriteFile}
import org.apache.spark.sql.SparkSession
import scala.io.Source


object LoadMoviesData extends App {

  val spark = SparkSession.builder()
    .appName("Load Movies data to stagging")
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

  val moviesColumns = Constants.prop.getProperty("movies_file_columns").split(",")
  val stagingLoc = Constants.prop.getProperty("movies_stagging_location")

  val moviesDF = ReadFile.readCSVFile(spark,
    Constants.prop.getProperty("movies_file_delimiter"),
    Constants.prop.getProperty("movies_file_location"))
    .toDF(moviesColumns:_*)

  WriteFile.writeParquetFile(moviesDF, stagingLoc)

}
