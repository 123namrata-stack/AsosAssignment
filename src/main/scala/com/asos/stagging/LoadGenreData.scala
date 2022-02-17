package com.asos.stagging

import java.io.FileNotFoundException
import com.asos.{Constants, ReadFile, WriteFile}
import org.apache.spark.sql.SparkSession
import scala.io.Source


object LoadGenreData extends App {

  val spark = SparkSession.builder()
    .appName("Load Movie Genre Data")
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

  val genreColumns = Constants.prop.getProperty("genre_file_columns").split(",")
  val stagingLoc = Constants.prop.getProperty("genre_stagging_location")

  val genreDF = ReadFile.readCSVFile(spark,
    Constants.prop.getProperty("genre_file_delimiter"),
    Constants.prop.getProperty("genre_file_location"))
    .toDF(genreColumns:_*)

  WriteFile.writeParquetFile(genreDF, stagingLoc)

}
