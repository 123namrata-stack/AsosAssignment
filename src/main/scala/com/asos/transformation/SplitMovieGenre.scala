package com.asos.transformation

import java.io.FileNotFoundException
import com.asos.{Constants, ReadFile, WriteFile}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object SplitMovieGenre {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Movie Genre Split")
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

    val movieStagingLoc = Constants.prop.getProperty("movies_stagging_location")
    val outputLoc = Constants.prop.getProperty("split_movie_genre_transformation_location")

    var movies = ReadFile.readParquetFile(spark, movieStagingLoc)

    val moviesWithCategory = splitGenre(movies)

    WriteFile.writeParquetFile(moviesWithCategory, outputLoc)

  }

  def splitGenre(moviesDF: DataFrame) = {

    var movies = moviesDF

    val movieGenre = List("unknown",
      "action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary", "Drama",
      "Fantasy", "Film_Noir","Horror", "Musical", "Mystery", "Romance", "sci_fi", "Thriller", "War", "Western")

    val genrecols = movieGenre.map(x => col(x))

    for(coll <- genrecols){

      movies = movies.withColumn(coll.toString(), when(coll < 1, "NA").otherwise(coll.toString()))
    }

    val moviesWithCategory = movies.withColumn("category", concat_ws(",", genrecols:_*))
      .withColumn("category", split(col("category"), ","))
      .withColumn("category", explode(col("category")))
      .filter("category != 'NA'")
      .select("movie_id", "movie_title", "release_date", "video_release_date", "imdb_url", "category")

    moviesWithCategory

  }

}
