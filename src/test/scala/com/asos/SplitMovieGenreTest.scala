package com.asos

import com.asos.stagging.RatingsStagTransformation
import com.asos.transformation.SplitMovieGenre
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec

class SplitMovieGenreTest extends FunSpec
  with TestSparkSession {

  val moviecols = "movie_id,movie_title,release_date,video_release_date,imdb_url,unknown,action,Adventure,Animation,Children,Comedy,Crime,Documentary,Drama,Fantasy,Film_Noir,Horror,Musical,Mystery,Romance,sci_fi,Thriller,War,Western"
    .split(",")

  val movies = ReadFile.readCSVFile(spark, "|", "src\\test\\resources\\movies.txt").toDF(moviecols:_*)

  val splittedgenreDF = SplitMovieGenre.splitGenre(movies)

  describe("Test function splitGenre function..."){

    it("Category od movie_id = 1...") {

      assertResult(List("Animation", "Children", "Comedy")) {
        splittedgenreDF.filter(col("movie_id") === "1").groupBy("movie_id").agg(collect_list("category").as("cat_list"))
          .select("cat_list").collect()(0)(0)
      }
    }


  }

}
