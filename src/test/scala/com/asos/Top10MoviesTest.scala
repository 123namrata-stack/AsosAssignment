package com.asos

import com.asos.stagging.RatingsStagTransformation
import com.asos.transformation.Top10Movies
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec

class Top10MoviesTest extends FunSpec
  with TestSparkSession {

  val ratingsData = Seq(
    Row("u1", "m1", 4, "1434"),
    Row("u1", "m1", 3, "1434"),
    Row("u1", "m1", 4, "1434"),
    Row("u1", "m1", 3, "1434"),
    Row("u1", "m1", 1, "1434"),
    Row("u2", "m2", 5, "1437"),
    Row("u2", "m2", 5, "1437"),
    Row("u1", "m3", 2, "1435"),
    Row("u2", "m3", 2, "1435"),
    Row("u3", "m3", 2, "1435"),
    Row("u4", "m3", 2, "1435"),
    Row("u5", "m3", 2, "1435"),
    Row("u1", "m4", 4, "1435"),
    Row("u2", "m4", 4, "1435"),
    Row("u3", "m4", 4, "1435"),
    Row("u4", "m4", 4, "1435"),
    Row("u5", "m4", 4, "1435")
  )

  val ratingSchema = StructType(List(
    StructField("user_id", StringType),
    StructField("movie_id", StringType),
    StructField("ratings", IntegerType),
    StructField("timestamp", StringType)
  ))

  val ratingsDF = spark.createDataFrame(spark.sparkContext.parallelize(ratingsData), ratingSchema)

  val outputDF = Top10Movies.getTop10Movies(ratingsDF, 2)

  describe("Test function Top10Movies function..."){

    it("check top 2 movies...") {

      assertResult(List(Row("m4"), Row("m1"))) {
        outputDF.select("movie_id").collect()
      }
    }

    it("check less than 5 ratings is not there...") {

      assertResult(0) {
        outputDF.filter(col("movie_id") === "m2").count()
      }
    }

  }

}
