package com.asos

import com.asos.stagging.RatingsStagTransformation
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec
import org.apache.spark.sql.functions._

class RatingsStagTransformationTest extends FunSpec
  with TestSparkSession {

  val newRatingsData = Seq(
    Row("u1", "m1", 4, "1434"),
    Row("u2", "m2", 1, "1437"),
    Row("u3", "m3", 2, "1435")
  )

  val oldRatingsData = Seq(
    Row("u1", "m1", 5, "1420"),
    Row("u5", "m5", 1, "1420"),
    Row("u3", "m3", 1, "1420"),
    Row("u3", "m1", 5, "1420")
  )

  val ratingSchema = StructType(List(
    StructField("user_id", StringType),
    StructField("movie_id", StringType),
    StructField("ratings", IntegerType),
    StructField("timestamp", StringType)
  ))

  val newRatingsDF = spark.createDataFrame(spark.sparkContext.parallelize(newRatingsData), ratingSchema)
  val oldRatingsDF = spark.createDataFrame(spark.sparkContext.parallelize(oldRatingsData), ratingSchema)

  val outputDF = RatingsStagTransformation.matchAndMergeRatings(newRatingsDF, oldRatingsDF)

  describe("Test function matchAndMergeRatings..."){

    it("Record which was in previous day but not in current day...") {

      assertResult(Row("u3", "m1", 5, "1420")) {
        outputDF.filter(col("user_id") === "u3" && col("movie_id") === "m1").collect()(0)
      }
    }

    it("Record which was in previous day and  in current day...") {

      assertResult(Row("u1", "m1", 4, "1434")) {
        outputDF.filter(col("user_id") === "u1" && col("movie_id") === "m1").collect()(0)
      }

    }

    it("Record which is not in previous day but  in current day...") {

      assertResult(Row("u2", "m2", 1, "1437")) {
        outputDF.filter(col("user_id") === "u2" && col("movie_id") === "m2").collect()(0)
      }

    }

  }

}
