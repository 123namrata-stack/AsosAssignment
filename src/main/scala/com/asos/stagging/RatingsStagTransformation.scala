package com.asos.stagging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RatingsStagTransformation {

  def matchAndMergeRatings(ratingsDF: DataFrame, oldRatingDF: DataFrame) = {

    val ratingsPrevDF = oldRatingDF.select(col("user_id").as("user_id_prev"),
      col("movie_id").as("movie_id_prev"),
      col("ratings").as("ratings_prev"),
      col("timestamp").as("timestamp_prev")
    )

    val joinedRating = ratingsDF.join(ratingsPrevDF,
      ratingsDF.col("user_id") === ratingsPrevDF.col("user_id_prev")
        && ratingsDF.col("movie_id") === ratingsPrevDF.col("movie_id_prev"),
      "fullouter"
    )

    val aggregatedRatings = joinedRating.select(
      coalesce(col("user_id"), col("user_id_prev")).as("user_id"),
      coalesce(col("movie_id"), col("movie_id_prev")).as("movie_id"),
      when(col("timestamp") > coalesce(col("timestamp_prev"),lit(0)), col("ratings")).otherwise(col("ratings_prev")).as("rating"),
      when(col("timestamp") > coalesce(col("timestamp_prev"),lit(0)), col("timestamp")).otherwise(col("timestamp_prev")).as("timestamp")
    )

    aggregatedRatings
  }

}
