package com.asos

import org.apache.spark.sql.SparkSession


trait TestSparkSession {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Movie Rating Test")
    .getOrCreate()

}
