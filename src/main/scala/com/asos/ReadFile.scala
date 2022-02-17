package com.asos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object ReadFile {

  def readCSVFileWithSchema(spark: SparkSession, delimiter: String, schema: StructType, filePath: String) = {

    spark.read
      .option("delimiter", delimiter)
      .schema(schema)
      .csv(filePath)
  }

  def readCSVFile(spark: SparkSession, delimiter: String, filePath: String) = {

    spark.read
      .option("delimiter", delimiter)
      .csv(filePath)
  }

  def readParquetFileWithSchema(spark: SparkSession, filePath: String, schema: StructType) = {

    spark.read.schema(schema).parquet(filePath)
  }

  def readParquetFile(spark: SparkSession, filePath: String) = {

    spark.read.parquet(filePath)
  }

}
