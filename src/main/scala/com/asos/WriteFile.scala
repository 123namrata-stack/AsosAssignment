package com.asos

import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteFile {

  def writeParquetFile(df: DataFrame, filePath: String) = {

    df.write.mode("append").parquet(filePath)
  }

}
