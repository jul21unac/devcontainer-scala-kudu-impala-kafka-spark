package com.StreamSpark.utils

class ReadIcebergTable extends SparkIceberg {

  def readIcebergTable(tableName: String) = {
    val spark = getSparkSession()

    val df = spark.read
      .format("iceberg")
      .load(tableName)

    df
  }

}
