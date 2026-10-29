package com.StreamSpark.utils

class ReadKuduTable extends SparkKudu {

  def readKuduTable(tableName: String) = {
    val spark = getSparkSession()

    val df = spark.read
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", tableName)
      .load()

    df
  }

}
