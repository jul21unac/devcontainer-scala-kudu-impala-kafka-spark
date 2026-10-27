package com.StreamSpark.extractDataToKudu
import com.StreamSpark.utils.SparkKudu
import org.apache.spark.sql.functions._

object ExtractSellers extends SparkKudu {

  val csvPath = "data/olist_sellers_dataset.csv"

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val kuduContext = getKuduContext()

    val df = spark.read
      .option("header", "true")
      // .option("inferSchema", "true")
      .csv(csvPath)

    df.show(10, false)

    df.printSchema()

    df.write
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", "master.sellers")
      .mode("append")
      .save()

    spark.stop()

  }

}
