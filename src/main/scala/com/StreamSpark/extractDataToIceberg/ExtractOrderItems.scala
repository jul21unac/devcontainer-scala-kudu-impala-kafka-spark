package com.StreamSpark.extractDataToIceberg
import org.apache.spark.sql.SparkSession
import java.io.File

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ExtractOrderItems {
  val csvPath = "data/olist_order_items_dataset.csv"

  def main(args: Array[String]): Unit = {
    val ake = "MpV3o8kVYR2d3Lp1gRPn"
    val sk = "QELgEuFQGx9siULGCrnuTvi8D2cbqJHbnBT2owbw"
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("HiveTableExample")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.access.key", ake)
      .config("spark.hadoop.fs.s3a.secret.key", sk)
      .config(
        "spark.sql.catalog.hive_prod",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config("spark.sql.catalog.hive_prod.type", "hive")
      .config("spar.sql.catalog.hive_prod.uri", "thrift://hive-metastore:9083")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      // .option("inferSchema", "true")
      .csv(csvPath)

    val df_final = df
      .withColumn("order_item_id", df("order_item_id").cast("int"))
      .withColumn("price", df("price").cast("double"))
      .withColumn("freight_value", df("freight_value").cast("double"))
      .withColumn(
        "shipping_limit_date",
        df("shipping_limit_date").cast("timestamp")
      )

    df_final.writeTo("hive_prod.orders.order_items").append()

    spark.stop()

  }
}
