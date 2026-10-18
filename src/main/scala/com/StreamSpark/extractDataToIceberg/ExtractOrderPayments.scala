package com.StreamSpark.extractDataToIceberg

import org.apache.spark.sql.SparkSession
import java.io.File

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.StreamSpark.utils.SparkIceberg

object ExtractOrderPayments extends SparkIceberg {
  val csvPath = "data/olist_order_payments_dataset.csv"

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession()
    val df = spark.read
      .option("header", "true")
      // .option("inferSchema", "true")
      .csv(csvPath)

    val df_final = df
      .withColumn("payment_sequential", df("payment_sequential").cast("int"))
      .withColumn(
        "payment_installments",
        df("payment_installments").cast("int")
      )
      .withColumn("payment_value", df("payment_value").cast("double"))

    df_final.writeTo("hive_prod.orders.order_payments").append()

    spark.stop()

  }

}
