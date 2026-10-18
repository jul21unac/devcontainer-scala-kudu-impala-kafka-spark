package com.StreamSpark.extractDataToIceberg
import com.StreamSpark.utils.SparkIceberg

object ExtractOrder extends SparkIceberg {
  val csvPath = "data/olist_orders_dataset.csv"

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession()
    val df = spark.read
      .option("header", "true")
      // .option("inferSchema", "true")
      .csv(csvPath)

    val df_final = df
      .withColumn(
        "order_purchase_timestamp",
        df("order_purchase_timestamp").cast("timestamp")
      )
      .withColumn(
        "order_approved_at",
        df("order_approved_at").cast("timestamp")
      )
      .withColumn(
        "order_delivered_carrier_date",
        df("order_delivered_carrier_date").cast("timestamp")
      )
      .withColumn(
        "order_delivered_customer_date",
        df("order_delivered_customer_date").cast("timestamp")
      )
      .withColumn(
        "order_estimated_delivery_date",
        df("order_estimated_delivery_date").cast("timestamp")
      )

    df_final.writeTo("hive_prod.orders.orders").append()

    spark.stop()

  }

}
