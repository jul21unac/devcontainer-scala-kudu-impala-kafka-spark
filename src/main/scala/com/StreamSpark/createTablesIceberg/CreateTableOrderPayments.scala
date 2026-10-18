package com.StreamSpark.createTablesIceberg

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object CreateTableOrderPayments {

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

    println(
      "✅ Creating table 'hive_prod.orders.order_payments' in Iceberg catalog"
    )

    spark.sql("""
        CREATE TABLE hive_prod.orders.order_payments (
          order_id STRING,
          payment_sequential INT,
          payment_type STRING,
          payment_installments INT,
          payment_value DOUBLE
        )
        USING iceberg
  """)

    println(
      "✅ Table 'hive_prod.orders.order_payments' created successfully in Iceberg catalog"
    )

    spark.stop()

  }

}
