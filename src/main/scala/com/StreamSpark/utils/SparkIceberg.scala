package com.StreamSpark.utils

import org.apache.spark.sql.SparkSession
import java.io.File

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

trait SparkIceberg {

  def getSparkSession(): SparkSession = {
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
    spark

  }
}
