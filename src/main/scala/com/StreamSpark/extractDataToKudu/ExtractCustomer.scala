package com.StreamSpark.extractDataToKudu
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import java.io.File

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ExtractCustomer {

  val KUDU_MASTERS = "kudu-master-1:7051,kudu-master-2:7051,kudu-master-3:7051"
  val csvPath = "data/olist_customers_dataset.csv"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("LoadCSVToKudu")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.kudu.operation.timeout.ms", "60000")
      .config("spark.kudu.admin.operation.timeout.ms", "60000")
      .enableHiveSupport()
      .getOrCreate()

    val kuduContext = new KuduContext(KUDU_MASTERS, spark.sparkContext)

    val df = spark.read
      .option("header", "true")
      // .option("inferSchema", "true")
      .csv(csvPath)

    df.write
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", "master.customers")
      .mode("append")
      .save()

    spark.stop()

  }

}
