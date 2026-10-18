package com.StreamSpark.utils
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.types._
import java.net.InetAddress
import java.util

trait SparkKudu {
  val KUDU_MASTERS = "kudu-master-1:7051,kudu-master-2:7051,kudu-master-3:7051"

  def getSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("CreateCustomerKuduTable")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.kudu.operation.timeout.ms", "60000")
      .config("spark.kudu.admin.operation.timeout.ms", "60000")
      .enableHiveSupport()
      .getOrCreate()
    spark

  }
  def getKuduContext(): KuduContext = {
    val spark = getSparkSession()

    new KuduContext(KUDU_MASTERS, spark.sparkContext)
  }

}
