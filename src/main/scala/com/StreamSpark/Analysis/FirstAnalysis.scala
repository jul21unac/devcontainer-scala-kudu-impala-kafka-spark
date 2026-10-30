package com.StreamSpark.Analysis

import org.apache.spark.sql.functions._

import com.StreamSpark.utils.ReadIcebergTable
import com.StreamSpark.utils.ReadKuduTable

object FirstAnalysis {

  val iread = new ReadIcebergTable()
  val kuduread = new ReadKuduTable()

  def main(args: Array[String]): Unit = {

    customerAnalysis()

  }

  def sellerAnalysis(): Unit = {

    // Read data from Kudu
    val sellersDF = kuduread.readKuduTable("master.sellers")

    sellersDF.show(10, false)

    sellersDF.printSchema()

    // Read data from Iceberg
    val orderItem =
      iread.readIcebergTable("hive_prod.orders.order_items")
    orderItem.show(10, false)

    orderItem.printSchema()

    val dfFinal = orderItem.join(sellersDF, "seller_id")

    dfFinal.select("order_id", "price").show(5, false)

    val dfGroup =
      dfFinal
        .groupBy("seller_state", "seller_city")
        .agg(sum("price").as("total_price"))

    dfGroup.orderBy(desc("total_price")).show(10)
    // Perform analysis or transformations as needed

  }

  def geolocationAnalysis(): Unit = {

    val geoDF = kuduread.readKuduTable("master.geolocation")

    geoDF.count()

    val finalDF = geoDF
      .groupBy("geolocation_lat", "geolocation_lng")
      .agg(count("*").as("Count"))

    finalDF.filter("Count > 1").orderBy(desc("Count")).show(10)

  }

  def customerAnalysis(): Unit = {

    val customerDf = kuduread.readKuduTable("master.customers")

    val total = customerDf.count()

    println(s"Total :$total")

    val dfGroup =
      customerDf.groupBy("customer_unique_id").agg(count("*").as("Count"))

    dfGroup.filter("Count > 1").show(5)

  }

}
