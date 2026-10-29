package com.StreamSpark.Analysis

import org.apache.spark.sql.functions._

import com.StreamSpark.utils.ReadIcebergTable
import com.StreamSpark.utils.ReadKuduTable

object FirstAnalysis {

  def main(args: Array[String]): Unit = {

    val iread = new ReadIcebergTable()
    val kuduread = new ReadKuduTable()
    // Read data from Kudu
    val sellersDF = kuduread.readKuduTable("master.sellers")

    sellersDF.show(10, false)

    sellersDF.printSchema()

    // Read data from Iceberg
    val orderItem =
      iread.readIcebergTable("orders.order_item")
    orderItem.show(10, false)

    orderItem.printSchema()

    dfFinal = orderItem.join(sellerDF, "seller_id")

    dfFinal.select("order_id", "price")

    // Perform analysis or transformations as needed

  }

}
