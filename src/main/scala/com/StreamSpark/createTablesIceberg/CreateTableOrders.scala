package com.StreamSpark.createTablesIceberg

import com.StreamSpark.utils.SparkIceberg

object CreateTableOrders extends SparkIceberg {

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession()

    println("✅ Creating table 'hive_prod.orders.orders' in Iceberg catalog")

    spark.sql("""
        CREATE TABLE hive_prod.orders.orders (
          order_id STRING,
          customer_id STRING,
          order_status STRING,
          order_purchase_timestamp Timestamp,
          order_approved_at Timestamp,
          order_delivered_carrier_date Timestamp,
          order_delivered_customer_date Timestamp,
          order_estimated_delivery_date Timestamp
        )
        USING iceberg
  """)

    println(
      "✅ Table 'hive_prod.orders.orders' created successfully in Iceberg catalog"
    )

    spark.stop()

  }

}
