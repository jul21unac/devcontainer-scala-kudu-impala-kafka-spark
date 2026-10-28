package com.StreamSpark.createTablesIceberg

import com.StreamSpark.utils.SparkIceberg

object CreateTableOrderPayments extends SparkIceberg {

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession()

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
