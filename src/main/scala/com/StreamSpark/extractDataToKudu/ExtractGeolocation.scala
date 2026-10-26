package com.StreamSpark.extractDataToKudu
import com.StreamSpark.utils.SparkKudu
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ExtractGeolocation extends SparkKudu {

  val csvPath = "data/olist_geolocation_dataset.csv"

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val kuduContext = getKuduContext()

    val df = spark.read
      .option("header", "true")
      // .option("inferSchema", "true")
      .csv(csvPath)

    val df_final = df
      .withColumn(
        "geolocation_id",
        row_number()
          .over(Window.orderBy(lit(1)))
          .cast("int")
      )
      .withColumn("geolocation_lat", df("geolocation_lat").cast("double"))
      .withColumn("geolocation_lng", df("geolocation_lng").cast("double"))

    df_final.show(10, false)

    df_final.printSchema()

    df_final.write
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", "master.geolocation")
      .mode("append")
      .save()

    spark.stop()

  }

}
