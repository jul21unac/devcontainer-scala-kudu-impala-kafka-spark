package com.StreamSpark.extractDataToKudu
import com.StreamSpark.utils.SparkKudu
import org.apache.spark.sql.functions._

object ExtractProducts extends SparkKudu {

  val csvPath = "data/olist_products_dataset.csv"

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val kuduContext = getKuduContext()

    val df = spark.read
      .option("header", "true")
      // .option("inferSchema", "true")
      .csv(csvPath)

    val df_final = df
      .withColumn("product_name_lenght", col("product_name_lenght").cast("int"))
      .withColumn(
        "product_description_lenght",
        col("product_description_lenght").cast("int")
      )
      .withColumn("product_photos_qty", col("product_photos_qty").cast("int"))
      .withColumn("product_weight_g", col("product_weight_g").cast("double"))
      .withColumn(
        "product_length_cm",
        col("product_length_cm").cast("double")
      )
      .withColumn(
        "product_height_cm",
        col("product_height_cm").cast("double")
      )
      .withColumn(
        "product_width_cm",
        col("product_width_cm").cast("double")
      )

    df_final.show(10, false)

    df_final.printSchema()

    df_final.write
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", "master.products")
      .mode("append")
      .save()

    spark.stop()

  }

}
