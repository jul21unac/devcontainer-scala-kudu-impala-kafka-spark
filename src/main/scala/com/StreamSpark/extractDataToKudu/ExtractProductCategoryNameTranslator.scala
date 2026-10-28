package com.StreamSpark.extractDataToKudu
import com.StreamSpark.utils.SparkKudu

object ExtractProductCategoryNameTranslator extends SparkKudu {

  val csvPath = "data/product_category_name_translation.csv"

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val kuduContext = getKuduContext()

    val df = spark.read
      .option("header", "true")
      // .option("inferSchema", "true")
      .csv(csvPath)

    df.show(10, false)

    df.printSchema()

    df.write
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTERS)
      .option("kudu.table", "master.product_category_name_translation")
      .mode("append")
      .save()

    spark.stop()

  }

}
