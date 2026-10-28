package com.StreamSpark.createTablesKudu
import com.StreamSpark.utils.SparkKudu
import java.util
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.types._
import java.net.InetAddress

object CreateTableProductCategoryNameTranslation extends SparkKudu {

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val kuduContext = getKuduContext()
    val tableName = "master.product_category_name_translation"

    val schema = StructType(
      Array(
        StructField("product_category_name", StringType, nullable = false),
        StructField(
          "product_category_name_english",
          StringType,
          nullable = true
        )
      )
    )

    val primaryKey =
      List("product_category_name") // Definir la clave primaria de la tabla

    val tableOptions = new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(
        util.Arrays.asList(
          "product_category_name"
        ),
        2
      )

    // 4. Crear la tabla si no existe
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    } // Eliminar la tabla si existe
    kuduContext.createTable(tableName, schema, primaryKey, tableOptions)
    println(s"✅ Tabla '$tableName' create table ok")

    println("\n" + "=" * 60)
    println(s"✅ End of $tableName process")
    println("=" * 60)

    spark.stop()
  }

}
