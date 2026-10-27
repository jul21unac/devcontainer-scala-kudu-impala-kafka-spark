package com.StreamSpark.createTablesKudu
import com.StreamSpark.utils.SparkKudu
import java.util
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.types._
import java.net.InetAddress

object CreateTableProducts extends SparkKudu {

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val kuduContext = getKuduContext()
    val tableName = "master.products"

    val schema = StructType(
      Array(
        StructField("product_id", StringType, nullable = false),
        StructField("product_category_name", StringType, nullable = true),
        StructField("product_name_lenght", IntegerType, nullable = true),
        StructField("product_description_lenght", IntegerType, nullable = true),
        StructField("product_photos_qty", IntegerType, nullable = true),
        StructField("product_weight_g", DoubleType, nullable = true),
        StructField("product_length_cm", DoubleType, nullable = true),
        StructField("product_height_cm", DoubleType, nullable = true),
        StructField("product_width_cm", DoubleType, nullable = true)
      )
    )

    val primaryKey =
      List("product_id") // Definir la clave primaria de la tabla

    val tableOptions = new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(
        util.Arrays.asList(
          "product_id"
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
