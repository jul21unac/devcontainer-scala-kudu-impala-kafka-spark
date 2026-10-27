package com.StreamSpark.createTablesKudu
import com.StreamSpark.utils.SparkKudu
import java.util
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.types._
import java.net.InetAddress

object CreateTableSellers extends SparkKudu {

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val kuduContext = getKuduContext()
    val tableName = "master.sellers"

    val schema = StructType(
      Array(
        StructField("seller_id", StringType, nullable = false),
        StructField("seller_city", StringType, nullable = true),
        StructField("seller_state", StringType, nullable = true),
        StructField("seller_zip_code_prefix", StringType, nullable = true)
      )
    )

    val primaryKey =
      List("seller_id") // Definir la clave primaria de la tabla

    val tableOptions = new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(
        util.Arrays.asList(
          "seller_id"
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
