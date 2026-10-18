package com.StreamSpark.createTablesKudu
import com.StreamSpark.utils.SparkKudu
import java.util
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.types._
import java.net.InetAddress

object CreateTableGeolocation extends SparkKudu {

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val kuduContext = getKuduContext()
    val tableName = "master.geolocation"

    val schema = StructType(
      Array(
        StructField("geolocation_id", IntegerType, nullable = false),
        StructField(
          "geolocation_zip_code_prefix",
          IntegerType,
          nullable = false
        ),
        StructField("geolocation_lat", DoubleType, nullable = false),
        StructField("geolocation_lng", DoubleType, nullable = false),
        StructField("geolocation_city", StringType, nullable = true),
        StructField("geolocation_state", StringType, nullable = true)
      )
    )

    val primaryKey =
      List("geolocation_id") // Definir la clave primaria de la tabla

    val tableOptions = new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(
        util.Arrays.asList(
          "geolocation_id"
        ),
        2
      ) // Mínimo 2 buckets requerido por Kudu

    // 4. Crear la tabla si no existe
    if (!kuduContext.tableExists(tableName)) {
      kuduContext.createTable(tableName, schema, primaryKey, tableOptions)
      println(s"✅ Tabla '$tableName' create table ok")
    } else {
      println(s"⚠️  the table '$tableName' exists")
    }
    println("\n" + "=" * 60)
    println(s"✅ End of $tableName process")
    println("=" * 60)

    spark.stop()
  }

}
