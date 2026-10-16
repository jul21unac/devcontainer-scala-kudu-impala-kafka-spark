package com.StreamSpark.createTablesIceberg
package com.StreamSpark.createTablesKudu
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.types._
import java.net.InetAddress
import java.util

object CreateOrderItems {

  def main(args: Array[String]): Unit = {

     val ake = "MpV3o8kVYR2d3Lp1gRPn"
    val sk = "QELgEuFQGx9siULGCrnuTvi8D2cbqJHbnBT2owbw"
    val spark = SparkSession
      .builder()
      .appName("HiveTableExample")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.access.key", ake)
      .config("spark.hadoop.fs.s3a.secret.key", sk)
      .enableHiveSupport()
      .getOrCreate()




    spark.sql("""
    CREATE TABLE IF NOT EXISTS test_2_minio.mi_tabla_7 (
      id INT,
      nombre STRING
    )
    STORED AS TEXTFILE
  """)

    spark.sql("""
  insert into test_2_minio.mi_tabla_7 values (2, 'lalal')
""")

    val resultado = spark.sql("SELECT * FROM test_2_minio.mi_tabla_7")
    resultado.show() // Muestra todas las filas (20 por defecto)

    spark.stop()

  }

}
