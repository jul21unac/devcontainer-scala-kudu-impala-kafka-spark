package com.StreamSpark.createTablesKudu
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.kudu.client.CreateTableOptions
import org.apache.spark.sql.types._
import java.net.InetAddress
import java.util

object LoadCustomer {
  val KUDU_MASTERS = "kudu-master-1:7051,kudu-master-2:7051,kudu-master-3:7051"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CreateCustomerKuduTable")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.kudu.operation.timeout.ms", "60000")
      .config("spark.kudu.admin.operation.timeout.ms", "60000")
      .enableHiveSupport()
      .getOrCreate()

    val kuduContext = new KuduContext(KUDU_MASTERS, spark.sparkContext)

    val tableName = "master.customers"

    // 2. Definir el schema
    val schema = StructType(
      Seq(
        StructField("customer_id", StringType, nullable = false),
        StructField("customer_unique_id", StringType, nullable = true),
        StructField("customer_zip_code_prefix", StringType, nullable = true),
        StructField("customer_city", StringType, nullable = true),
        StructField("customer_state", StringType, nullable = true)
      )
    )

    // 3. Definir la clave primaria y opciones de la tabla
    val primaryKey = List("customer_id")

    val tableOptions = new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(
        util.Arrays.asList("customer_id"),
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
    println("✅ End of LoadCustomer process")
    println("=" * 60)

    spark.stop()

  }
}
