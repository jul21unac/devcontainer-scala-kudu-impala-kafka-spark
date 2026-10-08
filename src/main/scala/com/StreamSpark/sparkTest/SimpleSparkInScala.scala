package com.StreamSpark.sparkTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SimpleSparkInScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Simple does nothing")
      .setMaster("local")
      .set("spark.driver.memory", "1g")

    val sc = new SparkContext(conf)

    val list = List(1, 2)
    val simpleRDD = sc.parallelize(list)
    simpleRDD.foreach { x => println(x) }

    testHive()
  }

  def testImpala(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ImpalaExample")
      .master("local")
      .getOrCreate()

    val impalaUrl = "jdbc:hive2://impala:21050/default;auth=noSasl"
    val driver = "org.apache.hive.jdbc.HiveDriver"

    // 1. Crear la tabla en Impala (ejecutar SQL directo vía JDBC)
    val conn = java.sql.DriverManager.getConnection(impalaUrl)
    val stmt = conn.createStatement()
    stmt.execute("""
      CREATE TABLE IF NOT EXISTS mi_tabla_7 (
        id INT,
        nombre STRING
      )
      STORED AS PARQUET
    """)
    stmt.execute("INSERT INTO mi_tabla_7 VALUES (2, 'lalal')")
    conn.close()

    // 2. Leer la tabla desde Spark
    val resultado = spark.read
      .format("jdbc")
      .option("url", impalaUrl)
      .option("dbtable", "mi_tabla_7")
      .option("driver", driver)
      .load()

    resultado.show()

    // 3. Escribir un DataFrame nuevo en Impala
    val nuevosDatos = Seq((3, "nuevo"), (4, "dato"))
    val df = spark.createDataFrame(nuevosDatos).toDF("id", "nombre")

    df.write
      .format("jdbc")
      .option("url", impalaUrl)
      .option("dbtable", "mi_tabla_7")
      .option("driver", driver)
      .mode("append")
      .save()

    spark.stop()
  }

  def testHive(): Unit = {
    /*    val spark = SparkSession.builder()
      .appName("HiveTableExample")
      .config("hive.metastore.uris", "thrift://localhost:9083")  // O host.docker.internal si Spark en Docker
      .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse")
      .enableHiveSupport()
      .getOrCreate()
     */
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

    println(
      spark.sparkContext.hadoopConfiguration.get("fs.s3a.access.key")
    )
    println(
      spark.sparkContext.hadoopConfiguration.get("fs.s3a.endpoint")
    )
    println(
      spark.sparkContext.hadoopConfiguration.get("fs.s3a.endpoint")
    )

    println(org.apache.hadoop.util.VersionInfo.getVersion)
    println(classOf[org.apache.hadoop.fs.s3a.S3AFileSystem])

    spark
      .sql("""
    DESCRIBE DATABASE EXTENDED test_2_minio
    """)
      .show(false)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS test_2_minio.mi_tabla_6 (
      id INT,
      nombre STRING
    )
    STORED AS TEXTFILE
  """)

    spark.sql("""
  insert into test_2_minio.mi_tabla_6 values (2, 'lalal')
""")

    val resultado = spark.sql("SELECT * FROM test_2_minio.mi_tabla_6")
    resultado.show() // Muestra todas las filas (20 por defecto)

    spark.stop()
  }
}
