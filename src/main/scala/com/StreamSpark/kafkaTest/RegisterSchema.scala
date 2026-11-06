package com.StreamSpark.kafkaTest
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.common.config.TopicConfig

import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.avro.AvroSchema

object RegisterSchema {

  def main(args: Array[String]): Unit = { registerSchema() }

  def registerSchema(): Unit = {
    val schemaRegistryUrl = "http://schema-registry:8081"
    val schemaRegistryClient =
      new CachedSchemaRegistryClient(schemaRegistryUrl, 100)

    val keySchema =
      """
    |{
    | "type":"record",
    | "name":"ProductKey",
    | "fields":[
    |   {"name":"product_id","type":"long"}
    | ]
    |}
    |""".stripMargin

    schemaRegistryClient.register(
      "mi-topic-scala_julio-key",
      new AvroSchema(keySchema)
    )

    val subject = "mi-topic-scala_julio-value"
    val schemaString =
      """
        |{
        |  "type": "record",
        |  "name": "MiRegistro",
        |  "namespace": "com.ejemplo",
        |  "fields": [
        |    {"name": "id", "type": "int"},
        |    {"name": "nombre", "type": "string"},
        |    {"name": "edad", "type": ["null", "int"], "default": null}
        |  ]
        |}
        |""".stripMargin

    val avroSchema = new AvroSchema(schemaString)

    Try(schemaRegistryClient.register(subject, avroSchema)) match {
      case Success(schemaId) =>
        println(s"¡Esquema registrado exitosamente con ID: $schemaId")
      case Failure(exception) =>
        System.err.println(
          s"Error al registrar el esquema: ${exception.getMessage}"
        )
    }
  }

  def testCreateSchema(): Unit = {

    // 1. Configurar las propiedades de conexión al clúster de Kafka
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:29092")

    // 2. Crear la instancia del cliente Admin de forma segura con Try
    val admin = Admin.create(props)

    val topicName = "mi-topic-scala_julio"
    val numPartitions = 3
    val replicationFactor: Short = 1 // Ajustar según los brokers disponibles

    // 3. Instanciar la clase NewTopic pasándole los parámetros requeridos
    val newTopic = new NewTopic(topicName, numPartitions, replicationFactor)

    // Opcional: Agregar configuraciones específicas convirtiendo un Map de Scala a Java
    val topicConfig = Map(
      TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT,
      TopicConfig.RETENTION_MS_CONFIG -> "604800000" // 7 días de retención
    )
    newTopic.configs(topicConfig.asJava)

    println(s"Enviando solicitud de creación para: $topicName")

    // 4. Ejecutar la creación (se envuelve la lista de Scala a una colección de Java)
    val result = admin.createTopics(List(newTopic).asJava)

    // 5. Resolver el Future de Java de manera síncrona
    Try(result.values().get(topicName).get()) match {
      case Success(_) =>
        println(s"¡El topic '$topicName' se creó exitosamente!")
      case Failure(exception) =>
        System.err.println(
          s"Error al intentar crear el topic: ${exception.getMessage}"
        )
    }

    // 6. Cerrar los recursos del cliente Admin
    admin.close()
  }

}
