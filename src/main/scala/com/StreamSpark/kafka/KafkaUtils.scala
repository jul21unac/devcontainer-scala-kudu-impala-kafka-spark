package com.StreamSpark.kafka
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.common.config.TopicConfig

import java.util.{Collections, Properties}
import scala.util.{Failure, Success, Try}
import com.StreamSpark.utils._
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

import scala.jdk.CollectionConverters._


class KafkaUtils {


  val conf = new LoadConfig
  val broker = conf.getConfig("kafka","bootstrapServers")
  val schemaRegistryUrl = conf.getConfig("kafka","registryUrl")
  def createTopic(topicName : String) : Unit=
    {


        // 1. Configurar las propiedades de conexión al clúster de Kafka
        val props = new Properties()
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker)

        // 2. Crear la instancia del cliente Admin de forma segura con Try
        val admin = Admin.create(props)


        val numPartitions = 1
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

  def deleteTopic( nameTopic : String) :Unit =
    {

      val props = new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker)

      // 2. Crear la instancia del cliente Admin de forma segura con Try
      val admin = Admin.create(props)
      val ltop = (nameTopic)
      try {
        admin.deleteTopics(Collections.singletonList(nameTopic))
          .all()
          .get() // espera a que termine
      } finally {
        admin.close()
      }
    }

  def loadSchema(subject :String , schema :String) : Unit = {

    val schemaRegistryClient =
      new CachedSchemaRegistryClient(schemaRegistryUrl, 100)

    val schemaStrip = schema.stripMargin

    schemaRegistryClient.register(
      subject,
      new AvroSchema(schemaStrip)
    )



  }

}
