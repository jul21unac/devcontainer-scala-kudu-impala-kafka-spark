package com.StreamSpark

import com.StreamSpark.kafka.KafkaUtils
import com.StreamSpark.utils.LoadConfig

/** @author
  *   ${user.name}
  */
object App {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    println("Hello World!")
    //println("concat arguments = " + foo(args))

    val testLoad = new LoadConfig

    val tstvalue = testLoad.getConfig("kafka","registryUrl")

    println(tstvalue)

    val kafkaU =  new KafkaUtils
    //kafkaU.createTopic("orders.order")
    //kafkaU.deleteTopic("mi-topic-scala_julio")

   /*
    val subjKey = """
      |{
      | "type":"record",
      | "name":"ProductKey",
      | "fields":[
      |   {"name":"product_id","type":"long"}
      | ]
      |}
      |"""



    kafkaU.loadSchema( "orders.order-key", subjKey)
    */

    val subjectValue =
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
                          |"""

    kafkaU.loadSchema( "orders.order-value", subjectValue)

  }

}
