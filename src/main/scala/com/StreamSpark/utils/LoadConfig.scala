package com.StreamSpark.utils
import io.circe.generic.auto._
import io.circe.yaml.parser

import scala.io.Source

class LoadConfig {


  def loadTest(): Unit = {
    val yamlString =
      Source
        .fromFile("properties.yml")
        .mkString

    val json = parser.parse(yamlString).toOption.get

    val brokers =
      json.hcursor
        .downField("kafka")
        .get[String]("bootstrapServers")
        .toOption
        .get

    println(brokers)

  }

}
