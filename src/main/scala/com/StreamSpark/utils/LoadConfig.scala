package com.StreamSpark.utils
import io.circe.generic.auto._
import io.circe.yaml.parser

import scala.io.Source

class LoadConfig {

  val propertiesFile =  "properties.yml"

  def getConfig(downF : String, property : String): String = {

    val yamlString = Source.fromFile(propertiesFile).mkString

    val json = parser.parse(yamlString).toOption.get

    val value =
      json.hcursor
        .downField(downF)
        .get[String](property)
        .toOption
        .get

    value

  }



}
