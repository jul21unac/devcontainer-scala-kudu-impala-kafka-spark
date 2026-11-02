package com.StreamSpark.kafkaTest
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

object RegisterSchema {

  def main(args: Array[String]): Unit = {

    val keySchema = """{
    "type": "record",
    "name": "KeySchema",
    "fields": [
        {"name": "id", "type": "string"}
    ] }"""

    val valueSchema = """{
    "type": "record",
    "name": "ValueSchema",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "email", "type": "string"}
    ] }"""

    registerSchema("my-key-schema", keySchema)
    registerSchema("my-value-schema", valueSchema)

  }

  def registerSchema(schemaName: String, schemaDefinition: String): Unit = {
    val url = s"http://schema-registry:8081/subjects/$schemaName/versions"
    val requestBody = s"""{"schema": "$schemaDefinition"}"""

    println(url)

    val request = HttpRequest
      .newBuilder()
      .uri(URI.create(url))
      .header("Content-Type", "application/vnd.schemaregistry.v1+json")
      .POST(HttpRequest.BodyPublishers.ofString(requestBody))
      .build()

    println("aqui")

    val client = HttpClient.newHttpClient()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    if (response.statusCode() == 200) {
      println(s"Schema '$schemaName' registered successfully.")
    } else {
      println(
        s"Failed to register schema '$schemaName'. Response code: ${response.statusCode()}"
      )
      println(s"Response body: ${response.body()}")
    }

  }

}
