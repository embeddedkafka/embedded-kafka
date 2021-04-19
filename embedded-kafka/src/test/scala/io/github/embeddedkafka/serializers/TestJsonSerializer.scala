package io.github.embeddedkafka.serializers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

import scala.util.control.NonFatal

/**
  * Inspired by `org.apache.kafka.connect.json.JsonSerializer`
  */
class TestJsonSerializer[T] extends Serializer[T] {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def serialize(topic: String, data: T): Array[Byte] =
    Option(data).map { _ =>
      try mapper.writeValueAsBytes(data)
      catch {
        case NonFatal(e) =>
          throw new SerializationException("Error serializing JSON message", e)
      }
    }.orNull
}
