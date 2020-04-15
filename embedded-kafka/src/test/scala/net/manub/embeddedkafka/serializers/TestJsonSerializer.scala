package net.manub.embeddedkafka.serializers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

/**
  * Inspired by org.apache.kafka.connect.json.JsonSerializer
  */
class TestJsonSerializer[T] extends Serializer[T] {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def serialize(topic: String, data: T): Array[Byte] =
    Option(data).map { _ =>
      try mapper.writeValueAsBytes(data)
      catch {
        case e: Exception =>
          throw new SerializationException("Error serializing JSON message", e)
      }
    }.orNull
}
