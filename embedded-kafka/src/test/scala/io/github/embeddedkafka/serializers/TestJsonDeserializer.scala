package io.github.embeddedkafka.serializers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * Inspired by `org.apache.kafka.connect.json.JsonDeserializer`
  */
class TestJsonDeserializer[T](implicit tag: ClassTag[T], ev: Null <:< T)
    extends Deserializer[T] {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def deserialize(topic: String, bytes: Array[Byte]): T =
    Option(bytes).map { _ =>
      try
        mapper.readValue(
          bytes,
          tag.runtimeClass.asInstanceOf[Class[T]]
        )
      catch {
        case NonFatal(e) => throw new SerializationException(e)
      }
    }.orNull
}
