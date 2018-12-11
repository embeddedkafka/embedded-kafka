package net.manub.embeddedkafka

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

package object avro {
  implicit def specificAvroSerializer[T <: SpecificRecord]: Serializer[T] =
    new KafkaAvroSerializer[T]

  def specificAvroDeserializer[T <: SpecificRecord](
      schema: Schema): Deserializer[T] =
    new KafkaAvroDeserializer[T](schema)
}
