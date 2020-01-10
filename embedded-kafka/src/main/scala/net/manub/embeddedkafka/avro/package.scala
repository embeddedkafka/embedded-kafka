package net.manub.embeddedkafka

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

package object avro {
  @deprecated
  implicit def specificAvroSerializer[T <: SpecificRecord]: Serializer[T] =
    new KafkaAvroSerializer[T]

  @deprecated
  def specificAvroDeserializer[T <: SpecificRecord](
      schema: Schema
  ): Deserializer[T] =
    new KafkaAvroDeserializer[T](schema)
}
