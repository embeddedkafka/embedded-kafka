package net.manub.embeddedkafka.marshalling

import kafka.serializer.{Encoder, Decoder}
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

package object avro {
  implicit def specificAvroSerializer[T <: SpecificRecord] : Serializer[T] = new KafkaAvroSerializer[T]
  implicit def specificAvroEncoder[T <: SpecificRecord] : Encoder[T] = new KafkaAvroEncoder[T]

  def specificAvroDeserializer[T <: SpecificRecord](schema: Schema) : Deserializer[T] =
    new KafkaAvroDeserializer[T](schema)

  def specificAvroDecoder[T <: SpecificRecord](schema: Schema, props: VerifiableProperties = null) : Decoder[T] =
    new KafkaAvroDecoder[T](schema, props)
}

