package net.manub.embeddedkafka

import io.confluent.kafka.serializers.{
  KafkaAvroDeserializer => ConfluentKafkaAvroDeserializer,
  KafkaAvroSerializer => ConfluentKafkaAvroSerializer
}
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaWithSchemaRegistry.{
  configForSchemaRegistry,
  consumerConfigForSchemaRegistry
}
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.{
  Deserializer,
  Serde,
  Serdes,
  Serializer
}

import scala.collection.JavaConverters._

package object schemaregistry {

  implicit def serdeFrom[T <: SpecificRecord](
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry): Serde[T] = {
    val ser = new ConfluentKafkaAvroSerializer
    ser.configure(configForSchemaRegistry.asJava, false)
    val deser = new ConfluentKafkaAvroDeserializer
    deser.configure(consumerConfigForSchemaRegistry.asJava, false)

    Serdes.serdeFrom(ser, deser).asInstanceOf[Serde[T]]
  }

  implicit def specificAvroSerializer[T <: SpecificRecord](
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry): Serializer[T] =
    serdeFrom[T].serializer

  implicit def specificAvroDeserializer[T <: SpecificRecord](
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry): Deserializer[T] =
    serdeFrom[T].deserializer

}
