package net.manub.embeddedkafka

import io.confluent.kafka.serializers.{
  AbstractKafkaAvroSerDeConfig,
  KafkaAvroDeserializerConfig,
  KafkaAvroDeserializer => ConfluentKafkaAvroDeserializer,
  KafkaAvroSerializer => ConfluentKafkaAvroSerializer
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

  /**
    * Returns a map of configuration to grant Schema Registry support.
    *
    * @param config an implicit [[EmbeddedKafkaConfig]].
    * @return
    */
  def configForSchemaRegistry(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, Object] =
    Map(
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> s"http://localhost:${config.schemaRegistryPort}")

  /**
    * Returns a map of Kafka Consumer configuration to grant Schema Registry support.
    *
    * @param config an implicit [[EmbeddedKafkaConfig]].
    * @return
    */
  def consumerConfigForSchemaRegistry(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, Object] =
    configForSchemaRegistry ++ Map(
      KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> true.toString
    )

}
