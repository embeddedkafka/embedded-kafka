package net.manub.embeddedkafka.avro

import io.confluent.kafka.serializers.{
  AbstractKafkaAvroSerDeConfig,
  KafkaAvroDeserializerConfig,
  KafkaAvroDeserializer => ConfluentKafkaAvroDeserializer,
  KafkaAvroSerializer => ConfluentKafkaAvroSerializer
}
import net.manub.embeddedkafka.EmbeddedKafkaConfig
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
      implicit config: EmbeddedKafkaConfig): Serde[T] = {
    val ser = new ConfluentKafkaAvroSerializer
    ser.configure(configForSchemaRegistry.getOrElse(Map.empty).asJava, false)
    val deser = new ConfluentKafkaAvroDeserializer
    deser.configure(consumerConfigForSchemaRegistry.getOrElse(Map.empty).asJava,
                    false)

    Serdes.serdeFrom(ser, deser).asInstanceOf[Serde[T]]
  }

  implicit def specificAvroSerializer[T <: SpecificRecord](
      implicit config: EmbeddedKafkaConfig): Serializer[T] =
    serdeFrom[T].serializer

  implicit def specificAvroDeserializer[T <: SpecificRecord](
      implicit config: EmbeddedKafkaConfig): Deserializer[T] =
    serdeFrom[T].deserializer

  /**
    * Returns an optional map of configuration to grant Schema Registry support.
    *
    * @param config an implicit [[EmbeddedKafkaConfig]].
    * @return
    */
  def configForSchemaRegistry(
      implicit config: EmbeddedKafkaConfig): Option[Map[String, Object]] =
    config.schemaRegistryPort.map(port =>
      Map[String, Object](
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> s"http://localhost:$port"
    ))

  /**
    * Returns an optional map of Kafka Consumer configuration to grant Schema Registry support.
    *
    * @param config an implicit [[EmbeddedKafkaConfig]].
    * @return
    */
  def consumerConfigForSchemaRegistry(
      implicit config: EmbeddedKafkaConfig): Option[Map[String, Object]] =
    configForSchemaRegistry.map(m =>
      m ++ Map(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> true.toString
    ))

}
