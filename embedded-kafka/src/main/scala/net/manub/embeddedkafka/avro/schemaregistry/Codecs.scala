package net.manub.embeddedkafka.avro.schemaregistry

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

object Codecs {

  implicit def stringKeyAvroValueCrDecoder[V <: SpecificRecord]
    : ConsumerRecord[String, V] => (String, V) =
    cr => (cr.key(), cr.value)
  implicit def avroValueCrDecoder[V <: SpecificRecord]
    : ConsumerRecord[String, V] => V =
    _.value()
  implicit def stringKeyAvroValueTopicCrDecoder[V <: SpecificRecord]
    : ConsumerRecord[String, V] => (String, String, V) =
    cr => (cr.topic(), cr.key(), cr.value())

}
