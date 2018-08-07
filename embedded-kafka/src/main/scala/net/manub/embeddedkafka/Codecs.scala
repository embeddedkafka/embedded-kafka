package net.manub.embeddedkafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization._

/** useful encoders/serializers, decoders/deserializers and [[ConsumerRecord]] decoders**/
object Codecs {
  implicit val stringSerializer: Serializer[String] = new StringSerializer()
  implicit val nullSerializer: Serializer[Array[Byte]] =
    new ByteArraySerializer()

  implicit val stringDeserializer: Deserializer[String] =
    new StringDeserializer()
  implicit val nullDeserializer: Deserializer[Array[Byte]] =
    new ByteArrayDeserializer()

  implicit val stringKeyValueCrDecoder
    : ConsumerRecord[String, String] => (String, String) =
    cr => (cr.key(), cr.value)
  implicit val stringValueCrDecoder: ConsumerRecord[String, String] => String =
    _.value()
  implicit val stringKeyValueTopicCrDecoder
    : ConsumerRecord[String, String] => (String, String, String) = cr =>
    (cr.topic(), cr.key(), cr.value())

  implicit val keyNullValueCrDecoder
    : ConsumerRecord[String, Array[Byte]] => (String, Array[Byte]) =
    cr => (cr.key(), cr.value)
  implicit val nullValueCrDecoder
    : ConsumerRecord[String, Array[Byte]] => Array[Byte] = _.value()
  implicit val keyNullValueTopicCrDecoder
    : ConsumerRecord[String, Array[Byte]] => (String, String, Array[Byte]) =
    cr => (cr.topic(), cr.key(), cr.value())
}
