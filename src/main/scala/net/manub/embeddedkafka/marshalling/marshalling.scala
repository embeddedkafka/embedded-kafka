package net.manub.embeddedkafka

import kafka.serializer._
import org.apache.kafka.common.serialization._


package object marshalling {
  implicit val stringEncoder: Encoder[String] = new StringEncoder()
  implicit val nullEncoder: Encoder[Array[Byte]] = new DefaultEncoder()
  implicit val stringSerializer: Serializer[String] = new StringSerializer()
  implicit val nullSerializer: Serializer[Array[Byte]] = new ByteArraySerializer()

  implicit val stringDecoder: Decoder[String] = new StringDecoder()
  implicit val nullDecoder: Decoder[Array[Byte]] = new DefaultDecoder()
  implicit val stringDeserializer: Deserializer[String] = new StringDeserializer()
  implicit val nullDeserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer()
}
