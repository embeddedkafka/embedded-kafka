package net.manub.embeddedkafka.schemaregistry.streams

import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.TestAvroClass
import net.manub.embeddedkafka.schemaregistry.avro.Codecs._
import net.manub.embeddedkafka.schemaregistry.{
  EmbeddedKafkaConfigWithSchemaRegistry,
  _
}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, Produced}
import org.apache.kafka.streams.{Consumed, StreamsBuilder}
import org.scalatest.{Matchers, WordSpec}

class ExampleKafkaStreamsSchemaRegistrySpec
    extends WordSpec
    with Matchers
    with EmbeddedKafkaStreamsWithSchemaRegistryAllInOne {

  implicit val config: EmbeddedKafkaConfigWithSchemaRegistry =
    EmbeddedKafkaConfigWithSchemaRegistry(kafkaPort = 7000,
                                          zooKeeperPort = 7001,
                                          schemaRegistryPort = 7002)

  val (inTopic, outTopic) = ("in", "out")

  val stringSerde: Serde[String] = Serdes.String()
  val avroSerde: Serde[TestAvroClass] = serdeFrom[TestAvroClass]

  "A Kafka streams test using Schema Registry" should {
    "be easy to run with streams and consumer lifecycle management" in {
      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, TestAvroClass] =
        streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, avroSerde))

      stream.to(outTopic, Produced.`with`(stringSerde, avroSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", TestAvroClass("world"))
        publishToKafka(inTopic, "foo", TestAvroClass("bar"))
        publishToKafka(inTopic, "baz", TestAvroClass("yaz"))
        withConsumer[String, TestAvroClass, Unit] { consumer =>
          val consumedMessages: Stream[(String, TestAvroClass)] =
            consumer.consumeLazily(outTopic)
          consumedMessages.take(2) should be(
            Seq("hello" -> TestAvroClass("world"),
                "foo" -> TestAvroClass("bar")))
          consumedMessages.drop(2).head should be("baz" -> TestAvroClass("yaz"))
        }
      }
    }

    "allow support creating custom consumers" in {
      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, TestAvroClass] =
        streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, avroSerde))

      stream.to(outTopic, Produced.`with`(stringSerde, avroSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", TestAvroClass("world"))
        publishToKafka(inTopic, "foo", TestAvroClass("bar"))
        val consumer = newConsumer[String, TestAvroClass]()
        consumer
          .consumeLazily[(String, TestAvroClass)](outTopic)
          .take(2) should be(
          Seq("hello" -> TestAvroClass("world"), "foo" -> TestAvroClass("bar")))
        consumer.close()
      }
    }
  }
}
