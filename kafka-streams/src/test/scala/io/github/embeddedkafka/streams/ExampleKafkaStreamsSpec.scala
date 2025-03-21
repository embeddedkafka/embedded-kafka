package io.github.embeddedkafka.streams

import io.github.embeddedkafka.Codecs._
import io.github.embeddedkafka.EmbeddedKafkaConfig
import io.github.embeddedkafka.streams.EmbeddedKafkaStreams._
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KStream, Produced}
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ExampleKafkaStreamsSpec
    extends AnyWordSpec
    with Matchers
    with Eventually {
  val (inTopic, outTopic) = ("in", "out")

  val stringSerde: Serde[String] = Serdes.String()

  "A Kafka streams test" should {
    "be easy to run with streams and consumer lifecycle management" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = 7000, controllerPort = 7001)

      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

      stream.to(outTopic, Produced.`with`(stringSerde, stringSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", "world")
        publishToKafka(inTopic, "foo", "bar")
        publishToKafka(inTopic, "baz", "yaz")

        val firstTwoMessages =
          consumeNumberKeyedMessagesFrom[String, String](outTopic, 2)

        firstTwoMessages should be(
          Seq("hello" -> "world", "foo" -> "bar")
        )

        val thirdMessage =
          consumeFirstKeyedMessageFrom[String, String](outTopic)

        thirdMessage should be("baz" -> "yaz")
      }
    }

    "be easy to run with streams on arbitrary available ports" in {
      val userDefinedConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = 0, controllerPort = 0)

      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

      stream.to(outTopic, Produced.`with`(stringSerde, stringSerde))

      runStreamsOnFoundPort(userDefinedConfig)(
        Seq(inTopic, outTopic),
        streamBuilder.build()
      ) { implicit config =>
        publishToKafka(inTopic, "hello", "world")
        publishToKafka(inTopic, "foo", "bar")
        publishToKafka(inTopic, "baz", "yaz")

        val firstTwoMessages =
          consumeNumberKeyedMessagesFrom[String, String](outTopic, 2)

        firstTwoMessages should be(
          Seq("hello" -> "world", "foo" -> "bar")
        )

        val thirdMessage =
          consumeFirstKeyedMessageFrom[String, String](outTopic)

        thirdMessage should be("baz" -> "yaz")
      }
    }

    "allow support creating custom consumers" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = 7000, controllerPort = 7001)

      implicit val patienceConfig: PatienceConfig =
        PatienceConfig(5.seconds, 100.millis)

      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

      stream.to(outTopic, Produced.`with`(stringSerde, stringSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", "world")
        publishToKafka(inTopic, "foo", "bar")

        withConsumer[String, String, Assertion] { consumer =>
          consumer.subscribe(java.util.Collections.singleton(outTopic))

          eventually {
            val records = consumer
              .poll(java.time.Duration.ofMillis(100.millis.toMillis))
              .asScala
              .map(r => (r.key, r.value))

            records shouldBe Seq("hello" -> "world", "foo" -> "bar")
          }
        }
      }
    }
  }
}
