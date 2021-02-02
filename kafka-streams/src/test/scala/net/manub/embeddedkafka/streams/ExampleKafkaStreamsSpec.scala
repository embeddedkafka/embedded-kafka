package net.manub.embeddedkafka.streams

import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreams._
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KStream, Produced}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ExampleKafkaStreamsSpec extends AnyWordSpec with Matchers {
  val (inTopic, outTopic) = ("in", "out")

  val stringSerde: Serde[String] = Serdes.String()

  "A Kafka streams test" should {
    "be easy to run with streams and consumer lifecycle management" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)

      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

      stream.to(outTopic, Produced.`with`(stringSerde, stringSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", "world")
        publishToKafka(inTopic, "foo", "bar")
        publishToKafka(inTopic, "baz", "yaz")
        withConsumer[String, String, Assertion] { consumer =>
          val consumedMessages =
            consumer.consumeLazily[(String, String)](outTopic)
          consumedMessages.take(2).toList should be(
            Seq("hello" -> "world", "foo" -> "bar")
          )
          val h :: _ = consumedMessages.drop(2).toList
          h should be("baz" -> "yaz")
        }
      }
    }

    "be easy to run with streams on arbitrary available ports" in {
      val userDefinedConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

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
        withConsumer[String, String, Assertion] { consumer =>
          val consumedMessages =
            consumer.consumeLazily[(String, String)](outTopic)
          consumedMessages.take(2).toList should be(
            Seq("hello" -> "world", "foo" -> "bar")
          )
          val h :: _ = consumedMessages.drop(2).toList
          h should be("baz" -> "yaz")
        }
      }
    }

    "allow support creating custom consumers" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)

      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

      stream.to(outTopic, Produced.`with`(stringSerde, stringSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", "world")
        publishToKafka(inTopic, "foo", "bar")

        withConsumer[String, String, Assertion] { consumer =>
          consumer.consumeLazily[(String, String)](outTopic).take(2) should be(
            Seq("hello" -> "world", "foo" -> "bar")
          )
        }
      }
    }

    "allow for easy string based testing" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)

      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

      stream.to(outTopic, Produced.`with`(stringSerde, stringSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build())(
        withConsumer[String, String, Assertion]({ consumer =>
          publishToKafka(inTopic, "hello", "world")
          val h :: _ = consumer.consumeLazily[(String, String)](outTopic).toList
          h should be("hello" -> "world")
        })
      )(config)

    }
  }
}
