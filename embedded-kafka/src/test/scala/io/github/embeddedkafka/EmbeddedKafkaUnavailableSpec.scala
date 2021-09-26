package io.github.embeddedkafka

import io.github.embeddedkafka.EmbeddedKafka._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterAll

class EmbeddedKafkaUnavailableSpec
    extends EmbeddedKafkaSpecSupport
    with BeforeAndAfterAll {
  "the publishToKafka method" should {
    "throw a KafkaUnavailableException when Kafka is unavailable when trying to publish" in {
      a[KafkaUnavailableException] shouldBe thrownBy {
        implicit val serializer: StringSerializer = new StringSerializer()
        publishToKafka("non_existing_topic", "a message")
      }
    }
  }

  "the consumeFirstStringMessageFrom method" should {
    "throw a KafkaUnavailableException when there's no running instance of Kafka" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(customConsumerProperties =
          Map(
            ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG -> 1000.toString
          )
        )

      a[KafkaUnavailableException] shouldBe thrownBy {
        consumeFirstStringMessageFrom("non_existing_topic")
      }
    }
  }
}
