package io.github.embeddedkafka

import io.github.embeddedkafka.Codecs.stringSerializer
import io.github.embeddedkafka.EmbeddedKafka._
import org.apache.kafka.clients.consumer.ConsumerConfig

class ConsumerGroupProtocolSpec extends EmbeddedKafkaSpecSupport {

  "embedded kafka with consumer group protocol" should {

    "work with the new consumer group protocol" in {

      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
        customBrokerProperties = Map(
          "group.coordinator.rebalance.protocols" -> "consumer,classic"
        ),
        customConsumerProperties =
          Map(ConsumerConfig.GROUP_PROTOCOL_CONFIG -> "consumer")
      )

      withRunningKafka {

        publishToKafka("my-topic", "hello")

        consumeFirstStringMessageFrom("my-topic") shouldBe "hello"

      }

    }
  }
}
