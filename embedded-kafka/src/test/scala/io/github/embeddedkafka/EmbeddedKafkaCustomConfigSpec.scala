package io.github.embeddedkafka

import io.github.embeddedkafka.EmbeddedKafka._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs}

import scala.language.postfixOps
import scala.util.Random

class EmbeddedKafkaCustomConfigSpec extends EmbeddedKafkaSpecSupport {
  final val TwoMegabytes   = 2097152
  final val ThreeMegabytes = 3145728

  "the custom config" should {
    "allow pass additional producer parameters" in {
      val customBrokerConfig =
        Map(
          ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG -> s"$ThreeMegabytes",
          ServerConfigs.MESSAGE_MAX_BYTES_CONFIG -> s"$ThreeMegabytes"
        )

      val customProducerConfig =
        Map(ProducerConfig.MAX_REQUEST_SIZE_CONFIG -> s"$ThreeMegabytes")
      val customConsumerConfig =
        Map(
          ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> s"$ThreeMegabytes"
        )

      implicit val customKafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(
          customBrokerProperties = customBrokerConfig,
          customProducerProperties = customProducerConfig,
          customConsumerProperties = customConsumerConfig
        )

      val bigMessage = generateMessageOfLength(TwoMegabytes)
      val topic      = "big-message-topic"

      withRunningKafka {
        publishStringMessageToKafka(topic, bigMessage)
        consumeFirstStringMessageFrom(topic) shouldBe bigMessage
      }
    }
  }

  def generateMessageOfLength(length: Int): String =
    Iterator.continually(Random.nextPrintableChar()) take length mkString
}
