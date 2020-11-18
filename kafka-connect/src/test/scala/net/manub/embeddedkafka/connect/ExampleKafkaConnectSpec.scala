package net.manub.embeddedkafka.connect

import java.nio.file.Files

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.runtime.WorkerConfig

import net.manub.embeddedkafka.{EmbeddedKafkaConfig, EmbeddedKafkaSpecSupport}
import net.manub.embeddedkafka.EmbeddedKafkaSpecSupport._
import net.manub.embeddedkafka.connect.EmbeddedKafkaConnect._

class ExampleKafkaConnectSpec extends EmbeddedKafkaSpecSupport {
  implicit val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)

  "A Kafka Connect test" should {
    "start a Connect server on a specified port" in {
      val connectPort = 7002
      val offsets     = Files.createTempFile("connect", ".offsets").toFile
      startConnect(connectPort, offsets) {
        expectedServerStatus(connectPort, Available)
      }

      expectedServerStatus(connectPort, NotAvailable)
    }

    "start a Connect server with custom properties" in {
      val connectPort = 7002
      val offsets     = Files.createTempFile("connect", ".offsets").toFile
      val extraConfig = Map(
        WorkerConfig.KEY_CONVERTER_CLASS_CONFIG   -> "org.apache.kafka.connect.storage.StringConverter",
        WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.storage.StringConverter"
      )
      startConnect(connectPort, offsets, extraConfig) {
        expectedServerStatus(connectPort, Available)
      }

      expectedServerStatus(connectPort, NotAvailable)
    }

    "fail to start a Connect server with invalid properties" in {
      val connectPort = 7002
      val offsets     = Files.createTempFile("connect", ".offsets").toFile
      val extraConfig = Map(
        WorkerConfig.KEY_CONVERTER_CLASS_CONFIG -> "InvalidKeyConverter"
      )
      a[ConfigException] shouldBe thrownBy {
        startConnect(connectPort, offsets, extraConfig) {}
      }

      expectedServerStatus(connectPort, NotAvailable)
    }
  }
}
