package io.github.embeddedkafka

import io.github.embeddedkafka.EmbeddedKafka._
import io.github.embeddedkafka.EmbeddedKafkaSpecSupport.{
  Available,
  NotAvailable
}
import org.scalatest.Assertion

class EmbeddedKafkaWithRunningKafkaOnFoundPortSpec
    extends EmbeddedKafkaSpecSupport {
  "the withRunningKafkaOnFoundPort method" should {
    "start and stop Kafka broker and controller successfully on non-zero ports" in {
      val userDefinedConfig =
        EmbeddedKafkaConfig(kafkaPort = 12345, controllerPort = 54321)
      val actualConfig = withRunningKafkaOnFoundPort(userDefinedConfig) {
        actualConfig =>
          actualConfig shouldBe userDefinedConfig
          everyServerIsAvailable(actualConfig)
          actualConfig
      }
      noServerIsAvailable(actualConfig)
    }

    "start and stop multiple Kafka broker and controller successfully on arbitrary available ports" in {
      val userDefinedConfig =
        EmbeddedKafkaConfig(kafkaPort = 0, controllerPort = 0)
      val actualConfig1 = withRunningKafkaOnFoundPort(userDefinedConfig) {
        actualConfig1 =>
          everyServerIsAvailable(actualConfig1)
          publishStringMessageToKafka("topic", "message1")(actualConfig1)
          consumeFirstStringMessageFrom("topic")(
            actualConfig1
          ) shouldBe "message1"
          val actualConfig2 = withRunningKafkaOnFoundPort(userDefinedConfig) {
            actualConfig2 =>
              everyServerIsAvailable(actualConfig2)
              publishStringMessageToKafka("topic", "message2")(actualConfig2)
              consumeFirstStringMessageFrom("topic")(
                actualConfig2
              ) shouldBe "message2"
              val allConfigs =
                Seq(userDefinedConfig, actualConfig1, actualConfig2)
              // Confirm both actual configs are running on separate non-zero ports, but otherwise equal
              allConfigs.map(_.kafkaPort).distinct should have size 3
              allConfigs.map(_.controllerPort).distinct should have size 3
              allConfigs
                .map(config =>
                  EmbeddedKafkaConfigImpl(
                    kafkaPort = 0,
                    controllerPort = 0,
                    config.customBrokerProperties,
                    config.customProducerProperties,
                    config.customConsumerProperties
                  )
                )
                .distinct should have size 1
              actualConfig2
          }
          noServerIsAvailable(actualConfig2)
          actualConfig1
      }
      noServerIsAvailable(actualConfig1)
    }

    "work with a simple example using implicits" in {
      val userDefinedConfig =
        EmbeddedKafkaConfig(kafkaPort = 0, controllerPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        publishStringMessageToKafka("topic", "message")
        consumeFirstStringMessageFrom("topic") shouldBe "message"
      }
    }
  }

  private def everyServerIsAvailable(config: EmbeddedKafkaConfig): Assertion = {
    expectedServerStatus(config.controllerPort, Available)
    expectedServerStatus(config.kafkaPort, Available)
  }

  private def noServerIsAvailable(config: EmbeddedKafkaConfig): Assertion = {
    expectedServerStatus(config.controllerPort, NotAvailable)
    expectedServerStatus(config.kafkaPort, NotAvailable)
  }
}
