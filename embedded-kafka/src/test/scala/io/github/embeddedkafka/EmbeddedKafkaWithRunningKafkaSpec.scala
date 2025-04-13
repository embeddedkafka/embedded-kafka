package io.github.embeddedkafka

import io.github.embeddedkafka.EmbeddedKafka._
import io.github.embeddedkafka.EmbeddedKafkaSpecSupport._
import org.scalatest.exceptions.TestFailedException
import io.github.embeddedkafka.EmbeddedKafkaConfig.{
  defaultControllerPort,
  defaultKafkaPort
}

class EmbeddedKafkaWithRunningKafkaSpec extends EmbeddedKafkaSpecSupport {
  "the withRunningKafka method" should {
    "start a Kafka broker on port 6001 by default" in {
      withRunningKafka {
        expectedServerStatus(defaultKafkaPort, Available)
      }
    }

    "stop Kafka successfully" when {
      "the enclosed test passes" in {
        withRunningKafka {
          true shouldBe true
        }

        expectedServerStatus(defaultControllerPort, NotAvailable)
        expectedServerStatus(defaultKafkaPort, NotAvailable)
      }

      "the enclosed test fails" in {
        a[TestFailedException] shouldBe thrownBy {
          withRunningKafka {
            true shouldBe false
          }
        }

        expectedServerStatus(defaultControllerPort, NotAvailable)
        expectedServerStatus(defaultKafkaPort, NotAvailable)
      }
    }

    "start a Kafka broker on a specified port" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = 12345)

      withRunningKafka {
        expectedServerStatus(12345, Available)
      }
    }

    "start a Kafka controller on a specified port" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(controllerPort = 12345)

      withRunningKafka {
        expectedServerStatus(12345, Available)
      }
    }

  }
}
