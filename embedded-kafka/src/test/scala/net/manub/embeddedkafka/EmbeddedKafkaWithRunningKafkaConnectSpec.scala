package net.manub.embeddedkafka

import net.manub.embeddedkafka.EmbeddedKafka._
import net.manub.embeddedkafka.EmbeddedKafkaConfig.{
  defaultKafkaPort,
  defaultZookeeperPort,
  defaultConnectPort
}
import net.manub.embeddedkafka.EmbeddedKafkaSpecSupport._
import org.scalatest.exceptions.TestFailedException

class EmbeddedKafkaWithRunningKafkaConnectSpec
    extends EmbeddedKafkaSpecSupport {
  "the withRunningKafkaConnect method" should {
    "start a Kafka broker on port 6001 by default" in {
      withRunningKafkaConnect {
        expectedServerStatus(defaultKafkaPort, Available)
      }
    }

    "start a ZooKeeper instance on port 6000 by default" in {
      withRunningKafkaConnect {
        expectedServerStatus(defaultZookeeperPort, Available)
      }
    }

    "start a Connect instance on port 6002 by default" in {
      withRunningKafkaConnect {
        expectedServerStatus(defaultConnectPort, Available)
      }
    }

    "stop Kafka, Connect and Zookeeper successfully" when {
      "the enclosed test passes" in {
        withRunningKafkaConnect {
          true shouldBe true
        }

        expectedServerStatus(defaultKafkaPort, NotAvailable)
        expectedServerStatus(defaultConnectPort, NotAvailable)
        expectedServerStatus(defaultZookeeperPort, NotAvailable)
      }

      "the enclosed test fails" in {
        a[TestFailedException] shouldBe thrownBy {
          withRunningKafkaConnect {
            true shouldBe false
          }
        }

        expectedServerStatus(defaultKafkaPort, NotAvailable)
        expectedServerStatus(defaultConnectPort, NotAvailable)
        expectedServerStatus(defaultZookeeperPort, NotAvailable)
      }
    }

    "start a Kafka broker on a specified port" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = 12345)

      withRunningKafkaConnect {
        expectedServerStatus(12345, Available)
      }
    }

    "start a Zookeeper server on a specified port" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(zooKeeperPort = 12345)

      withRunningKafkaConnect {
        expectedServerStatus(12345, Available)
      }
    }

    "start a Connect server on a specified port" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(connectPort = 12345)

      withRunningKafkaConnect {
        expectedServerStatus(12345, Available)
      }
    }
  }
}
