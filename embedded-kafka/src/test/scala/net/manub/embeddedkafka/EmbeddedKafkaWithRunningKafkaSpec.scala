package net.manub.embeddedkafka

import net.manub.embeddedkafka.EmbeddedKafka._
import net.manub.embeddedkafka.EmbeddedKafkaSpecSupport._
import org.scalatest.exceptions.TestFailedException

class EmbeddedKafkaWithRunningKafkaSpec extends EmbeddedKafkaSpecSupport {
  "the withRunningKafka method" should {
    "start a Kafka broker on port 6001 by default" in {
      withRunningKafka {
        expectedServerStatus(defaultKafkaPort, Available)
      }
    }

    "start a ZooKeeper instance on port 6000 by default" in {
      withRunningKafka {
        expectedServerStatus(defaultZookeeperPort, Available)
      }
    }

    "stop Kafka and Zookeeper successfully" when {
      "the enclosed test passes" in {
        withRunningKafka {
          true shouldBe true
        }

        expectedServerStatus(defaultKafkaPort, NotAvailable)
        expectedServerStatus(defaultZookeeperPort, NotAvailable)
      }

      "the enclosed test fails" in {
        a[TestFailedException] shouldBe thrownBy {
          withRunningKafka {
            true shouldBe false
          }
        }

        expectedServerStatus(defaultKafkaPort, NotAvailable)
        expectedServerStatus(defaultZookeeperPort, NotAvailable)
      }
    }

    "start a Kafka broker on a specified port" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = 12345)

      withRunningKafka {
        expectedServerStatus(12345, Available)
      }
    }

    "start a Zookeeper server on a specified port" in {
      implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(zooKeeperPort = 12345)

      withRunningKafka {
        expectedServerStatus(12345, Available)
      }
    }
  }
}
