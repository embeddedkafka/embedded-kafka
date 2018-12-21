package net.manub.embeddedkafka

import org.scalatest.exceptions.TestFailedException

class EmbeddedKafkaWithRunningKafkaSpec
    extends EmbeddedKafkaSpecSupport
    with EmbeddedKafka {

  "the withRunningKafka method" should {
    "start a Kafka broker on port 6001 by default" in {
      withRunningKafka {
        kafkaIsAvailable()
      }
    }

    "start a ZooKeeper instance on port 6000 by default" in {
      withRunningKafka {
        zookeeperIsAvailable()
      }
    }

    "stop Kafka and Zookeeper successfully" when {
      "the enclosed test passes" in {
        withRunningKafka {
          true shouldBe true
        }

        kafkaIsNotAvailable()
        zookeeperIsNotAvailable()
      }

      "the enclosed test fails" in {
        a[TestFailedException] shouldBe thrownBy {
          withRunningKafka {
            true shouldBe false
          }
        }

        kafkaIsNotAvailable()
        zookeeperIsNotAvailable()
      }
    }

    "start a Kafka broker on a specified port" in {
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 12345)

      withRunningKafka {
        kafkaIsAvailable(12345)
      }
    }

    "start a Zookeeper server on a specified port" in {
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(zooKeeperPort = 12345)

      withRunningKafka {
        zookeeperIsAvailable(12345)
      }
    }

  }
}
