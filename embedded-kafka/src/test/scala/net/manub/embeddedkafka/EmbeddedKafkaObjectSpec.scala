package net.manub.embeddedkafka

class EmbeddedKafkaObjectSpec extends EmbeddedKafkaSpecSupport {

  "the EmbeddedKafka object" when {
    "invoking the start and stop methods" should {
      "start and stop Kafka and Zookeeper on the default ports" in {
        EmbeddedKafka.start()

        kafkaIsAvailable()
        zookeeperIsAvailable()

        EmbeddedKafka.stop()

        kafkaIsNotAvailable()
        zookeeperIsNotAvailable()
      }

      "start and stop Kafka and Zookeeper on different specified ports using an implicit configuration" in {
        implicit val config =
          EmbeddedKafkaConfig(kafkaPort = 12345, zooKeeperPort = 54321)
        EmbeddedKafka.start()

        kafkaIsAvailable(12345)
        zookeeperIsAvailable(54321)

        EmbeddedKafka.stop()
      }
    }

    "invoking the isRunnning method" should {
      "return whether both Kafka and Zookeeper are running" in {
        EmbeddedKafka.start()
        EmbeddedKafka.isRunning shouldBe true
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false
      }
    }
  }
}
