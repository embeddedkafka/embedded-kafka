package net.manub.embeddedkafka.schemaregistry

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaSpecSupport}

class EmbeddedKafkaWithSchemaRegistryObjectSpec
    extends EmbeddedKafkaSpecSupport {

  "EmbeddedKafkaWithSchemaRegistry" should {
    "start and stop a specific Kafka along with Schema Registry" in {
      val firstBroker = EmbeddedKafkaWithSchemaRegistry.start()(
        EmbeddedKafkaConfigWithSchemaRegistry(kafkaPort = 7000,
                                              zooKeeperPort = 7001,
                                              schemaRegistryPort = 7002))
      EmbeddedKafkaWithSchemaRegistry.start()(
        EmbeddedKafkaConfigWithSchemaRegistry(kafkaPort = 8000,
                                              zooKeeperPort = 8001,
                                              schemaRegistryPort = 8002))

      schemaRegistryIsAvailable(7002)
      kafkaIsAvailable(7000)
      zookeeperIsAvailable(7001)

      schemaRegistryIsAvailable(8002)
      kafkaIsAvailable(8000)
      zookeeperIsAvailable(8001)

      EmbeddedKafkaWithSchemaRegistry.stop(firstBroker)

      schemaRegistryIsNotAvailable(7002)
      kafkaIsNotAvailable(7000)
      zookeeperIsNotAvailable(7001)

      schemaRegistryIsAvailable(8002)
      kafkaIsAvailable(8000)
      zookeeperIsAvailable(8001)

      EmbeddedKafkaWithSchemaRegistry.stop()
    }

    "start and stop Kafka, Zookeeper, and Schema Registry on different specified ports using an implicit configuration" in {
      implicit val config: EmbeddedKafkaConfigWithSchemaRegistry =
        EmbeddedKafkaConfigWithSchemaRegistry(kafkaPort = 12345,
                                              zooKeeperPort = 54321,
                                              schemaRegistryPort = 13542)

      EmbeddedKafkaWithSchemaRegistry.start()

      schemaRegistryIsAvailable(13542)
      kafkaIsAvailable(12345)
      zookeeperIsAvailable(54321)

      EmbeddedKafkaWithSchemaRegistry.stop()
    }
  }
}
