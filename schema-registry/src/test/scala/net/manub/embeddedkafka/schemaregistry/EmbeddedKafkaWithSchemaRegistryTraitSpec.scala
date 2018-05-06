package net.manub.embeddedkafka.schemaregistry

import net.manub.embeddedkafka.EmbeddedKafkaSpecSupport

class EmbeddedKafkaWithSchemaRegistryTraitSpec
    extends EmbeddedKafkaSpecSupport
    with EmbeddedKafkaWithSchemaRegistry {

  "the withRunningKafka method" should {
    "start a Schema Registry server on a specified port" in {
      implicit val config: EmbeddedKafkaConfigWithSchemaRegistry =
        EmbeddedKafkaConfigWithSchemaRegistry(schemaRegistryPort = 12345)

      withRunningKafka {
        schemaRegistryIsAvailable(12345)
      }
    }
  }
  "the withRunningKafkaOnFoundPort method" should {

    "start and stop Kafka, Zookeeper, and Schema Registry successfully on non-zero ports" in {
      val userDefinedConfig =
        EmbeddedKafkaConfigWithSchemaRegistry(kafkaPort = 12345,
                                              zooKeeperPort = 12346,
                                              schemaRegistryPort = 12347)
      val actualConfig = withRunningKafkaOnFoundPort(userDefinedConfig) {
        actualConfig =>
          actualConfig shouldBe userDefinedConfig
          everyServerIsAvailable(actualConfig)
          actualConfig
      }
      noServerIsAvailable(actualConfig)
    }
  }

  private def everyServerIsAvailable(
      config: EmbeddedKafkaConfigWithSchemaRegistry): Unit = {
    kafkaIsAvailable(config.kafkaPort)
    schemaRegistryIsAvailable(config.schemaRegistryPort)
    zookeeperIsAvailable(config.zooKeeperPort)
  }

  private def noServerIsAvailable(
      config: EmbeddedKafkaConfigWithSchemaRegistry): Unit = {
    kafkaIsNotAvailable(config.kafkaPort)
    schemaRegistryIsNotAvailable(config.schemaRegistryPort)
    zookeeperIsNotAvailable(config.zooKeeperPort)
  }
}
