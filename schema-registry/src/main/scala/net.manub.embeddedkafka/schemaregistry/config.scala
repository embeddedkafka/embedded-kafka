package net.manub.embeddedkafka.schemaregistry

import net.manub.embeddedkafka.EmbeddedKafkaConfig

trait EmbeddedKafkaConfigWithSchemaRegistry extends EmbeddedKafkaConfig {
  def schemaRegistryPort: Int
}

case class EmbeddedKafkaConfigWithSchemaRegistryImpl(
    kafkaPort: Int,
    zooKeeperPort: Int,
    schemaRegistryPort: Int,
    customBrokerProperties: Map[String, String],
    customProducerProperties: Map[String, String],
    customConsumerProperties: Map[String, String]
) extends EmbeddedKafkaConfigWithSchemaRegistry

object EmbeddedKafkaConfigWithSchemaRegistry {
  implicit val defaultConfig: EmbeddedKafkaConfig = apply()

  def apply(
      kafkaPort: Int = 6001,
      zooKeeperPort: Int = 6000,
      schemaRegistryPort: Int = 6002,
      customBrokerProperties: Map[String, String] = Map.empty,
      customProducerProperties: Map[String, String] = Map.empty,
      customConsumerProperties: Map[String, String] = Map.empty
  ): EmbeddedKafkaConfigWithSchemaRegistry =
    EmbeddedKafkaConfigWithSchemaRegistryImpl(kafkaPort,
                                              zooKeeperPort,
                                              schemaRegistryPort,
                                              customBrokerProperties,
                                              customProducerProperties,
                                              customConsumerProperties)
}
