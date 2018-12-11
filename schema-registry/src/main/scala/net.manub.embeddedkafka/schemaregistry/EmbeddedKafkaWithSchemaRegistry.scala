package net.manub.embeddedkafka.schemaregistry

import net.manub.embeddedkafka.ops.{EmbeddedKafkaOps, RunningEmbeddedKafkaOps}
import net.manub.embeddedkafka.schemaregistry.ops.{
  RunningSchemaRegistryOps,
  SchemaRegistryOps
}
import net.manub.embeddedkafka.{
  EmbeddedKafka,
  EmbeddedKafkaSupport,
  EmbeddedServer,
  EmbeddedZ
}

import scala.reflect.io.Directory

trait EmbeddedKafkaWithSchemaRegistry
    extends EmbeddedKafkaSupport[EmbeddedKafkaConfigWithSchemaRegistry]
    with EmbeddedKafkaOps[EmbeddedKafkaConfigWithSchemaRegistry,
                          EmbeddedKWithSR]
    with SchemaRegistryOps {

  override private[embeddedkafka] def baseConsumerConfig(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, Object] =
    defaultConsumerConfig ++ consumerConfigForSchemaRegistry ++ config.customConsumerProperties

  override private[embeddedkafka] def baseProducerConfig(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, Object] =
    defaultProducerConf ++ configForSchemaRegistry ++ config.customProducerProperties

  override private[embeddedkafka] def withRunningServers[T](
      config: EmbeddedKafkaConfigWithSchemaRegistry,
      actualZkPort: Int,
      kafkaLogsDir: Directory)(
      body: EmbeddedKafkaConfigWithSchemaRegistry => T): T = {
    val broker =
      startKafka(config.kafkaPort,
                 actualZkPort,
                 config.customBrokerProperties,
                 kafkaLogsDir)
    val restApp = startSchemaRegistry(
      config.schemaRegistryPort,
      actualZkPort
    )
    val actualSchemaRegistryPort = restApp.restServer.getURI.getPort

    val configWithUsedPorts = EmbeddedKafkaConfigWithSchemaRegistry(
      EmbeddedKafka.kafkaPort(broker),
      actualZkPort,
      actualSchemaRegistryPort,
      config.customBrokerProperties,
      config.customProducerProperties,
      config.customConsumerProperties
    )

    try {
      body(configWithUsedPorts)
    } finally {
      restApp.stop()
      broker.shutdown()
      broker.awaitShutdown()
    }
  }

}

object EmbeddedKafkaWithSchemaRegistry
    extends EmbeddedKafkaWithSchemaRegistry
    with RunningEmbeddedKafkaOps[EmbeddedKafkaConfigWithSchemaRegistry,
                                 EmbeddedKWithSR]
    with RunningSchemaRegistryOps {

  override def start()(implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : EmbeddedKWithSR = {
    val zkLogsDir = Directory.makeTemp("zookeeper-logs")
    val kafkaLogsDir = Directory.makeTemp("kafka-logs")

    val factory =
      EmbeddedZ(startZooKeeper(config.zooKeeperPort, zkLogsDir), zkLogsDir)

    val configWithUsedZooKeeperPort = EmbeddedKafkaConfigWithSchemaRegistryImpl(
      config.kafkaPort,
      zookeeperPort(factory),
      config.schemaRegistryPort,
      config.customBrokerProperties,
      config.customProducerProperties,
      config.customConsumerProperties
    )

    val kafkaBroker = startKafka(configWithUsedZooKeeperPort, kafkaLogsDir)

    val restApp = EmbeddedSR(
      startSchemaRegistry(
        configWithUsedZooKeeperPort.schemaRegistryPort,
        configWithUsedZooKeeperPort.zooKeeperPort
      ))

    val server = EmbeddedKWithSR(
      factory = Option(factory),
      broker = kafkaBroker,
      app = restApp,
      logsDirs = kafkaLogsDir
    )

    runningServers.add(server)
    server
  }

  override def isRunning: Boolean =
    runningServers.list
      .toFilteredSeq[EmbeddedKWithSR](
        s =>
          // Need to consider both independently-started Schema Registry and
          // all-in-one Kafka with SR
          isEmbeddedKWithSR(s) || isEmbeddedSR(s))
      .nonEmpty

  private[embeddedkafka] def isEmbeddedKWithSR(
      server: EmbeddedServer): Boolean =
    server.isInstanceOf[EmbeddedKWithSR]

}
