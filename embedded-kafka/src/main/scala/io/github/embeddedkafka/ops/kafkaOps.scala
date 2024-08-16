package io.github.embeddedkafka.ops

import java.nio.file.Path
import kafka.server.{KafkaConfig, KafkaServer}
import io.github.embeddedkafka.{
  EmbeddedK,
  EmbeddedKafkaConfig,
  EmbeddedServer,
  EmbeddedZ
}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.config.{
  ServerConfigs,
  ServerLogConfigs,
  ZkConfigs
}
import org.apache.kafka.storage.internals.log.CleanerConfig

import scala.jdk.CollectionConverters._

/**
  * Trait for Kafka-related actions.
  */
trait KafkaOps {
  protected val brokerId: Short                 = 0
  protected val autoCreateTopics: Boolean       = true
  protected val logCleanerDedupeBufferSize: Int = 1048577

  private[embeddedkafka] def startKafka(
      kafkaPort: Int,
      zooKeeperPort: Int,
      customBrokerProperties: Map[String, String],
      kafkaLogDir: Path
  ) = {
    val zkAddress = s"localhost:$zooKeeperPort"
    val listener  = s"${SecurityProtocol.PLAINTEXT}://localhost:$kafkaPort"

    val brokerProperties = Map[String, Object](
      ZkConfigs.ZK_CONNECT_CONFIG                     -> zkAddress,
      ServerConfigs.BROKER_ID_CONFIG                  -> brokerId.toString,
      SocketServerConfigs.LISTENERS_CONFIG            -> listener,
      SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG -> listener,
      ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG -> autoCreateTopics.toString,
      ServerLogConfigs.LOG_DIRS_CONFIG -> kafkaLogDir.toAbsolutePath.toString,
      ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG -> 1.toString,
      GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG -> 1.toString,
      GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG -> 1.toString,
      TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG -> 1.toString,
      TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG -> 1.toString,
      // The total memory used for log deduplication across all cleaner threads, keep it small to not exhaust suite memory
      CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP -> logCleanerDedupeBufferSize.toString
    ) ++ customBrokerProperties

    val broker = new KafkaServer(new KafkaConfig(brokerProperties.asJava))
    broker.startup()
    broker
  }

}

/**
  * [[KafkaOps]] extension relying on `RunningServersOps` for keeping track of
  * running [[EmbeddedK]] instances.
  */
trait RunningKafkaOps {
  this: KafkaOps with RunningServersOps =>

  /**
    * Starts a Kafka broker in memory, storing logs in a specific location.
    *
    * @param kafkaLogsDir
    *   the path for the Kafka logs
    * @param factory
    *   an [[EmbeddedZ]] server
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    * @return
    *   an [[EmbeddedK]] server
    */
  def startKafka(kafkaLogsDir: Path, factory: Option[EmbeddedZ] = None)(
      implicit config: EmbeddedKafkaConfig
  ): EmbeddedK = {
    val kafkaServer = startKafka(
      config.kafkaPort,
      config.zooKeeperPort,
      config.customBrokerProperties,
      kafkaLogsDir
    )

    val configWithUsedPorts = EmbeddedKafkaConfig(
      kafkaPort(kafkaServer),
      config.zooKeeperPort,
      config.customBrokerProperties,
      config.customProducerProperties,
      config.customConsumerProperties
    )

    val broker =
      EmbeddedK(factory, kafkaServer, kafkaLogsDir, configWithUsedPorts)
    runningServers.add(broker)
    broker
  }

  /**
    * Stops all in memory Kafka instances, preserving the logs directories.
    */
  def stopKafka(): Unit =
    runningServers.stopAndRemove(isEmbeddedK, clearLogs = false)

  private[embeddedkafka] def isEmbeddedK(server: EmbeddedServer): Boolean =
    server.isInstanceOf[EmbeddedK]

  private[embeddedkafka] def kafkaPort(kafkaServer: KafkaServer): Int =
    kafkaServer.boundPort(kafkaServer.config.listeners.head.listenerName)
}
