package io.github.embeddedkafka.ops

import io.github.embeddedkafka.{EmbeddedK, EmbeddedKafkaConfig, EmbeddedServer}
import kafka.server._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.config.{
  KRaftConfigs,
  ServerConfigs,
  ServerLogConfigs
}
import org.apache.kafka.server.{ProcessRole, ServerSocketFactory}
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.slf4j.LoggerFactory

import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
  * Trait for Kafka-related actions.
  */
trait KafkaOps {

  private val LOGGER = LoggerFactory.getLogger(getClass)

  protected val brokerId: Short                 = 0
  protected val autoCreateTopics: Boolean       = true
  protected val logCleanerDedupeBufferSize: Int = 1048577

  private[embeddedkafka] def startKafka(
      kafkaPort: Int,
      customBrokerProperties: Map[String, String],
      kafkaLogDir: Path
  ): BrokerServer = {
    val listener = s"${SecurityProtocol.PLAINTEXT}://localhost:$kafkaPort"

    val brokerProperties = Map[String, Object](
      KRaftConfigs.PROCESS_ROLES_CONFIG -> List(
        ProcessRole.BrokerRole,
        ProcessRole.ControllerRole
      ).asJava,
      ServerConfigs.BROKER_ID_CONFIG                  -> brokerId.toString,
      SocketServerConfigs.LISTENERS_CONFIG            -> listener,
      SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG -> listener,
      ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG -> autoCreateTopics.toString,
      ServerLogConfigs.LOG_DIRS_CONFIG -> kafkaLogDir.toAbsolutePath.toString,
      ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG -> 1.toString,
      GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG -> 1.toString,
      GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG -> 1.toString,
      // The total memory used for log deduplication across all cleaner threads, keep it small to not exhaust suite memory
      CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP -> logCleanerDedupeBufferSize.toString
    ) ++ customBrokerProperties

    val config = new KafkaConfig(brokerProperties.asJava)

    val time = Time.SYSTEM

    // Note: the following is copied from KafkaRaftServer because it doesn't expose a way to retrieve the BrokerServer from the KafkaRaftServer instance

    val logIdent = s"[KafkaRaftServer nodeId=${config.nodeId}] "

    val (metaPropsEnsemble, _) =
      KafkaRaftServer.initializeLogDirs(config, LOGGER, logIdent)

    val metrics = Server.initializeMetrics(
      config,
      time,
      metaPropsEnsemble.clusterId().get()
    )

    val sharedServer = new SharedServer(
      config,
      metaPropsEnsemble,
      time,
      metrics,
      CompletableFuture.completedFuture(
        QuorumConfig.parseVoterConnections(config.quorumConfig.voters)
      ),
      QuorumConfig.parseBootstrapServers(config.quorumConfig.bootstrapServers),
      new StandardFaultHandlerFactory(),
      ServerSocketFactory.INSTANCE
    )

    val brokerServer = new BrokerServer(sharedServer)

    brokerServer.startup()
    brokerServer
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
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    * @return
    *   an [[EmbeddedK]] server
    */
  def startKafka(kafkaLogsDir: Path)(
      implicit config: EmbeddedKafkaConfig
  ): EmbeddedK = {
    val kafkaBrokerServer = startKafka(
      config.kafkaPort,
      config.customBrokerProperties,
      kafkaLogsDir
    )

    val configWithUsedPorts = EmbeddedKafkaConfig(
      kafkaPort(kafkaBrokerServer),
      config.customBrokerProperties,
      config.customProducerProperties,
      config.customConsumerProperties
    )

    val broker =
      EmbeddedK(kafkaBrokerServer, kafkaLogsDir, configWithUsedPorts)
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

  private[embeddedkafka] def kafkaPort(kafkaBrokerServer: BrokerServer): Int =
    kafkaBrokerServer.boundPort(
      kafkaBrokerServer.config.listeners.head.listenerName
    )
}
