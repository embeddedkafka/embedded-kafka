package io.github.embeddedkafka.ops

import io.github.embeddedkafka.{EmbeddedK, EmbeddedKafkaConfig, EmbeddedServer}
import kafka.server._
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.metadata.properties.{
  MetaProperties,
  MetaPropertiesEnsemble,
  MetaPropertiesVersion,
  PropertiesUtils
}
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.ServerSocketFactory
import org.apache.kafka.server.config.{
  KRaftConfigs,
  ReplicationConfigs,
  ServerConfigs,
  ServerLogConfigs
}
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.slf4j.LoggerFactory

import java.io.{File, IOException}
import java.net.ServerSocket
import java.nio.file.{Path, Paths}
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try, Using}

/**
  * Trait for Kafka-related actions.
  */
trait KafkaOps {

  private val logger = LoggerFactory.getLogger(getClass)

  protected val nodeId: Int                     = 0
  protected val autoCreateTopics: Boolean       = true
  protected val logCleanerDedupeBufferSize: Int = 1048577

  private[embeddedkafka] def startKafka(
      kafkaPort: Int,
      controllerPort: Int,
      customBrokerProperties: Map[String, String],
      kafkaLogDir: Path
  ): (BrokerServer, ControllerServer) = {

    // We need to know the controller port beforehand to set the config for the broker
    // Without this the controller starts correctly on a random port but it's too late to use this port in the configs for the broker
    val actualControllerPort = findPortForControllerOrFail(controllerPort)

    val brokerListener     = s"BROKER://localhost:$kafkaPort"
    val controllerListener = s"CONTROLLER://localhost:$actualControllerPort"

    val configProperties = Map[String, Object](
      KRaftConfigs.PROCESS_ROLES_CONFIG -> "broker,controller",
      KRaftConfigs.NODE_ID_CONFIG       -> nodeId.toString,
      ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG -> "BROKER",
      KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG        -> "CONTROLLER",
      QuorumConfig.QUORUM_VOTERS_CONFIG -> s"$nodeId@localhost:$actualControllerPort",
      ServerConfigs.BROKER_ID_CONFIG -> nodeId.toString,
      SocketServerConfigs.LISTENERS_CONFIG -> s"$brokerListener,$controllerListener",
      SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG -> brokerListener,
      SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG -> "BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
      ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG -> autoCreateTopics.toString,
      ServerLogConfigs.LOG_DIRS_CONFIG -> kafkaLogDir.toAbsolutePath.toString,
      ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG -> 1.toString,
      GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG -> 1.toString,
      GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG -> 1.toString,
      TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG -> 1.toString,
      TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG -> 1.toString,
      // The total memory used for log deduplication across all cleaner threads, keep it small to not exhaust suite memory
      CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP -> logCleanerDedupeBufferSize.toString
    ) ++ customBrokerProperties

    val config = new KafkaConfig(configProperties.asJava)

    val time = Time.SYSTEM

    val clusterIdBase64 = generateRandomClusterId()

    val metaProperties = new MetaProperties.Builder()
      .setVersion(MetaPropertiesVersion.V1)
      .setClusterId(clusterIdBase64)
      .setNodeId(nodeId)
      .build()

    // Note: the following is copied from KafkaRaftServer because it doesn't expose a way to retrieve the BrokerServer from the KafkaRaftServer instance

    val logIdent = s"[KafkaRaftServer nodeId=${config.nodeId}] "

    writeMetaProperties(Paths.get(config.metadataLogDir).toFile, metaProperties)

    val (metaPropsEnsemble, bootstrapMetadata) =
      KafkaRaftServer.initializeLogDirs(config, logger, logIdent)

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

    val broker: BrokerServer = new BrokerServer(sharedServer)

    val controller: ControllerServer = new ControllerServer(
      sharedServer,
      KafkaRaftServer.configSchema,
      bootstrapMetadata
    )

    // Controller component must be started before the broker component so that
    // the controller endpoints are passed to the KRaft manager
    controller.startup()
    broker.startup()

    (broker, controller)
  }

  private def generateRandomClusterId(): String = {
    Uuid.randomUuid().toString
  }

  private def writeMetaProperties(
      logDir: File,
      metaProperties: MetaProperties
  ): Unit = {
    val metaPropertiesFile = new File(
      logDir.getAbsolutePath,
      MetaPropertiesEnsemble.META_PROPERTIES_NAME
    )
    PropertiesUtils.writePropertiesFile(
      metaProperties.toProperties,
      metaPropertiesFile.getAbsolutePath,
      false
    )
  }

  private def findPortForControllerOrFail(controllerPort: Int): Int = {
    if (controllerPort == 0) {
      findRandomFreePort() match {
        case scala.util.Success(port) =>
          logger.info(s"Found free port $port for controller")
          port
        case Failure(exception) =>
          logger.error(
            "Could not find a free port for the controller",
            exception
          )
          throw new RuntimeException(
            s"Could not find a free port for the controller",
            exception
          )
      }
    } else {
      controllerPort
    }
  }

  private def findRandomFreePort(): Try[Int] = {
    Using(new ServerSocket(0))(serverSocket => serverSocket.getLocalPort())
      .recoverWith {
        case ex: IOException =>
          Failure(new RuntimeException("Could not find a free port", ex))
      }
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
    val (brokerServer, controllerServer) = startKafka(
      config.kafkaPort,
      config.controllerPort,
      config.customBrokerProperties,
      kafkaLogsDir
    )

    val configWithUsedPorts = EmbeddedKafkaConfig(
      kafkaPort(brokerServer),
      controllerPort(controllerServer),
      config.customBrokerProperties,
      config.customProducerProperties,
      config.customConsumerProperties
    )

    val servers =
      EmbeddedK(
        brokerServer,
        controllerServer,
        kafkaLogsDir,
        configWithUsedPorts
      )
    runningServers.add(servers)
    servers
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

  private[embeddedkafka] def controllerPort(
      controllerServer: ControllerServer
  ): Int =
    controllerServer.socketServer.boundPort(
      controllerServer.config.controllerListeners.head.listenerName
    )
}
