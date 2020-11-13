package net.manub.embeddedkafka.ops

import java.nio.file.Path

import org.apache.kafka.common.utils.Time
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy
import org.apache.kafka.connect.runtime.isolation.Plugins
import org.apache.kafka.connect.runtime.rest.RestServer
import org.apache.kafka.connect.runtime.standalone.{
  StandaloneConfig,
  StandaloneHerder
}
import org.apache.kafka.connect.runtime.{Connect, Worker, WorkerConfig}
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore
import org.apache.kafka.connect.util.ConnectUtils

import net.manub.embeddedkafka.{EmbeddedKafkaConfig, EmbeddedServer, EmbeddedC}

import scala.jdk.CollectionConverters._

/**
  * Trait for Connect-related actions.
  */
trait ConnectOps {

  private[embeddedkafka] def startConnect(
      connectPort: Int,
      offsets: Path,
      customConnectProperties: Map[String, String],
      kafkaPort: Int
  ): Connect = {
    val listeners = s"http://localhost:$connectPort"

    val connectProperties = Map[String, String](
      WorkerConfig.LISTENERS_CONFIG                        -> listeners,
      WorkerConfig.BOOTSTRAP_SERVERS_CONFIG                -> s"localhost:$kafkaPort",
      WorkerConfig.KEY_CONVERTER_CLASS_CONFIG              -> "org.apache.kafka.connect.json.JsonConverter",
      WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG            -> "org.apache.kafka.connect.json.JsonConverter",
      WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG        -> "10000",
      StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG -> offsets.toFile.getAbsolutePath
    ) ++ customConnectProperties

    val standaloneConfig = new StandaloneConfig(connectProperties.asJava)
    val rest             = new RestServer(standaloneConfig)
    rest.initializeServer()

    val plugins = new Plugins(connectProperties.asJava)
    plugins.compareAndSwapWithDelegatingLoader
    val connectorClientConfigOverridePolicy = plugins.newPlugin(
      standaloneConfig.getString(
        WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG
      ),
      standaloneConfig,
      classOf[ConnectorClientConfigOverridePolicy]
    )

    val workerId = s"localhost:$connectPort"
    val worker = new Worker(
      workerId,
      Time.SYSTEM,
      plugins,
      standaloneConfig,
      new MemoryOffsetBackingStore,
      connectorClientConfigOverridePolicy
    )
    val clusterId = ConnectUtils.lookupKafkaClusterId(standaloneConfig)
    val herder = new StandaloneHerder(
      worker,
      clusterId,
      connectorClientConfigOverridePolicy
    )

    val connect = new Connect(herder, rest)
    connect.start()
    connect
  }
}

/**
  * [[ConnectOps]] extension relying on `RunningServersOps` for
  * keeping track of running [[EmbeddedC]] instances.
  */
trait RunningConnectOps {
  this: ConnectOps with RunningServersOps =>

  /**
    * Starts a Connect instance in memory.
    *
    * @param config    an implicit [[EmbeddedKafkaConfig]]
    * @return          an [[EmbeddedC]] server
    */
  def startConnect(
      offsets: Path
  )(implicit config: EmbeddedKafkaConfig): EmbeddedC = {
    val connectServer = startConnect(
      config.connectPort,
      offsets,
      config.customConnectProperties,
      config.kafkaPort
    )

    val configWithUsedPorts = EmbeddedKafkaConfig(
      config.kafkaPort,
      config.zooKeeperPort,
      connectPort(connectServer),
      config.customBrokerProperties,
      config.customProducerProperties,
      config.customConsumerProperties,
      config.customConnectProperties
    )

    val connect = EmbeddedC(connectServer, configWithUsedPorts)
    runningServers.add(connect)
    connect
  }

  /**
    * Stops in memory Connect instances.
    */
  def stopConnect(): Unit =
    runningServers.stopAndRemove(isEmbeddedC, clearLogs = false)

  private[embeddedkafka] def isEmbeddedC(server: EmbeddedServer): Boolean =
    server.isInstanceOf[EmbeddedC]

  private[embeddedkafka] def connectPort(connect: Connect): Int =
    connect.restUrl.getPort
}
