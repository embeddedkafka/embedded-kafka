package io.github.embeddedkafka.connect

import java.nio.file.Path
import org.apache.kafka.common.utils.Time
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy
import org.apache.kafka.connect.runtime.isolation.Plugins
import org.apache.kafka.connect.runtime.rest.{RestClient, RestServer}
import org.apache.kafka.connect.runtime.standalone.{
  StandaloneConfig,
  StandaloneHerder
}
import org.apache.kafka.connect.runtime.{Connect, Worker, WorkerConfig}
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore
import org.apache.kafka.connect.util.ConnectUtils

import io.github.embeddedkafka.ops.{KafkaOps, ZooKeeperOps}
import io.github.embeddedkafka.{
  EmbeddedKafka,
  EmbeddedKafkaConfig,
  EmbeddedKafkaSupport
}

import scala.jdk.CollectionConverters._

/**
  * Helper trait for running Kafka Connect server. Use `.startConnect` to start
  * the server.
  */
trait EmbeddedKafkaConnect
    extends EmbeddedKafkaConnectSupport[EmbeddedKafkaConfig]
    with EmbeddedKafka {
  override protected[embeddedkafka] val connectConfig =
    new EmbeddedConnectConfigImpl
}

private[embeddedkafka] trait EmbeddedKafkaConnectSupport[
    C <: EmbeddedKafkaConfig
] {
  this: EmbeddedKafkaSupport[C] with ZooKeeperOps with KafkaOps =>

  protected[embeddedkafka] def connectConfig: EmbeddedConnectConfig[C]

  /**
    * Start a Kafka Connect server and pass a block of code that can operate
    * while the server is active.
    *
    * @param connectPort
    *   the Kafka Connect port for the REST API to listen on.
    * @param offsets
    *   the file to store offset data in.
    * @param extraConfig
    *   additional Kafka Connect configuration (overwrite existing keys in
    *   default config)
    * @param block
    *   the code block that will executed while the Kafka Connect server is
    *   active. Once the block has been executed the server will be stopped.
    */
  def startConnect[T](
      connectPort: Int,
      offsets: Path,
      extraConfig: Map[String, String] = Map.empty
  )(block: => T)(implicit config: C): T =
    withRunningKafka {
      val configMap = connectConfig.config(
        connectPort,
        offsets,
        extraConfig
      )
      val standaloneConfig = new StandaloneConfig(configMap.asJava)
      val restClient       = new RestClient(standaloneConfig)
      val rest             = new RestServer(standaloneConfig, restClient)
      rest.initializeServer()

      val plugins = new Plugins(configMap.asJava)
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
      val clusterId = standaloneConfig.kafkaClusterId()
      val herder = new StandaloneHerder(
        worker,
        clusterId,
        connectorClientConfigOverridePolicy
      )

      val connect = new Connect(herder, rest)
      connect.start()
      try {
        block
      } finally {
        connect.stop()
      }
    }(config)

}

object EmbeddedKafkaConnect extends EmbeddedKafkaConnect
