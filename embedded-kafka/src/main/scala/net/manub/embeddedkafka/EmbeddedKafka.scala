package net.manub.embeddedkafka

import java.nio.file.{Files, Path}

import net.manub.embeddedkafka.ops._

import scala.reflect.io.Directory

trait EmbeddedKafka
    extends EmbeddedKafkaSupport[EmbeddedKafkaConfig]
    with EmbeddedKafkaOps[EmbeddedKafkaConfig, EmbeddedK] {
  override private[embeddedkafka] def baseConsumerConfig(
      implicit config: EmbeddedKafkaConfig
  ): Map[String, Object] =
    defaultConsumerConfig ++ config.customConsumerProperties

  override private[embeddedkafka] def baseProducerConfig(
      implicit config: EmbeddedKafkaConfig
  ): Map[String, Object] =
    defaultProducerConf ++ config.customProducerProperties

  override private[embeddedkafka] def withRunningServers[T](
      config: EmbeddedKafkaConfig,
      actualZkPort: Int,
      kafkaLogsDir: Path
  )(body: EmbeddedKafkaConfig => T): T = {
    val broker =
      startKafka(
        config.kafkaPort,
        actualZkPort,
        config.customBrokerProperties,
        kafkaLogsDir
      )

    val configWithUsedPorts = EmbeddedKafkaConfig(
      EmbeddedKafka.kafkaPort(broker),
      actualZkPort,
      config.customBrokerProperties,
      config.customProducerProperties,
      config.customConsumerProperties
    )

    try {
      body(configWithUsedPorts)
    } finally {
      broker.shutdown()
      broker.awaitShutdown()
    }
  }
}

object EmbeddedKafka
    extends EmbeddedKafka
    with RunningEmbeddedKafkaOps[EmbeddedKafkaConfig, EmbeddedK] {
  override def start()(implicit config: EmbeddedKafkaConfig): EmbeddedK = {
    val zkLogsDir    = Files.createTempDirectory("zookeeper-logs")
    val kafkaLogsDir = Files.createTempDirectory("kafka-logs")

    val factory =
      EmbeddedZ(startZooKeeper(config.zooKeeperPort, zkLogsDir), zkLogsDir)

    val configWithUsedPorts = EmbeddedKafkaConfig(
      config.kafkaPort,
      zookeeperPort(factory),
      config.customBrokerProperties,
      config.customProducerProperties,
      config.customConsumerProperties
    )

    startKafka(kafkaLogsDir, Option(factory))(configWithUsedPorts)
  }

  override def isRunning: Boolean =
    runningServers.list
      .toFilteredSeq[EmbeddedK](isEmbeddedK)
      .nonEmpty
}

private[embeddedkafka] trait EmbeddedKafkaSupport[C <: EmbeddedKafkaConfig] {
  this: ZooKeeperOps with KafkaOps =>

  /**
    * Starts a Kafka broker (and performs additional logic, if any), then executes the body passed as a parameter.
    *
    * @param config       the user-defined [[EmbeddedKafkaConfig]]
    * @param actualZkPort the actual ZooKeeper port
    * @param kafkaLogsDir the path for the Kafka logs
    * @param body         the function to execute
    */
  private[embeddedkafka] def withRunningServers[T](
      config: C,
      actualZkPort: Int,
      kafkaLogsDir: Path
  )(body: C => T): T

  /**
    * Starts a ZooKeeper instance and a Kafka broker (and performs additional logic, if any),
    * then executes the body passed as a parameter.
    *
    * @param body   the function to execute
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def withRunningKafka[T](body: => T)(implicit config: C): T = {
    withRunningZooKeeper(config.zooKeeperPort) { actualZkPort =>
      withTempDir("kafka") { kafkaLogsDir =>
        withRunningServers(config, actualZkPort, kafkaLogsDir)(_ => body)
      }
    }
  }

  /**
    * Starts a ZooKeeper instance and a Kafka broker (and performs additional logic, if any),
    * then executes the body passed as a parameter.
    * The actual ports of the servers will be detected and inserted into a copied version of
    * the [[EmbeddedKafkaConfig]] that gets passed to body. This is useful if you set any port
    * to 0, which will listen on an arbitrary available port.
    *
    * @param config the user-defined [[EmbeddedKafkaConfig]]
    * @param body   the function to execute, given an [[EmbeddedKafkaConfig]] with the actual
    *               ports the servers are running on
    */
  def withRunningKafkaOnFoundPort[T](config: C)(body: C => T): T = {
    withRunningZooKeeper(config.zooKeeperPort) { actualZkPort =>
      withTempDir("kafka") { kafkaLogsDir =>
        withRunningServers(config, actualZkPort, kafkaLogsDir)(body)
      }
    }
  }

  private[embeddedkafka] def withRunningZooKeeper[T](
      port: Int
  )(body: Int => T): T = {
    withTempDir("zookeeper-logs") { zkLogsDir =>
      val factory = startZooKeeper(port, zkLogsDir)
      try {
        body(factory.getLocalPort)
      } finally {
        factory.shutdown()
      }
    }
  }

  private[embeddedkafka] def withTempDir[T](
      prefix: String
  )(body: Path => T): T = {
    val dir = Files.createTempDirectory(prefix)
    try {
      body(dir)
    } finally {
      Directory(dir.toFile).deleteRecursively
    }
  }
}
