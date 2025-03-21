package io.github.embeddedkafka

import java.nio.file.{Files, Path}

import io.github.embeddedkafka.ops._

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
      kafkaLogsDir: Path
  )(body: EmbeddedKafkaConfig => T): T = {
    val (broker, controller) =
      startKafka(
        config.kafkaPort,
        config.controllerPort,
        config.customBrokerProperties,
        kafkaLogsDir
      )

    val configWithUsedPorts = EmbeddedKafkaConfig(
      EmbeddedKafka.kafkaPort(broker),
      EmbeddedKafka.controllerPort(controller),
      config.customBrokerProperties,
      config.customProducerProperties,
      config.customConsumerProperties
    )

    try {
      body(configWithUsedPorts)
    } finally {
      // In combined mode, we want to shut down the broker first, since the controller may be
      // needed for controlled shutdown. Additionally, the controller shutdown process currently
      // stops the raft client early on, which would disrupt broker shutdown.
      broker.shutdown()
      controller.shutdown()
      broker.awaitShutdown()
      controller.awaitShutdown()
    }
  }
}

object EmbeddedKafka
    extends EmbeddedKafka
    with RunningEmbeddedKafkaOps[EmbeddedKafkaConfig, EmbeddedK] {
  override def start()(implicit config: EmbeddedKafkaConfig): EmbeddedK = {
    val kafkaLogsDir = Files.createTempDirectory("kafka-logs")
    startKafka(kafkaLogsDir)(config)
  }

  override def isRunning: Boolean =
    runningServers.list
      .toFilteredSeq[EmbeddedK](isEmbeddedK)
      .nonEmpty
}

private[embeddedkafka] trait EmbeddedKafkaSupport[C <: EmbeddedKafkaConfig] {
  this: KafkaOps =>

  /**
    * Starts a Kafka broker (and performs additional logic, if any), then
    * executes the body passed as a parameter.
    *
    * @param config
    *   the user-defined [[EmbeddedKafkaConfig]]
    * @param kafkaLogsDir
    *   the path for the Kafka logs
    * @param body
    *   the function to execute
    */
  private[embeddedkafka] def withRunningServers[T](
      config: C,
      kafkaLogsDir: Path
  )(body: C => T): T

  /**
    * Starts a Kafka broker and controller (and performs additional logic, if
    * any), then executes the body passed as a parameter.
    *
    * @param body
    *   the function to execute
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    */
  def withRunningKafka[T](body: => T)(implicit config: C): T = {
    withTempDir("kafka") { kafkaLogsDir =>
      withRunningServers(config, kafkaLogsDir)(_ => body)
    }
  }

  /**
    * Starts a Kafka broker and controller (and performs additional logic, if
    * any), then executes the body passed as a parameter. The actual ports of
    * the servers will be detected and inserted into a copied version of the
    * [[EmbeddedKafkaConfig]] that gets passed to body. This is useful if you
    * set any port to `0`, which will listen on an arbitrary available port.
    *
    * @param config
    *   the user-defined [[EmbeddedKafkaConfig]]
    * @param body
    *   the function to execute, given an [[EmbeddedKafkaConfig]] with the
    *   actual ports the servers are running on
    */
  def withRunningKafkaOnFoundPort[T](config: C)(body: C => T): T = {
    withTempDir("kafka") { kafkaLogsDir =>
      withRunningServers(config, kafkaLogsDir)(body)
    }
  }

  private[embeddedkafka] def withTempDir[T](
      prefix: String
  )(body: Path => T): T = {
    val dir = Files.createTempDirectory(prefix)
    try {
      body(dir)
    } finally {
      val _ = Directory(dir.toFile).deleteRecursively()
    }
  }
}
