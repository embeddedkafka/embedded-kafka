package io.github.embeddedkafka

import java.nio.file.Path

import kafka.server.KafkaServer
import org.apache.zookeeper.server.ServerCnxnFactory

import scala.reflect.io.Directory

/**
  * Represents a running server with a method of stopping the instance.
  */
private[embeddedkafka] trait EmbeddedServer {
  def stop(clearLogs: Boolean): Unit
}

/**
  * An instance of an embedded Zookeeper server.
  *
  * @param factory
  *   the server.
  * @param logsDirs
  *   the directory logs are to be written to.
  */
case class EmbeddedZ(
    factory: ServerCnxnFactory,
    logsDirs: Path
) extends EmbeddedServer {

  /**
    * Shuts down the factory and then optionally deletes the log directory.
    *
    * @param clearLogs
    *   pass `true` to recursively delete the log directory.
    */
  override def stop(clearLogs: Boolean): Unit = {
    factory.shutdown()
    if (clearLogs) {
      val _ = Directory(logsDirs.toFile).deleteRecursively()
    }
  }
}

private[embeddedkafka] trait EmbeddedServerWithKafka extends EmbeddedServer {
  def factory: Option[EmbeddedZ]
  def broker: KafkaServer
  def logsDirs: Path
}

/**
  * An instance of an embedded Kafka server.
  *
  * @param factory
  *   the optional [[EmbeddedZ]] server which Kafka relies upon.
  * @param broker
  *   the Kafka server.
  * @param logsDirs
  *   the directory logs are to be written to.
  * @param config
  *   the [[EmbeddedKafkaConfig]] used to start the broker.
  */
case class EmbeddedK(
    factory: Option[EmbeddedZ],
    broker: KafkaServer,
    logsDirs: Path,
    config: EmbeddedKafkaConfig
) extends EmbeddedServerWithKafka {

  /**
    * Shuts down the broker, the factory it relies upon, if defined, and the
    * app, if defined. Optionally deletes the log directory.
    *
    * @param clearLogs
    *   pass `true` to recursively delete the log directory.
    */
  override def stop(clearLogs: Boolean): Unit = {
    broker.shutdown()
    broker.awaitShutdown()

    factory.foreach(_.stop(clearLogs))

    if (clearLogs) {
      val _ = Directory(logsDirs.toFile).deleteRecursively()
    }
  }
}

object EmbeddedK {
  def apply(
      broker: KafkaServer,
      logsDirs: Path,
      config: EmbeddedKafkaConfig
  ): EmbeddedK =
    EmbeddedK(factory = None, broker, logsDirs, config)
}
