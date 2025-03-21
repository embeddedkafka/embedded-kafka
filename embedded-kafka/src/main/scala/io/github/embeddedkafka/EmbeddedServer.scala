package io.github.embeddedkafka

import kafka.server.BrokerServer

import java.nio.file.Path
import scala.reflect.io.Directory

/**
  * Represents a running server with a method of stopping the instance.
  */
private[embeddedkafka] trait EmbeddedServer {
  def stop(clearLogs: Boolean): Unit
}

private[embeddedkafka] trait EmbeddedServerWithKafka extends EmbeddedServer {
  def broker: BrokerServer
  def logsDirs: Path
}

/**
  * An instance of an embedded Kafka server.
  *
  * @param broker
  *   the Kafka server.
  * @param logsDirs
  *   the directory logs are to be written to.
  * @param config
  *   the [[EmbeddedKafkaConfig]] used to start the broker.
  */
case class EmbeddedK(
    broker: BrokerServer,
    logsDirs: Path,
    config: EmbeddedKafkaConfig
) extends EmbeddedServerWithKafka {

  /**
    * Shuts down the broker and the app, if defined. Optionally deletes the log
    * directory.
    *
    * @param clearLogs
    *   pass `true` to recursively delete the log directory.
    */
  override def stop(clearLogs: Boolean): Unit = {
    broker.shutdown()
    broker.awaitShutdown()

    if (clearLogs) {
      val _ = Directory(logsDirs.toFile).deleteRecursively()
    }
  }
}
