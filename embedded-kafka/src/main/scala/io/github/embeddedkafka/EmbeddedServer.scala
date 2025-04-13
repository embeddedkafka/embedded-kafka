package io.github.embeddedkafka

import kafka.server.{BrokerServer, ControllerServer}

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
  def controller: ControllerServer
  def logsDirs: Path
}

/**
  * An instance of an embedded Kafka server.
  *
  * @param broker
  *   the Kafka broker server.
  * @param controller
  *   the Kafka controller server.
  * @param logsDirs
  *   the directory logs are to be written to.
  * @param config
  *   the [[EmbeddedKafkaConfig]] used to start the broker.
  */
case class EmbeddedK(
    broker: BrokerServer,
    controller: ControllerServer,
    logsDirs: Path,
    config: EmbeddedKafkaConfig
) extends EmbeddedServerWithKafka {

  /**
    * Shuts down the broker and controller, and the app, if defined. Optionally
    * deletes the log directory.
    *
    * @param clearLogs
    *   pass `true` to recursively delete the log directory.
    */
  override def stop(clearLogs: Boolean): Unit = {
    // In combined mode, we want to shut down the broker first, since the controller may be
    // needed for controlled shutdown. Additionally, the controller shutdown process currently
    // stops the raft client early on, which would disrupt broker shutdown.
    broker.shutdown()
    controller.shutdown()
    broker.awaitShutdown()
    controller.awaitShutdown()

    if (clearLogs) {
      val _ = Directory(logsDirs.toFile).deleteRecursively()
    }
  }
}
