package net.manub.embeddedkafka

import io.confluent.kafka.schemaregistry.RestApp
import kafka.server.KafkaServer
import org.apache.zookeeper.server.ServerCnxnFactory

import scala.reflect.io.Directory

/**
  * Represents a running server with a method of stopping the instance.
  */
sealed trait EmbeddedServer {

  def stop(clearLogs: Boolean): Unit
}

/**
  * An instance of an embedded Zookeeper server.
  *
  * @param factory    the server.
  * @param logsDirs   the [[Directory]] logs are to be written to.
  * @param config     the [[EmbeddedKafkaConfig]] used to start the factory.
  */
case class EmbeddedZ(factory: ServerCnxnFactory, logsDirs: Directory)(
    implicit config: EmbeddedKafkaConfig)
    extends EmbeddedServer {

  /**
    * Shuts down the factory and then optionally deletes the log directory.
    *
    * @param clearLogs  pass `true` to recursively delete the log directory.
    */
  override def stop(clearLogs: Boolean) = {
    factory.shutdown()
    if (clearLogs) logsDirs.deleteRecursively()
  }
}

/**
  * An instance of an embedded Kafka server.
  *
  * @param factory         the optional [[EmbeddedZ]] server which Kafka relies upon.
  * @param broker          the Kafka server.
  * @param app             the optional [[EmbeddedSR]] app.
  * @param logsDirs        the [[Directory]] logs are to be written to.
  * @param config          the [[EmbeddedKafkaConfig]] used to start the broker.
  */
case class EmbeddedK(factory: Option[EmbeddedZ],
                     broker: KafkaServer,
                     app: Option[EmbeddedSR],
                     logsDirs: Directory)(implicit config: EmbeddedKafkaConfig)
    extends EmbeddedServer {

  /**
    * Shuts down the broker, the factory it relies upon, if defined, and the app, if defined.
    * Optionally deletes the log directory.
    *
    * @param clearLogs  pass `true` to recursively delete the log directory.
    */
  override def stop(clearLogs: Boolean) = {
    app.foreach(_.stop())

    broker.shutdown()
    broker.awaitShutdown()

    factory.foreach(_.stop(clearLogs))

    if (clearLogs) logsDirs.deleteRecursively()
  }
}

/**
  * An instance of an embedded Schema Registry app.
  *
  * @param app the Schema Registry app.
  */
case class EmbeddedSR(app: RestApp) extends EmbeddedServer {

  /**
    * Shuts down the app.
    */
  override def stop(clearLogs: Boolean = false): Unit = app.stop()
}

object EmbeddedK {
  def apply(broker: KafkaServer, logsDirs: Directory)(
      implicit config: EmbeddedKafkaConfig): EmbeddedK =
    EmbeddedK(factory = None, broker, app = None, logsDirs)
}
