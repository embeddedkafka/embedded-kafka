package net.manub.embeddedkafka.schemaregistry

import io.confluent.kafka.schemaregistry.RestApp
import kafka.server.KafkaServer
import net.manub.embeddedkafka.{
  EmbeddedServer,
  EmbeddedServerWithKafka,
  EmbeddedZ
}

import scala.reflect.io.Directory

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

case class EmbeddedKWithSR(
    factory: Option[EmbeddedZ],
    broker: KafkaServer,
    app: EmbeddedSR,
    logsDirs: Directory)(implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    extends EmbeddedServerWithKafka {

  override def stop(clearLogs: Boolean): Unit = {
    app.stop()

    broker.shutdown()
    broker.awaitShutdown()

    factory.foreach(_.stop(clearLogs))

    if (clearLogs) logsDirs.deleteRecursively()
  }
}
