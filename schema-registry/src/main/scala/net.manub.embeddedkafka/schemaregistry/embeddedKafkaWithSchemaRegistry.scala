package net.manub.embeddedkafka.schemaregistry

import java.util.Properties

import io.confluent.kafka.schemaregistry.RestApp
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import kafka.server.KafkaServer
import net.manub.embeddedkafka.EmbeddedKafka.servers
import net.manub.embeddedkafka.{
  EmbeddedKafka,
  EmbeddedKafkaConfig,
  EmbeddedKafkaSupport,
  EmbeddedServer
}
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy}
import org.apache.kafka.clients.producer.ProducerConfig

trait EmbeddedKafkaWithSchemaRegistry
    extends EmbeddedKafkaWithSchemaRegistrySupport {

  override def baseProducerConfig(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, Object] = EmbeddedKafkaWithSchemaRegistry.baseProducerConfig

  override def baseConsumerConfig(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, Object] = EmbeddedKafkaWithSchemaRegistry.baseConsumerConfig
}

sealed trait EmbeddedKafkaWithSchemaRegistrySupport
    extends EmbeddedKafkaSupport[EmbeddedKafkaConfigWithSchemaRegistry] {

  /**
    * Starts a ZooKeeper instance, a Kafka broker and a Schema Registry app, then executes the body passed as a parameter.
    *
    * @param body   the function to execute
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def withRunningKafka[T](body: => T)(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry): T = {
    withRunningZooKeeper(config.zooKeeperPort) { zkPort =>
      withTempDir("kafka") { kafkaLogsDir =>
        val broker =
          startKafka(config.kafkaPort,
                     zkPort,
                     config.customBrokerProperties,
                     kafkaLogsDir)
        val app =
          startSchemaRegistry(config.schemaRegistryPort, zkPort)

        try {
          body
        } finally {
          app.stop()
          broker.shutdown()
          broker.awaitShutdown()
        }
      }
    }
  }

  /**
    * Starts a ZooKeeper instance, a Kafka broker, and optionally a Schema Registry app, then executes the body passed as a parameter.
    * The actual ZooKeeper, Kafka, and Schema Registry ports will be detected and inserted into a copied version of
    * the EmbeddedKafkaConfig that gets passed to body. This is useful if you set any port to 0, which will listen on an arbitrary available port.
    *
    * @param config the user-defined [[EmbeddedKafkaConfig]]
    * @param body   the function to execute, given an [[EmbeddedKafkaConfig]] with the actual
    *               ports Kafka, ZooKeeper, and Schema Registry are running on
    */
  def withRunningKafkaOnFoundPort[T](
      config: EmbeddedKafkaConfigWithSchemaRegistry)(
      body: EmbeddedKafkaConfigWithSchemaRegistry => T): T = {
    withRunningZooKeeper(config.zooKeeperPort) { zkPort =>
      withTempDir("kafka") { kafkaLogsDir =>
        val broker: KafkaServer =
          startKafka(config.kafkaPort,
                     zkPort,
                     config.customBrokerProperties,
                     kafkaLogsDir)
        val kafkaPort =
          broker.boundPort(broker.config.listeners.head.listenerName)
        // Optionally start Schema Registry
        val app =
          startSchemaRegistry(config.schemaRegistryPort, zkPort)
        val schemaRegistryPort = app.restServer.getURI.getPort

        val actualConfig =
          EmbeddedKafkaConfigWithSchemaRegistryImpl(
            kafkaPort,
            zkPort,
            schemaRegistryPort,
            config.customBrokerProperties,
            config.customProducerProperties,
            config.customConsumerProperties)
        try {
          body(actualConfig)
        } finally {
          app.stop()
          broker.shutdown()
          broker.awaitShutdown()
        }
      }
    }
  }

  // Make sure to start Schema Registry after Kafka and ZooKeeper
  protected def startSchemaRegistry(
      schemaRegistryPort: Int,
      zooKeeperPort: Int,
      avroCompatibilityLevel: AvroCompatibilityLevel =
        AvroCompatibilityLevel.NONE,
      properties: Properties = new Properties): RestApp = {
    val server = new RestApp(schemaRegistryPort,
                             s"localhost:$zooKeeperPort",
                             "_schemas",
                             avroCompatibilityLevel.name,
                             properties)
    server.start()
    server
  }

}

object EmbeddedKafkaWithSchemaRegistry
    extends EmbeddedKafkaWithSchemaRegistrySupport {

  override def baseProducerConfig(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, Object] =
    Map[String, Object](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ProducerConfig.MAX_BLOCK_MS_CONFIG -> 10000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
    ) ++ config.customProducerProperties ++ configForSchemaRegistry

  override def baseConsumerConfig(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, Object] =
    Map[String, Object](
      ConsumerConfig.GROUP_ID_CONFIG -> "embedded-kafka-spec",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> OffsetResetStrategy.EARLIEST.toString.toLowerCase,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> false.toString
    ) ++ config.customConsumerProperties ++ consumerConfigForSchemaRegistry

  /**
    * Starts a ZooKeeper instance and a Kafka broker in memory, using temporary directories for storing logs.
    * Also starts a Schema Registry app.
    * The log directories will be cleaned after calling the [[stop()]] method or on JVM exit, whichever happens earlier.
    *
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def start()(implicit config: EmbeddedKafkaConfigWithSchemaRegistry)
    : EmbeddedKWithSR = {

    val embeddedK = EmbeddedKafka.start()

    val restApp =
      EmbeddedSR(
        startSchemaRegistry(config.schemaRegistryPort, config.zooKeeperPort))

    EmbeddedKWithSR(
      factory = embeddedK.factory,
      broker = embeddedK.broker,
      app = restApp,
      logsDirs = embeddedK.logsDirs
    )
  }

  /**
    * Stops all in memory ZooKeeper instances, Kafka brokers, Schema Registry apps, and deletes the log directories.
    */
  def stop(): Unit = {
    servers.foreach(_.stop(true))
    servers = Seq.empty
  }

  /**
    * Stops a specific [[EmbeddedServer]] instance, and deletes the log directory.
    *
    * @param server the [[EmbeddedServer]] to be stopped.
    */
  def stop(server: EmbeddedServer): Unit = {
    server.stop(true)
    servers = servers.filter(x => x != server)
  }

  /**
    * Stops all in memory Schema Registry instances.
    */
  def stopSchemaRegistry(): Unit = {
    val apps = servers.toFilteredSeq[EmbeddedSR](isEmbeddedSR)

    apps.foreach(_.stop())

    servers = servers.filter(!apps.contains(_))
  }

  private def isEmbeddedSR(server: EmbeddedServer): Boolean =
    server.isInstanceOf[EmbeddedSR]

}
