package net.manub.embeddedkafka

import java.net.InetSocketAddress
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.scalatest.Suite
import scala.collection.JavaConversions.mapAsJavaMap

import scala.reflect.io.Directory

trait EmbeddedKafka {

  this: Suite =>

  /**
   * Starts a ZooKeeper instance and a Kafka broker, then executes the body passed as a parameter.
   *
   * @param body the function to execute
   * @param config an implicit {@see EmbeddedKafkaConfig}
   */
  def withRunningKafka(body: => Unit)(implicit config: EmbeddedKafkaConfig) = {

    val factory = startZooKeeper(config.zooKeeperPort)
    val broker = startKafka(config)

    try {
      body
    } finally {
      broker.shutdown()
      factory.shutdown()
    }
  }

  /**
   * Publishes asynchronously a message to the running Kafka broker.
   *
   * @param topic the topic to which publish the message (it will be auto-created)
   * @param message the message to publish
   * @param config an implicit {@see EmbeddedKafkaConfig}
   */
  def publishToKafka(topic: String, message: String)(implicit config: EmbeddedKafkaConfig) = {

    val producerProps = Map(
      "bootstrap.servers" -> s"localhost:${config.kafkaPort}",
      "key.serializer" -> classOf[StringSerializer].getName,
      "value.serializer" -> classOf[StringSerializer].getName
    )

    val kafkaProducer = new KafkaProducer[String, String](producerProps)

    kafkaProducer.send(new ProducerRecord[String, String](topic, message))
    kafkaProducer.close()
  }

  private def startZooKeeper(zooKeeperPort: Int): ServerCnxnFactory = {
    val zkLogsDir = Directory.makeTemp("zookeeper-logs")
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile, zkLogsDir.toFile.jfile, tickTime)

    val factory = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("localhost", zooKeeperPort), 1024)
    factory.startup(zkServer)
    factory
  }

  private def startKafka(config: EmbeddedKafkaConfig): KafkaServer = {
    val kafkaLogDir = Directory.makeTemp("kafka")

    val zkAddress = s"localhost:${config.zooKeeperPort}"

    val properties: Properties = new Properties
    properties.setProperty("zookeeper.connect", zkAddress)
    properties.setProperty("broker.id", "0")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("port", config.kafkaPort.toString)
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)

    val broker = new KafkaServer(new KafkaConfig(properties))
    broker.startup()
    broker
  }
}
