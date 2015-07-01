package net.manub.embeddedkafka

import java.net.InetSocketAddress
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.scalatest.Suite

import scala.reflect.io.Directory

trait EmbeddedKafka {

  this: Suite =>

  def withRunningKafka(body: => Unit) = {

    val factory = startZooKeeper()
    val broker = startKafka()

    try {
      body
    } finally {
      broker.shutdown()
      factory.shutdown()
    }
  }

  def startKafka(): KafkaServerStartable = {
    val kafkaLogDir = Directory.makeTemp("kafka")

    val zkAddress = "localhost:6000"
    val brokerPort = 6001

    val properties: Properties = new Properties
    properties.setProperty("zookeeper.connect", zkAddress)
    properties.setProperty("broker.id", "1")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("port", Integer.toString(brokerPort))
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", String.valueOf(1))

    val broker = new KafkaServerStartable(new KafkaConfig(properties))
    broker.startup()
    broker
  }

  def startZooKeeper(): ServerCnxnFactory = {
    val zkSnapshotDir = Directory.makeTemp("zookeeper-snapshots")
    val zkLogDir = Directory.makeTemp("zookeeper-logs")

    val tickTime = 500

    val zkServer = new ZooKeeperServer(zkSnapshotDir.toFile.jfile, zkLogDir.toFile.jfile, tickTime)

    val factory = ServerCnxnFactory.createFactory()

    factory.configure(new InetSocketAddress("localhost", 6000), 16)
    factory.startup(zkServer)
    factory
  }
}
