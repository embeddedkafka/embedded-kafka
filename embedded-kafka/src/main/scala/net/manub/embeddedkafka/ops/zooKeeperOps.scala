package net.manub.embeddedkafka.ops

import java.net.InetSocketAddress

import net.manub.embeddedkafka.{EmbeddedKafkaConfig, EmbeddedServer, EmbeddedZ}
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}

import scala.reflect.io.Directory

/**
  * Trait for ZooKeeper-related actions.
  */
trait ZooKeeperOps {

  def startZooKeeper(
      zooKeeperPort: Int,
      zkLogsDir: Directory
  ): ServerCnxnFactory = {
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(
      zkLogsDir.toFile.jfile,
      zkLogsDir.toFile.jfile,
      tickTime
    )

    val factory = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("localhost", zooKeeperPort), 1024)
    factory.startup(zkServer)
    factory
  }

}

/**
  * [[ZooKeeperOps]] extension relying on [[RunningServersOps]] for
  * keeping track of running [[EmbeddedZ]] instances.
  *
  * @see [[RunningServersOps]]
  */
trait RunningZooKeeperOps {
  this: ZooKeeperOps with RunningServersOps =>

  /**
    * Starts a Zookeeper instance in memory, storing logs in a specific location.
    *
    * @param zkLogsDir the path for the Zookeeper logs
    * @param config    an implicit [[EmbeddedKafkaConfig]]
    * @return          an [[EmbeddedZ]] server
    */
  def startZooKeeper(
      zkLogsDir: Directory
  )(implicit config: EmbeddedKafkaConfig): EmbeddedZ = {
    val factory =
      EmbeddedZ(startZooKeeper(config.zooKeeperPort, zkLogsDir), zkLogsDir)
    runningServers.add(factory)
    factory
  }

  /**
    * Stops all in memory Zookeeper instances, preserving the logs directories.
    */
  def stopZooKeeper(): Unit =
    runningServers.stopAndRemove(isEmbeddedZ, clearLogs = false)

  private def isEmbeddedZ(server: EmbeddedServer): Boolean =
    server.isInstanceOf[EmbeddedZ]

  private[embeddedkafka] def zookeeperPort(zk: EmbeddedZ): Int =
    zookeeperPort(zk.factory)
  private def zookeeperPort(fac: ServerCnxnFactory): Int =
    fac.getLocalPort

}
