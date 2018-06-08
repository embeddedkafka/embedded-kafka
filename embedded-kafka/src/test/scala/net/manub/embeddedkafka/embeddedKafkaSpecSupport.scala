package net.manub.embeddedkafka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.Tcp.{Connect, Connected}
import akka.io.{IO, Tcp}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.concurrent.{
  Eventually,
  IntegrationPatience,
  JavaFutures,
  ScalaFutures
}
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

abstract class EmbeddedKafkaSpecSupport
    extends TestKit(ActorSystem("embedded-kafka-spec"))
    with WordSpecLike
    with Matchers
    with ImplicitSender
    with BeforeAndAfterAll
    with ScalaFutures
    with JavaFutures
    with Eventually
    with IntegrationPatience {

  implicit val config: PatienceConfig =
    PatienceConfig(Span(2, Seconds), Span(100, Milliseconds))

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 5.seconds)
    super.afterAll()
  }

  def kafkaIsAvailable(kafkaPort: Int = 6001): Unit = {
    system.actorOf(
      TcpClient.props(new InetSocketAddress("localhost", kafkaPort), testActor))
    expectMsg(1 second, Connection.Success)
  }

  def schemaRegistryIsAvailable(schemaRegistryPort: Int = 6002): Unit = {
    system.actorOf(
      TcpClient.props(new InetSocketAddress("localhost", schemaRegistryPort),
                      testActor))
    expectMsg(1 second, Connection.Success)
  }

  def zookeeperIsAvailable(zookeeperPort: Int = 6000): Unit = {
    system.actorOf(
      TcpClient.props(new InetSocketAddress("localhost", zookeeperPort),
                      testActor))
    expectMsg(1 second, Connection.Success)
  }

  def kafkaIsNotAvailable(kafkaPort: Int = 6001): Unit = {
    system.actorOf(
      TcpClient.props(new InetSocketAddress("localhost", kafkaPort), testActor))
    expectMsg(1 second, Connection.Failure)
  }

  def schemaRegistryIsNotAvailable(schemaRegistryPort: Int = 6002): Unit = {
    system.actorOf(
      TcpClient.props(new InetSocketAddress("localhost", schemaRegistryPort),
                      testActor))
    expectMsg(1 second, Connection.Failure)
  }

  def zookeeperIsNotAvailable(zookeeperPort: Int = 6000): Unit = {
    system.actorOf(
      TcpClient.props(new InetSocketAddress("localhost", zookeeperPort),
                      testActor))
    expectMsg(1 second, Connection.Failure)
  }
}

object TcpClient {
  def props(remote: InetSocketAddress, replies: ActorRef) =
    Props(new TcpClient(remote, replies))
}

object Connection {
  object Success
  object Failure
}

class TcpClient(remote: InetSocketAddress, listener: ActorRef) extends Actor {

  import context.system

  IO(Tcp) ! Connect(remote)

  def receive: Receive = {
    case Connected(_, _) =>
      listener ! Connection.Success
      context stop self

    case _ =>
      listener ! Connection.Failure
      context stop self
  }
}
