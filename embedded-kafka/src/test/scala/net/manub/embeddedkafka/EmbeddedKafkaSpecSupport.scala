package net.manub.embeddedkafka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.Tcp.{Connect, Connected}
import akka.io.{IO, Tcp}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.concurrent.{JavaFutures, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps

abstract class EmbeddedKafkaSpecSupport extends TestKit(ActorSystem("embedded-kafka-spec")) with WordSpecLike with Matchers
  with ImplicitSender with BeforeAndAfterAll with ScalaFutures with JavaFutures {

  def kafkaIsAvailable(kafkaPort: Int = 6001): Unit = {
    system.actorOf(TcpClient.props(new InetSocketAddress("localhost", kafkaPort), testActor))
    expectMsg(1 second, ConnectionSuccessful)
  }

  def zookeeperIsAvailable(zookeeperPort: Int = 6000): Unit = {
    system.actorOf(TcpClient.props(new InetSocketAddress("localhost", zookeeperPort), testActor))
    expectMsg(1 second, ConnectionSuccessful)
  }

  def kafkaIsNotAvailable(): Unit = {
    system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 6001), testActor))
    expectMsg(1 second, ConnectionFailed)
  }

  def zookeeperIsNotAvailable(): Unit = {
    system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 6000), testActor))
    expectMsg(1 second, ConnectionFailed)
  }

}

object TcpClient {
  def props(remote: InetSocketAddress, replies: ActorRef) = Props(classOf[TcpClient], remote, replies)
}

case object ConnectionSuccessful

case object ConnectionFailed

class TcpClient(remote: InetSocketAddress, listener: ActorRef) extends Actor {

  import context.system

  IO(Tcp) ! Connect(remote)

  def receive: Receive = {
    case Connected(_, _) =>
      listener ! ConnectionSuccessful
      context stop self

    case _ =>
      listener ! ConnectionFailed
      context stop self
  }
}
