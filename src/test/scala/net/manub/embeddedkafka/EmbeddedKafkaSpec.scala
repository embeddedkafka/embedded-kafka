package net.manub.embeddedkafka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps

class EmbeddedKafkaSpec
  extends TestKit(ActorSystem("embedded-kafka-spec")) with WordSpecLike with EmbeddedKafka with Matchers
  with ImplicitSender with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    system.shutdown()
  }

  "the withRunningKafka method" should {

    "start a Kafka broker on port 6001" in {

      withRunningKafka {
        system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 6001), testActor))
        expectMsg(1 second, ConnectionSuccessful)
      }
    }
  }
}

object TcpClient {
  def props(remote: InetSocketAddress, replies: ActorRef) = Props(classOf[TcpClient], remote, replies)
}

case object ConnectionSuccessful

class TcpClient(remote: InetSocketAddress, listener: ActorRef) extends Actor {

  import context.system

  IO(Tcp) ! Connect(remote)

  def receive: Receive = {
    case Connected(_, _) =>
      listener ! ConnectionSuccessful
      context stop self
  }
}