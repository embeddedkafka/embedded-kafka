package net.manub.embeddedkafka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.exceptions.TestFailedException
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

    "start a Kafka broker on port 6001 by default" in {

      withRunningKafka {
        system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 6001), testActor))
        expectMsg(1 second, ConnectionSuccessful)
      }
    }

    "stop Kafka and Zookeeper successfully" when {

      "the enclosed test passes" in {

        withRunningKafka {
          true shouldBe true
        }

        system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 6001), testActor))
        expectMsg(1 second, ConnectionFailed)

        system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 6000), testActor))
        expectMsg(1 second, ConnectionFailed)

      }

      "the enclosed test fails" in {

        a [TestFailedException] shouldBe thrownBy {
          withRunningKafka {
            true shouldBe false
          }
        }

        system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 6001), testActor))
        expectMsg(1 second, ConnectionFailed)

        system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 6000), testActor))
        expectMsg(1 second, ConnectionFailed)
      }
    }

    "start a Kafka broker on a specified port" in {

      implicit val config = EmbeddedKafkaConfig(kafkaPort = 12345)

      withRunningKafka {
        system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 12345), testActor))
        expectMsg(1 second, ConnectionSuccessful)
      }
    }
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