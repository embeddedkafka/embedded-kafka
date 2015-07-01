package net.manub.embeddedkafka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
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

  "an embedded kafka spec" when {

    "providing a spec inside whenEmbeddedKafka" should {

      "start a kafka broker" in {

        withEmbeddedKafka {

          system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 6001), testActor))
          expectMsg(2 seconds, "connect successful")

        }

      }
    }

  }

}

object TcpClient {
  def props(remote: InetSocketAddress, replies: ActorRef) = Props(classOf[TcpClient], remote, replies)
}

class TcpClient(remote: InetSocketAddress, listener: ActorRef) extends Actor {

  import Tcp._
  import context.system

  IO(Tcp) ! Connect(remote)

  def receive: Receive = {
    case CommandFailed(_: Connect) =>
      listener ! "connect failed"
      context stop self

    case c @ Connected(_, _) =>
      listener ! "connect successful"
      context stop self
  }
}