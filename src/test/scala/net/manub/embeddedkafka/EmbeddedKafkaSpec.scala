package net.manub.embeddedkafka

import java.net.InetSocketAddress
import java.util.Properties

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.testkit.{ImplicitSender, TestKit}
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.serializer.StringDecoder
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.exceptions.TestFailedException
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class EmbeddedKafkaSpec
  extends TestKit(ActorSystem("embedded-kafka-spec")) with WordSpecLike with EmbeddedKafka with Matchers
  with ImplicitSender with BeforeAndAfterAll with ScalaFutures {

  implicit val config = PatienceConfig(Span(2, Seconds), Span(100, Milliseconds))

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

    "start a ZooKeeper instance on port 6000 by default" in {

      withRunningKafka {
        system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 6000), testActor))
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

        a[TestFailedException] shouldBe thrownBy {
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

    "start a Zookeeper server on a specified port" in {

      implicit val config = EmbeddedKafkaConfig(zooKeeperPort = 12345)

      withRunningKafka {
        system.actorOf(TcpClient.props(new InetSocketAddress("localhost", 12345), testActor))
        expectMsg(1 second, ConnectionSuccessful)
      }
    }
  }

  "the publishToKafka method" should {

    "publish synchronously a message to Kafka as String" in {

      withRunningKafka {

        val message = "hello world!"
        val topic = "test_topic"

        publishToKafka(topic, message)

        val consumer = Consumer.create(consumerConfigForEmbeddedKafka)

        val filter = new Whitelist("test_topic")
        val stringDecoder = new StringDecoder

        val messageStreams = consumer.createMessageStreamsByFilter(filter, 1, stringDecoder, stringDecoder)

        val eventualMessage = Future {
          messageStreams.head.iterator().next().message()
        }

        whenReady(eventualMessage) { msg =>
          msg shouldBe message
        }

        consumer.shutdown()

      }

    }
  }

  lazy val consumerConfigForEmbeddedKafka: ConsumerConfig = {
    val props = new Properties()
    props.put("group.id", "test")
    props.put("zookeeper.connect", "localhost:6000")
    props.put("auto.offset.reset", "smallest")

    new ConsumerConfig(props)
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