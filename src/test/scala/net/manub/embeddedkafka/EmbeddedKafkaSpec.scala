package net.manub.embeddedkafka

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.TimeoutException

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.testkit.{ImplicitSender, TestKit}
import kafka.consumer.{ConsumerConfig, Consumer, Whitelist}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.{JavaFutures, ScalaFutures}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class EmbeddedKafkaSpec
  extends TestKit(ActorSystem("embedded-kafka-spec")) with WordSpecLike with EmbeddedKafka with Matchers
  with ImplicitSender with BeforeAndAfterAll with ScalaFutures with JavaFutures {

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

    "publishes asynchronously a message to Kafka as String" in {

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

  "the consumeFirstMessageFrom method" should {

    "returns a message published to a topic" in {

      withRunningKafka {

        val message = "hello world!"
        val topic = "test_topic"

        val producer = new KafkaProducer[String, String](Map(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:6001",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName
        ))

        whenReady(producer.send(new ProducerRecord[String, String](topic, message))) { _ =>
          consumeFirstMessageFrom(topic) shouldBe message
        }

        producer.close()
      }
    }

    "throws a TimeoutExeption when a message is not available" in {

      withRunningKafka {

        a[TimeoutException] shouldBe thrownBy {
          consumeFirstMessageFrom("non_existing_topic")
        }
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