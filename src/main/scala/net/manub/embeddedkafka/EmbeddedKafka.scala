package net.manub.embeddedkafka

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.{TimeUnit, Executors}

import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.serializer.StringDecoder
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.scalatest.Suite

import scala.collection.JavaConversions.mapAsJavaMap
import scala.concurrent.duration._
import scala.concurrent._
import scala.language.postfixOps
import scala.reflect.io.Directory
import scala.util.Try

trait EmbeddedKafka {

  this: Suite =>

  val executorService = Executors.newFixedThreadPool(2)
  implicit val executionContext = ExecutionContext.fromExecutorService(executorService)

  /**
   * Starts a ZooKeeper instance and a Kafka broker, then executes the body passed as a parameter.
   *
   * @param body the function to execute
   * @param config an implicit [[EmbeddedKafkaConfig]]
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
   * @param config an implicit [[EmbeddedKafkaConfig]]
   * @throws KafkaUnavailableException if unable to connect to Kafka
   */
  @throws(classOf[KafkaUnavailableException])
  def publishToKafka(topic: String, message: String)(implicit config: EmbeddedKafkaConfig): Unit = {

    val kafkaProducer = new KafkaProducer[String, String](Map[String, String](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG       -> s"localhost:${config.kafkaPort}",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG    -> classOf[StringSerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG  -> classOf[StringSerializer].getName,
      ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG  -> 3000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG        -> 1000.toString
    ))

    val sendFuture = kafkaProducer.send(new ProducerRecord[String, String](topic, message))
    val sendResult = Try { sendFuture.get(3, SECONDS) }

    kafkaProducer.close()

    if (sendResult.isFailure) throw new KafkaUnavailableException
  }


  /**
   * Consumes the first message available in a given topic, deserializing it as a String.
   *
   * @param topic the topic to consume a message from
   * @param config an implicit [[EmbeddedKafkaConfig]]
   * @return the first message consumed from the given topic
   * @throws TimeoutException if unable to consume a message within 3 seconds
   * @throws KafkaUnavailableException if unable to connect to Kafka
   */
  @throws(classOf[TimeoutException])
  @throws(classOf[KafkaUnavailableException])
  def consumeFirstMessageFrom(topic: String)(implicit config: EmbeddedKafkaConfig): String = {
    val props = new Properties()
    props.put("group.id", "scalatest-embedded-kafka-spec")
    props.put("zookeeper.connect", s"localhost:${config.zooKeeperPort}")
    props.put("auto.offset.reset", "smallest")
    props.put("zookeeper.connection.timeout.ms", "6000")

    val consumer = Try {
      Consumer.create(new ConsumerConfig(props))
    }.getOrElse(throw new KafkaUnavailableException)

    val filter = Whitelist(topic)
    val messageStreams =
      consumer.createMessageStreamsByFilter(filter, keyDecoder = new StringDecoder, valueDecoder = new StringDecoder)

    val messageFuture = Future {
      messageStreams.headOption.getOrElse(throw new KafkaSpecException("Unable to find a message stream")).iterator().next().message()
    }

    try {
      Await.result(messageFuture, 3 seconds)
    } finally {
      consumer.shutdown()
    }
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
