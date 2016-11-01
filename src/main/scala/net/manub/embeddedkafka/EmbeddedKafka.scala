package net.manub.embeddedkafka

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.Executors

import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.serializer.{Decoder, StringDecoder}
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.scalatest.Suite

import scala.collection.JavaConversions.mapAsJavaMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}
import scala.reflect.io.Directory
import scala.util.Try
import scala.util.control.NonFatal

trait EmbeddedKafka extends EmbeddedKafkaSupport {
  this: Suite =>
}

object EmbeddedKafka extends EmbeddedKafkaSupport {

  private[this] var factory: Option[ServerCnxnFactory] = None
  private[this] var broker: Option[KafkaServer] = None

  /**
    * Starts a ZooKeeper instance and a Kafka broker in memory.
    *
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def start()(implicit config: EmbeddedKafkaConfig) = {
    factory = Option(startZooKeeper(config.zooKeeperPort))
    broker = Option(startKafka(config))
  }

  def startZooKeeper(zkLogsDir: Directory)(implicit config: EmbeddedKafkaConfig): Unit = {
    factory = Option(startZooKeeper(config.zooKeeperPort, zkLogsDir))
  }

  def startKafka(kafkaLogDir: Directory)(implicit config: EmbeddedKafkaConfig): Unit = {
    broker = Option(startKafka(config, kafkaLogDir))
  }

  /**
    * Stops the in memory ZooKeeper instance and Kafka broker.
    */
  def stop(): Unit = {
    stopKafka()
    stopZooKeeper()
  }

  def stopZooKeeper(): Unit = {
    factory.foreach(_.shutdown())
    factory = None
  }

  def stopKafka(): Unit = {
    broker.foreach(_.shutdown)
    broker = None
  }

  /**
    * Returns whether the in memory Kafka and Zookeeper are running.
    */
  def isRunning: Boolean = factory.nonEmpty && broker.nonEmpty
}

sealed trait EmbeddedKafkaSupport {
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
    * Publishes synchronously a message of type [[String]] to the running Kafka broker.
    *
    * @see [[EmbeddedKafka#publishToKafka]]
    * @param topic the topic to which publish the message (it will be auto-created)
    * @param message the [[String]] message to publish
    * @param config an implicit [[EmbeddedKafkaConfig]]
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  def publishStringMessageToKafka(topic: String, message: String)(implicit config: EmbeddedKafkaConfig): Unit =
    publishToKafka(topic, message)(config, new StringSerializer)

  /**
    * Publishes synchronously a message to the running Kafka broker.
    *
    * @param topic the topic to which publish the message (it will be auto-created)
    * @param message the message of type [[T]] to publish
    * @param config an implicit [[EmbeddedKafkaConfig]]
    * @param serializer an implicit [[Serializer]] for the type [[T]]
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[KafkaUnavailableException])
  def publishToKafka[T](topic: String, message: T)
                       (implicit config: EmbeddedKafkaConfig, serializer: Serializer[T]): Unit = {

    val kafkaProducer = new KafkaProducer(Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG -> 5000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
    ), new StringSerializer, serializer)

    val sendFuture = kafkaProducer.send(new ProducerRecord(topic, message))
    val sendResult = Try {
      sendFuture.get(5, SECONDS)
    }

    kafkaProducer.close()

    if (sendResult.isFailure)
      throw new KafkaUnavailableException(sendResult.failed.get)
  }

  def consumeFirstStringMessageFrom(topic: String)(implicit config: EmbeddedKafkaConfig): String =
    consumeFirstMessageFrom(topic)(config, new StringDecoder())


  /**
    * Consumes the first message available in a given topic, deserializing it as a String.
    *
    * @param topic the topic to consume a message from
    * @param config an implicit [[EmbeddedKafkaConfig]]
    * @param decoder an implicit [[Decoder]] for the type [[T]]
    * @return the first message consumed from the given topic, with a type [[T]]
    * @throws TimeoutException if unable to consume a message within 5 seconds
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[TimeoutException])
  @throws(classOf[KafkaUnavailableException])
  def consumeFirstMessageFrom[T](topic: String)(implicit config: EmbeddedKafkaConfig, decoder: Decoder[T]): T = {
    val props = new Properties()
    props.put("group.id", s"embedded-kafka-spec")
    props.put("zookeeper.connect", s"localhost:${config.zooKeeperPort}")
    props.put("auto.offset.reset", "smallest")
    props.put("zookeeper.connection.timeout.ms", "6000")

    val consumer =
      try Consumer.create(new ConsumerConfig(props))
      catch {
        case NonFatal(e) =>
          throw new KafkaUnavailableException(e)
      }

    val messageStreams =
      consumer.createMessageStreamsByFilter(Whitelist(topic), keyDecoder = new StringDecoder, valueDecoder = decoder)

    val messageFuture = Future {
      messageStreams.headOption
        .getOrElse(throw new KafkaSpecException("Unable to find a message stream")).iterator().next().message()
    }

    try {
      Await.result(messageFuture, 5 seconds)
    } finally {
      consumer.shutdown()
    }
  }

  object aKafkaProducer {
    private[this] var producers = Vector.empty[KafkaProducer[_, _]]

    sys.addShutdownHook {
      producers.foreach(_.close())
    }

    def thatSerializesValuesWith[V](serializer: Class[_ <: Serializer[V]])(implicit config: EmbeddedKafkaConfig) = {
      val producer = new KafkaProducer[String, V](basicKafkaConfig(config) + (
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> serializer.getName)
      )
      producers :+= producer
      producer
    }

    def apply[V](implicit valueSerializer: Serializer[V], config: EmbeddedKafkaConfig) = {
      val producer = new KafkaProducer[String, V](basicKafkaConfig(config), new StringSerializer, valueSerializer)
      producers :+= producer
      producer
    }

    def basicKafkaConfig[V](config: EmbeddedKafkaConfig): Map[String, String] = Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG -> 5000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
    )
  }

  def startZooKeeper(zooKeeperPort: Int, zkLogsDir: Directory = Directory.makeTemp("zookeeper-logs")): ServerCnxnFactory = {
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile, zkLogsDir.toFile.jfile, tickTime)

    val factory = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("localhost", zooKeeperPort), 1024)
    factory.startup(zkServer)
    factory
  }

  def startKafka(config: EmbeddedKafkaConfig, kafkaLogDir: Directory = Directory.makeTemp("kafka")): KafkaServer = {
    val zkAddress = s"localhost:${config.zooKeeperPort}"

    val properties: Properties = new Properties
    properties.setProperty("zookeeper.connect", zkAddress)
    properties.setProperty("broker.id", "0")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("port", config.kafkaPort.toString)
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)

    config.customBrokerProperties.foreach {
      case (key, value) => properties.setProperty(key, value)
    }

    val broker = new KafkaServer(new KafkaConfig(properties))
    broker.startup()
    broker
  }
}