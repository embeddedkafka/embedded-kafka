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
      ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG -> 3000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
    ), new StringSerializer, serializer)

    val sendFuture = kafkaProducer.send(new ProducerRecord(topic, message))
    val sendResult = Try {
      sendFuture.get(3, SECONDS)
    }

    kafkaProducer.close()

    if (sendResult.isFailure) throw new KafkaUnavailableException
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
   * @throws TimeoutException if unable to consume a message within 3 seconds
   * @throws KafkaUnavailableException if unable to connect to Kafka
   */
  @throws(classOf[TimeoutException])
  @throws(classOf[KafkaUnavailableException])
  def consumeFirstMessageFrom[T](topic: String)(implicit config: EmbeddedKafkaConfig, decoder: Decoder[T]): T = {
    val props = new Properties()
    props.put("group.id", s"embedded-kafka-spec-$suiteId")
    props.put("zookeeper.connect", s"localhost:${config.zooKeeperPort}")
    props.put("auto.offset.reset", "smallest")
    props.put("zookeeper.connection.timeout.ms", "6000")

    val consumer = Try {
      Consumer.create(new ConsumerConfig(props))
    }.getOrElse(throw new KafkaUnavailableException)

    val messageStreams =
      consumer.createMessageStreamsByFilter(Whitelist(topic), keyDecoder = new StringDecoder, valueDecoder = decoder)

    val messageFuture = Future {
      messageStreams.headOption
        .getOrElse(throw new KafkaSpecException("Unable to find a message stream")).iterator().next().message()
    }

    try {
      Await.result(messageFuture, 3 seconds)
    } finally {
      consumer.shutdown()
    }
  }

  object aKafkaProducer {
    def thatSerializesValuesWith[V](serializer: Class[_ <: Serializer[V]])(implicit config: EmbeddedKafkaConfig) = {
      new KafkaProducer[String, V](basicKafkaConfig(config) +(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> serializer.getName))
    }

    def apply[V](implicit valueSerializer: Serializer[V], config: EmbeddedKafkaConfig) =
      new KafkaProducer[String, V](basicKafkaConfig(config), new StringSerializer, valueSerializer)

    def basicKafkaConfig[V](config: EmbeddedKafkaConfig): Map[String, String] = Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG -> 3000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
    )
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
