package net.manub.embeddedkafka

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.Executors

import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.scalatest.Suite

import scala.collection.JavaConversions.mapAsJavaMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.language.{higherKinds, postfixOps}
import scala.reflect.io.Directory
import scala.util.Try

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

  val zkSessionTimeoutMs = 10000
  val zkConnectionTimeoutMs = 10000
  val zkSecurityEnabled = false

  /**
    * Starts a ZooKeeper instance and a Kafka broker, then executes the body passed as a parameter.
    *
    * @param body   the function to execute
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def withRunningKafka(body: => Any)(implicit config: EmbeddedKafkaConfig) = {

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
    * @param topic   the topic to which publish the message (it will be auto-created)
    * @param message the [[String]] message to publish
    * @param config  an implicit [[EmbeddedKafkaConfig]]
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  def publishStringMessageToKafka(topic: String, message: String)(implicit config: EmbeddedKafkaConfig): Unit =
    publishToKafka(topic, message)(config, new StringSerializer)

  /**
    * Publishes synchronously a message to the running Kafka broker.
    *
    * @param topic      the topic to which publish the message (it will be auto-created)
    * @param message    the message of type [[T]] to publish
    * @param config     an implicit [[EmbeddedKafkaConfig]]
    * @param serializer an implicit [[Serializer]] for the type [[T]]
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[KafkaUnavailableException])
  def publishToKafka[T](topic: String, message: T)
                       (implicit config: EmbeddedKafkaConfig, serializer: Serializer[T]): Unit = {

    val kafkaProducer = new KafkaProducer(baseProducerConfig, new StringSerializer, serializer)

    val sendFuture = kafkaProducer.send(new ProducerRecord(topic, message))
    val sendResult = Try {
      sendFuture.get(5, SECONDS)
    }

    kafkaProducer.close()

    if (sendResult.isFailure)
      throw new KafkaUnavailableException(sendResult.failed.get)
  }

  private def baseProducerConfig(implicit config: EmbeddedKafkaConfig) = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
    ProducerConfig.MAX_BLOCK_MS_CONFIG -> 5000.toString,
    ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
  )

  def consumeFirstStringMessageFrom(topic: String)(implicit config: EmbeddedKafkaConfig): String =
    consumeFirstMessageFrom(topic)(config, new StringDeserializer())


  /**
    * Consumes the first message available in a given topic, deserializing it as a String.
    *
    * @param topic        the topic to consume a message from
    * @param config       an implicit [[EmbeddedKafkaConfig]]
    * @param deserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[T]]
    * @return the first message consumed from the given topic, with a type [[T]]
    * @throws TimeoutException          if unable to consume a message within 5 seconds
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[TimeoutException])
  @throws(classOf[KafkaUnavailableException])
  def consumeFirstMessageFrom[T](topic: String)(implicit config: EmbeddedKafkaConfig, deserializer: Deserializer[T]): T = {

    import scala.collection.JavaConversions._

    val props = new Properties()
    props.put("group.id", s"embedded-kafka-spec")
    props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
    props.put("auto.offset.reset", "earliest")

    val consumer = new KafkaConsumer[String, T](props, new StringDeserializer, deserializer)

    val message = Try {
      consumer.subscribe(List(topic))
      consumer.partitionsFor(topic) // as poll doesn't honour the timeout, forcing the consumer to fail here.
      val records = consumer.poll(5000)
      if (records.isEmpty) {
        throw new TimeoutException("Unable to retrieve a message from Kafka in 5000ms")
      }
      records.iterator().next().value()
    }

    consumer.close()
    message.recover {
      case ex: KafkaException => throw new KafkaUnavailableException(ex)
    }.get
  }

  object aKafkaProducer {
    private[this] var producers = Vector.empty[KafkaProducer[_, _]]

    sys.addShutdownHook {
      producers.foreach(_.close())
    }

    def thatSerializesValuesWith[V](serializer: Class[_ <: Serializer[V]])(implicit config: EmbeddedKafkaConfig) = {
      val producer = new KafkaProducer[String, V](baseProducerConfig +(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> serializer.getName)
      )
      producers :+= producer
      producer
    }

    def apply[V](implicit valueSerializer: Serializer[V], config: EmbeddedKafkaConfig) = {
      val producer = new KafkaProducer[String, V](baseProducerConfig(config), new StringSerializer, valueSerializer)
      producers :+= producer
      producer
    }
  }

  def startZooKeeper(zooKeeperPort: Int, zkLogsDir: Directory = Directory.makeTemp("zookeeper-logs")): ServerCnxnFactory = {
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile, zkLogsDir.toFile.jfile, tickTime)

    val factory = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("0.0.0.0", zooKeeperPort), 1024)
    factory.startup(zkServer)
    factory
  }

  def startKafka(config: EmbeddedKafkaConfig, kafkaLogDir: Directory = Directory.makeTemp("kafka")): KafkaServer = {
    val zkAddress = s"localhost:${config.zooKeeperPort}"

    val properties: Properties = new Properties
    config.customBrokerProperties.foreach { case (key, value) => properties.setProperty(key, value) }
    properties.setProperty("zookeeper.connect", zkAddress)
    properties.setProperty("broker.id", "0")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("advertised.host.name", "localhost")
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("port", config.kafkaPort.toString)
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)

    val broker = new KafkaServer(new KafkaConfig(properties))
    broker.startup()
    broker
  }

  /**
    * Creates a topic with a custom configuration
    *
    * @param topic             the topic name
    * @param topicConfig       per topic configuration [[Map]]
    * @param partitions        number of partitions [[Int]]
    * @param replicationFactor replication factor [[Int]]
    * @param config            an implicit [[EmbeddedKafkaConfig]]
    */
  def createCustomTopic(topic: String, topicConfig: Map[String,String] = Map.empty,
    partitions: Int = 1, replicationFactor: Int = 1)(implicit config: EmbeddedKafkaConfig): Unit = {

    val zkUtils = ZkUtils(s"localhost:${config.zooKeeperPort}", zkSessionTimeoutMs, zkConnectionTimeoutMs, zkSecurityEnabled)
    val topicProperties = topicConfig.foldLeft(new Properties){case (props, (k,v)) => props.put(k,v); props}

    try AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, topicProperties) finally zkUtils.close()
  }

}