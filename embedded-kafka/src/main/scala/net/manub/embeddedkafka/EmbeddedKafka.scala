package net.manub.embeddedkafka

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.Executors

import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.serialization.{
  Deserializer,
  Serializer,
  StringDeserializer,
  StringSerializer
}
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.scalatest.Suite

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.language.{higherKinds, postfixOps}
import scala.reflect.io.Directory
import scala.util.Try

trait EmbeddedKafka extends EmbeddedKafkaSupport { this: Suite =>
}

object EmbeddedKafka extends EmbeddedKafkaSupport {

  private[this] var factory: Option[ServerCnxnFactory] = None
  private[this] var broker: Option[KafkaServer] = None
  private[this] val logsDirs = mutable.Buffer.empty[Directory]

  /**
    * Starts a ZooKeeper instance and a Kafka broker in memory, using temporary directories for storing logs.
    * The log directories will be cleaned after calling the [[stop()]] method or on JVM exit, whichever happens earlier.
    *
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def start()(implicit config: EmbeddedKafkaConfig): Unit = {
    val zkLogsDir = Directory.makeTemp("zookeeper-logs")
    val kafkaLogsDir = Directory.makeTemp("kafka-logs")

    factory = Option(startZooKeeper(config.zooKeeperPort, zkLogsDir))
    broker = Option(startKafka(config, kafkaLogsDir))

    logsDirs ++= Seq(zkLogsDir, kafkaLogsDir)
  }

  /**
    * Starts a Zookeeper instance in memory, storing logs in a specific location.
    *
    * @param zkLogsDir the path for the Zookeeper logs
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def startZooKeeper(zkLogsDir: Directory)(
      implicit config: EmbeddedKafkaConfig): Unit = {
    factory = Option(startZooKeeper(config.zooKeeperPort, zkLogsDir))
  }

  /**
    * Starts a Kafka broker in memory, storing logs in a specific location.
    *
    * @param kafkaLogDir the path for the Kafka logs
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def startKafka(kafkaLogDir: Directory)(
      implicit config: EmbeddedKafkaConfig): Unit = {
    broker = Option(startKafka(config, kafkaLogDir))
  }

  /**
    * Stops the in memory ZooKeeper instance and Kafka broker, and deletes the log directories.
    */
  def stop(): Unit = {
    stopKafka()
    stopZooKeeper()
    logsDirs.foreach(_.deleteRecursively())
    logsDirs.clear()
  }

  /**
    * Stops the in memory Zookeeper instance, preserving the logs directory.
    */
  def stopZooKeeper(): Unit = {
    factory.foreach(_.shutdown())
    factory = None
  }

  /**
    * Stops the in memory Kafka instance, preserving the logs directory.
    */
  def stopKafka(): Unit = {
    broker.foreach { b =>
      b.shutdown()
      b.awaitShutdown()
    }
    broker = None
  }

  /**
    * Returns whether the in memory Kafka and Zookeeper are running.
    */
  def isRunning: Boolean = factory.nonEmpty && broker.nonEmpty
}

sealed trait EmbeddedKafkaSupport {
  private val executorService = Executors.newFixedThreadPool(2)
  implicit private val executionContext =
    ExecutionContext.fromExecutorService(executorService)

  val zkSessionTimeoutMs = 10000
  val zkConnectionTimeoutMs = 10000
  val zkSecurityEnabled = false

  /**
    * Starts a ZooKeeper instance and a Kafka broker, then executes the body passed as a parameter.
    *
    * @param body   the function to execute
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def withRunningKafka(body: => Any)(
      implicit config: EmbeddedKafkaConfig): Any = {

    def cleanLogs(directories: Directory*): Unit = {
      directories.foreach(_.deleteRecursively())
    }

    val zkLogsDir = Directory.makeTemp("zookeeper-logs")
    val kafkaLogsDir = Directory.makeTemp("kafka")

    val factory = startZooKeeper(config.zooKeeperPort, zkLogsDir)
    val broker = startKafka(config, kafkaLogsDir)

    try {
      body
    } finally {
      broker.shutdown()
      broker.awaitShutdown()
      factory.shutdown()
      cleanLogs(zkLogsDir, kafkaLogsDir)
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
  def publishStringMessageToKafka(topic: String, message: String)(
      implicit config: EmbeddedKafkaConfig): Unit =
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
  def publishToKafka[T](topic: String, message: T)(
      implicit config: EmbeddedKafkaConfig,
      serializer: Serializer[T]): Unit =
    publishToKafka(new KafkaProducer(baseProducerConfig,
                                     new StringSerializer(),
                                     serializer),
                   new ProducerRecord[String, T](topic, message))

  /**
    * Publishes synchronously a message to the running Kafka broker.
    *
    * @param topic      the topic to which publish the message (it will be auto-created)
    * @param key        the key of type [[K]] to publish
    * @param message    the message of type [[T]] to publish
    * @param config     an implicit [[EmbeddedKafkaConfig]]
    * @param serializer an implicit [[Serializer]] for the type [[T]]
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[KafkaUnavailableException])
  def publishToKafka[K, T](topic: String, key: K, message: T)(
      implicit config: EmbeddedKafkaConfig,
      keySerializer: Serializer[K],
      serializer: Serializer[T]): Unit =
    publishToKafka(
      new KafkaProducer(baseProducerConfig, keySerializer, serializer),
      new ProducerRecord(topic, key, message))

  private def publishToKafka[K, T](kafkaProducer: KafkaProducer[K, T],
                                   record: ProducerRecord[K, T]) = {
    val sendFuture = kafkaProducer.send(record)
    val sendResult = Try {
      sendFuture.get(10, SECONDS)
    }

    kafkaProducer.close()

    if (sendResult.isFailure)
      throw new KafkaUnavailableException(sendResult.failed.get)
  }

  private def baseProducerConfig(implicit config: EmbeddedKafkaConfig) = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
    ProducerConfig.MAX_BLOCK_MS_CONFIG -> 10000.toString,
    ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
  ) ++ config.customProducerProperties

  private def baseConsumerConfig(
      implicit config: EmbeddedKafkaConfig): Properties = {
    val props = new Properties()
    props.put("group.id", s"embedded-kafka-spec")
    props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")
    props.putAll(config.customConsumerProperties)
    props
  }

  def consumeFirstStringMessageFrom(topic: String,
                                    autoCommit: Boolean = false)(
      implicit config: EmbeddedKafkaConfig): String =
    consumeFirstMessageFrom(topic, autoCommit)(config,
                                               new StringDeserializer())

  def consumeNumberStringMessagesFrom(topic: String,
                                      number: Int,
                                      autoCommit: Boolean = false)(
      implicit config: EmbeddedKafkaConfig): List[String] =
    consumeNumberMessagesFrom(topic, number, autoCommit)(
      config,
      new StringDeserializer())

  /**
    * Consumes the first message available in a given topic, deserializing it as a String.
    *
    * Only the messsage that is returned is committed if autoCommit is false.
    * If autoCommit is true then all messages that were polled will be committed.
    *
    * @param topic        the topic to consume a message from
    * @param autoCommit   if false, only the offset for the consumed message will be commited.
    *                     if true, the offset for the last polled message will be committed instead.
    *                     Defaulted to false.
    * @param config       an implicit [[EmbeddedKafkaConfig]]
    * @param deserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[T]]
    * @return the first message consumed from the given topic, with a type [[T]]
    * @throws TimeoutException          if unable to consume a message within 5 seconds
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[TimeoutException])
  @throws(classOf[KafkaUnavailableException])
  def consumeFirstMessageFrom[T](topic: String, autoCommit: Boolean = false)(
      implicit config: EmbeddedKafkaConfig,
      deserializer: Deserializer[T]): T = {

    import scala.collection.JavaConversions._

    val props = baseConsumerConfig
    props.put("enable.auto.commit", autoCommit.toString)

    val consumer =
      new KafkaConsumer[String, T](props, new StringDeserializer, deserializer)

    val message = Try {
      consumer.subscribe(List(topic))
      consumer.partitionsFor(topic) // as poll doesn't honour the timeout, forcing the consumer to fail here.
      val records = consumer.poll(5000)
      if (records.isEmpty) {
        throw new TimeoutException(
          "Unable to retrieve a message from Kafka in 5000ms")
      }

      val record = records.iterator().next()

      val tp = new TopicPartition(record.topic(), record.partition())
      val om = new OffsetAndMetadata(record.offset() + 1)
      consumer.commitSync(Map(tp -> om))

      record.value()
    }

    consumer.close()
    message.recover {
      case ex: KafkaException => throw new KafkaUnavailableException(ex)
    }.get
  }

  /**
    * Consumes the first n messages available in a given topic, deserializing it as a String, and returns
    * the n messages as a List.
    *
    * Only the messsages that are returned are committed if autoCommit is false.
    * If autoCommit is true then all messages that were polled will be committed.
    *
    * @param topic        the topic to consume a message from
    * @param number       the number of messagese to consume in a batch
    * @param autoCommit   if false, only the offset for the consumed message will be commited.
    *                     if true, the offset for the last polled message will be committed instead.
    *                     Defaulted to false.
    * @param config       an implicit [[EmbeddedKafkaConfig]]
    * @param deserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[T]]
    * @return the first message consumed from the given topic, with a type [[T]]
    * @throws TimeoutException          if unable to consume a message within 5 seconds
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  def consumeNumberMessagesFrom[T](topic: String,
                                   number: Int,
                                   autoCommit: Boolean = false)(
      implicit config: EmbeddedKafkaConfig,
      deserializer: Deserializer[T]): List[T] = {

    import scala.collection.JavaConverters._

    val props = baseConsumerConfig
    props.put("enable.auto.commit", autoCommit.toString)

    val consumer =
      new KafkaConsumer[String, T](props, new StringDeserializer, deserializer)

    val messages = Try {
      val messagesBuffer = ListBuffer.empty[T]
      var messagesRead = 0
      consumer.subscribe(List(topic).asJava)
      consumer.partitionsFor(topic)

      while (messagesRead < number) {
        val records = consumer.poll(5000)
        if (records.isEmpty) {
          throw new TimeoutException(
            "Unable to retrieve a message from Kafka in 5000ms")
        }

        val recordIter = records.iterator()
        while (recordIter.hasNext && messagesRead < number) {
          val record = recordIter.next()
          messagesBuffer += record.value()
          val tp = new TopicPartition(record.topic(), record.partition())
          val om = new OffsetAndMetadata(record.offset() + 1)
          consumer.commitSync(Map(tp -> om).asJava)
          messagesRead += 1
        }
      }
      messagesBuffer.toList
    }

    consumer.close()
    messages.recover {
      case ex: KafkaException => throw new KafkaUnavailableException(ex)
    }.get
  }

  object aKafkaProducer {
    private[this] var producers = Vector.empty[KafkaProducer[_, _]]

    sys.addShutdownHook {
      producers.foreach(_.close())
    }

    def thatSerializesValuesWith[V](serializer: Class[_ <: Serializer[V]])(
        implicit config: EmbeddedKafkaConfig): KafkaProducer[String, V] = {
      val producer = new KafkaProducer[String, V](
        baseProducerConfig + (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
          StringSerializer].getName,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> serializer.getName))
      producers :+= producer
      producer
    }

    def apply[V](implicit valueSerializer: Serializer[V],
                 config: EmbeddedKafkaConfig): KafkaProducer[String, V] = {
      val producer = new KafkaProducer[String, V](baseProducerConfig(config),
                                                  new StringSerializer,
                                                  valueSerializer)
      producers :+= producer
      producer
    }
  }

  def startZooKeeper(zooKeeperPort: Int,
                     zkLogsDir: Directory): ServerCnxnFactory = {
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile,
                                       zkLogsDir.toFile.jfile,
                                       tickTime)

    val factory = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("0.0.0.0", zooKeeperPort), 1024)
    factory.startup(zkServer)
    factory
  }

  def startKafka(config: EmbeddedKafkaConfig,
                 kafkaLogDir: Directory): KafkaServer = {
    val zkAddress = s"localhost:${config.zooKeeperPort}"

    val properties: Properties = new Properties
    properties.setProperty("zookeeper.connect", zkAddress)
    properties.setProperty("broker.id", "0")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("advertised.host.name", "localhost")
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("port", config.kafkaPort.toString)
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)

    // The total memory used for log deduplication across all cleaner threads, keep it small to not exhaust suite memory
    properties.setProperty("log.cleaner.dedupe.buffer.size", "1048577")

    config.customBrokerProperties.foreach {
      case (key, value) => properties.setProperty(key, value)
    }

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
  def createCustomTopic(topic: String,
                        topicConfig: Map[String, String] = Map.empty,
                        partitions: Int = 1,
                        replicationFactor: Int = 1)(
      implicit config: EmbeddedKafkaConfig): Unit = {

    val zkUtils = ZkUtils(s"localhost:${config.zooKeeperPort}",
                          zkSessionTimeoutMs,
                          zkConnectionTimeoutMs,
                          zkSecurityEnabled)
    val topicProperties = topicConfig.foldLeft(new Properties) {
      case (props, (k, v)) => props.put(k, v); props
    }

    try AdminUtils.createTopic(zkUtils,
                               topic,
                               partitions,
                               replicationFactor,
                               topicProperties)
    finally zkUtils.close()
  }

}
