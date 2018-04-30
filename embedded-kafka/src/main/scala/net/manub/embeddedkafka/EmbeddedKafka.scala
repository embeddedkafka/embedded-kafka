package net.manub.embeddedkafka

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.Executors

import io.confluent.kafka.schemaregistry.RestApp
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.serialization.{
  Deserializer,
  Serializer,
  StringDeserializer,
  StringSerializer
}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.scalatest.Suite

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{
  ExecutionContext,
  ExecutionContextExecutorService,
  TimeoutException
}
import scala.language.{higherKinds, postfixOps}
import scala.reflect.io.Directory
import scala.util.Try

trait EmbeddedKafka extends EmbeddedKafkaSupport {
  this: Suite =>
}

object EmbeddedKafka extends EmbeddedKafkaSupport {

  private[this] var servers: Seq[EmbeddedServer] = Seq.empty

  /**
    * Starts a ZooKeeper instance and a Kafka broker in memory, using temporary directories for storing logs.
    * Optionally also starts a Schema Registry app.
    * The log directories will be cleaned after calling the [[stop()]] method or on JVM exit, whichever happens earlier.
    *
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def start()(implicit config: EmbeddedKafkaConfig): EmbeddedK = {
    val zkLogsDir = Directory.makeTemp("zookeeper-logs")
    val kafkaLogsDir = Directory.makeTemp("kafka-logs")

    val factory =
      EmbeddedZ(startZooKeeper(config.zooKeeperPort, zkLogsDir), zkLogsDir)

    val kafkaServer = startKafka(config, kafkaLogsDir)
    // Optionally start Schema Registry
    val restApp = config.schemaRegistryPort.map(schemaRegistryPort =>
      EmbeddedSR(
        startSchemaRegistry(schemaRegistryPort, factory.factory.getLocalPort)))
    val broker =
      EmbeddedK(Option(factory), kafkaServer, restApp, kafkaLogsDir)

    servers :+= broker
    broker
  }

  /**
    * Starts a Zookeeper instance in memory, storing logs in a specific location.
    *
    * @param zkLogsDir the path for the Zookeeper logs
    * @param config    an implicit [[EmbeddedKafkaConfig]]
    * @return          an [[EmbeddedZ]] server
    */
  def startZooKeeper(zkLogsDir: Directory)(
      implicit config: EmbeddedKafkaConfig): EmbeddedZ = {
    val factory =
      EmbeddedZ(startZooKeeper(config.zooKeeperPort, zkLogsDir), zkLogsDir)
    servers :+= factory
    factory
  }

  /**
    * Starts a Kafka broker in memory, storing logs in a specific location.
    *
    * @param kafkaLogsDir the path for the Kafka logs
    * @param config       an implicit [[EmbeddedKafkaConfig]]
    * @return             an [[EmbeddedK]] server
    */
  def startKafka(kafkaLogsDir: Directory)(
      implicit config: EmbeddedKafkaConfig): EmbeddedK = {
    val broker = EmbeddedK(startKafka(config, kafkaLogsDir), kafkaLogsDir)
    servers :+= broker
    broker
  }

  /**
    * Stops all in memory ZooKeeper instances, Kafka brokers, Schema Registry apps, and deletes the log directories.
    */
  def stop(): Unit = {
    servers.foreach(_.stop(true))
    servers = Seq.empty
  }

  /**
    * Stops a specific [[EmbeddedServer]] instance, and deletes the log directory.
    *
    * @param server the [[EmbeddedServer]] to be stopped.
    */
  def stop(server: EmbeddedServer): Unit = {
    server.stop(true)
    servers = servers.filter(x => x != server)
  }

  /**
    * Stops all in memory Zookeeper instances, preserving the logs directories.
    */
  def stopZooKeeper(): Unit = {
    val factories = servers.toFilteredSeq[EmbeddedZ](isEmbeddedZ)

    factories
      .foreach(_.stop(false))

    servers = servers.filter(!factories.contains(_))
  }

  /**
    * Stops all in memory Kafka instances, preserving the logs directories.
    */
  def stopKafka(): Unit = {
    val brokers = servers.toFilteredSeq[EmbeddedK](isEmbeddedK)

    brokers
      .foreach(_.stop(false))

    servers = servers.filter(!brokers.contains(_))
  }

  /**
    * Stops all in memory Schema Registry instances.
    */
  def stopSchemaRegistry(): Unit = {
    val apps = servers.toFilteredSeq[EmbeddedSR](isEmbeddedSR)

    apps.foreach(_.stop())

    servers = servers.filter(!apps.contains(_))
  }

  /**
    * Returns whether the in memory Kafka and Zookeeper are both running.
    */
  def isRunning: Boolean =
    servers.toFilteredSeq[EmbeddedK](isEmbeddedK).exists(_.factory.isDefined)

  private def isEmbeddedK(server: EmbeddedServer): Boolean =
    server.isInstanceOf[EmbeddedK]
  private def isEmbeddedZ(server: EmbeddedServer): Boolean =
    server.isInstanceOf[EmbeddedZ]
  private def isEmbeddedSR(server: EmbeddedServer): Boolean =
    server.isInstanceOf[EmbeddedSR]

  implicit class ServerOps(servers: Seq[EmbeddedServer]) {
    def toFilteredSeq[T <: EmbeddedServer](
        filter: EmbeddedServer => Boolean): Seq[T] =
      servers.filter(filter).asInstanceOf[Seq[T]]
  }
}

sealed trait EmbeddedKafkaSupport {
  private val executorService = Executors.newFixedThreadPool(3)
  implicit private val executionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(executorService)

  val zkSessionTimeoutMs = 10000
  val zkConnectionTimeoutMs = 10000

  /**
    * Starts a ZooKeeper instance, a Kafka broker, and optionally a Schema Registry app, then executes the body passed as a parameter.
    *
    * @param body   the function to execute
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def withRunningKafka[T](body: => T)(
      implicit config: EmbeddedKafkaConfig): T = {
    withRunningZooKeeper(config.zooKeeperPort) { zkPort =>
      withTempDir("kafka") { kafkaLogsDir =>
        val broker =
          startKafka(config.kafkaPort,
                     zkPort,
                     config.customBrokerProperties,
                     kafkaLogsDir)
        // Optionally start Schema Registry
        val app = config.schemaRegistryPort.map(schemaRegistryPort =>
          startSchemaRegistry(schemaRegistryPort, zkPort))

        try {
          body
        } finally {
          app.foreach(_.stop())
          broker.shutdown()
          broker.awaitShutdown()
        }
      }
    }
  }

  /**
    * Starts a ZooKeeper instance, a Kafka broker, and optionally a Schema Registry app, then executes the body passed as a parameter.
    * The actual ZooKeeper, Kafka, and Schema Registry ports will be detected and inserted into a copied version of
    * the EmbeddedKafkaConfig that gets passed to body. This is useful if you set any port to 0, which will listen on an arbitrary available port.
    *
    * @param config the user-defined [[EmbeddedKafkaConfig]]
    * @param body   the function to execute, given an [[EmbeddedKafkaConfig]] with the actual
    *               ports Kafka, ZooKeeper, and Schema Registry are running on
    */
  def withRunningKafkaOnFoundPort[T](config: EmbeddedKafkaConfig)(
      body: EmbeddedKafkaConfig => T): T = {
    withRunningZooKeeper(config.zooKeeperPort) { zkPort =>
      withTempDir("kafka") { kafkaLogsDir =>
        val broker: KafkaServer =
          startKafka(config.kafkaPort,
                     zkPort,
                     config.customBrokerProperties,
                     kafkaLogsDir)
        val kafkaPort =
          broker.boundPort(broker.config.listeners.head.listenerName)
        // Optionally start Schema Registry
        val app = config.schemaRegistryPort.map(schemaRegistryPort =>
          startSchemaRegistry(schemaRegistryPort, zkPort))
        val schemaRegistryPort = app.map(_.restServer.getURI.getPort)

        val actualConfig =
          EmbeddedKafkaConfigImpl(kafkaPort,
                                  zkPort,
                                  schemaRegistryPort,
                                  config.customBrokerProperties,
                                  config.customProducerProperties,
                                  config.customConsumerProperties)
        try {
          body(actualConfig)
        } finally {
          app.foreach(_.stop())
          broker.shutdown()
          broker.awaitShutdown()
        }
      }
    }
  }

  private def withRunningZooKeeper[T](port: Int)(body: Int => T): T = {
    withTempDir("zookeeper-logs") { zkLogsDir =>
      val factory = startZooKeeper(port, zkLogsDir)
      try {
        body(factory.getLocalPort)
      } finally {
        factory.shutdown()
      }
    }
  }

  private def withTempDir[T](prefix: String)(body: Directory => T): T = {
    val dir = Directory.makeTemp(prefix)
    try {
      body(dir)
    } finally {
      dir.deleteRecursively()
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
    publishToKafka(new KafkaProducer(baseProducerConfig.asJava,
                                     new StringSerializer(),
                                     serializer),
                   new ProducerRecord[String, T](topic, message))

  /**
    * Publishes synchronously a message to the running Kafka broker.
    *
    * @param producerRecord the producerRecord of type [[T]] to publish
    * @param config         an implicit [[EmbeddedKafkaConfig]]
    * @param serializer     an implicit [[Serializer]] for the type [[T]]
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[KafkaUnavailableException])
  def publishToKafka[T](producerRecord: ProducerRecord[String, T])(
      implicit config: EmbeddedKafkaConfig,
      serializer: Serializer[T]): Unit =
    publishToKafka(new KafkaProducer(baseProducerConfig.asJava,
                                     new StringSerializer(),
                                     serializer),
                   producerRecord)

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
      new KafkaProducer(baseProducerConfig.asJava, keySerializer, serializer),
      new ProducerRecord(topic, key, message))

  /**
    * Publishes synchronously a batch of message to the running Kafka broker.
    *
    * @param topic      the topic to which publish the message (it will be auto-created)
    * @param messages    the keys and messages of type [[(K, T)]] to publish
    * @param config     an implicit [[EmbeddedKafkaConfig]]
    * @param keySerializer an implicit [[Serializer]] for the type [[K]]
    * @param serializer an implicit [[Serializer]] for the type [[T]]
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[KafkaUnavailableException])
  def publishToKafka[K, T](topic: String, messages: Seq[(K, T)])(
      implicit config: EmbeddedKafkaConfig,
      keySerializer: Serializer[K],
      serializer: Serializer[T]): Unit = {

    val producer =
      new KafkaProducer(baseProducerConfig.asJava, keySerializer, serializer)

    val tupleToRecord = (new ProducerRecord(topic, _: K, _: T)).tupled

    val futureSend = tupleToRecord andThen producer.send

    val futures = messages.map(futureSend)

    // Assure all messages sent before returning, and fail on first send error
    val records = futures.map(f => Try(f.get(10, SECONDS)))
    records
      .find(_.isFailure)
      .foreach(record => throw new KafkaUnavailableException(record.failed.get))

    producer.close()
  }

  private def publishToKafka[K, T](kafkaProducer: KafkaProducer[K, T],
                                   record: ProducerRecord[K, T]): Unit = {
    val sendFuture = kafkaProducer.send(record)
    val sendResult = Try {
      sendFuture.get(10, SECONDS)
    }

    kafkaProducer.close()

    if (sendResult.isFailure)
      throw new KafkaUnavailableException(sendResult.failed.get)
  }

  def kafkaProducer[K, T](topic: String, key: K, message: T)(
      implicit config: EmbeddedKafkaConfig,
      keySerializer: Serializer[K],
      serializer: Serializer[T]) =
    new KafkaProducer[K, T](baseProducerConfig.asJava,
                            keySerializer,
                            serializer)

  def kafkaConsumer[K, T](implicit config: EmbeddedKafkaConfig,
                          keyDeserializer: Deserializer[K],
                          deserializer: Deserializer[T]) =
    new KafkaConsumer[K, T](baseConsumerConfig, keyDeserializer, deserializer)

  private def baseProducerConfig(implicit config: EmbeddedKafkaConfig) =
    Map[String, Object](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ProducerConfig.MAX_BLOCK_MS_CONFIG -> 10000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
    ) ++ avro.schemaregistry.configForSchemaRegistry
      .getOrElse(Map.empty[String, Object]) ++ config.customProducerProperties

  private def baseConsumerConfig(
      implicit config: EmbeddedKafkaConfig): Properties = {
    val props = new Properties()
    props.put("group.id", s"embedded-kafka-spec")
    props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")
    avro.schemaregistry.consumerConfigForSchemaRegistry.foreach(m =>
      props.putAll(m.asJava))
    props.putAll(config.customConsumerProperties.asJava)
    props
  }

  def consumeFirstStringMessageFrom(topic: String, autoCommit: Boolean = false)(
      implicit config: EmbeddedKafkaConfig): String =
    consumeNumberStringMessagesFrom(topic, 1, autoCommit)(config).head

  def consumeNumberStringMessagesFrom(topic: String,
                                      number: Int,
                                      autoCommit: Boolean = false)(
      implicit config: EmbeddedKafkaConfig): List[String] =
    consumeNumberMessagesFrom(topic, number, autoCommit)(
      config,
      new StringDeserializer())

  /**
    * Consumes the first message available in a given topic, deserializing it as type [[V]].
    *
    * Only the message that is returned is committed if autoCommit is false.
    * If autoCommit is true then all messages that were polled will be committed.
    *
    * @param topic        the topic to consume a message from
    * @param autoCommit   if false, only the offset for the consumed message will be committed.
    *                     if true, the offset for the last polled message will be committed instead.
    *                     Defaulted to false.
    * @param config       an implicit [[EmbeddedKafkaConfig]]
    * @param valueDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[V]]
    * @return the first message consumed from the given topic, with a type [[V]]
    * @throws TimeoutException          if unable to consume a message within 5 seconds
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[TimeoutException])
  @throws(classOf[KafkaUnavailableException])
  def consumeFirstMessageFrom[V](topic: String, autoCommit: Boolean = false)(
      implicit config: EmbeddedKafkaConfig,
      valueDeserializer: Deserializer[V]): V =
    consumeNumberMessagesFrom[V](topic, 1, autoCommit)(config,
                                                       valueDeserializer).head

  /**
    * Consumes the first message available in a given topic, deserializing it as type [[(K, V)]].
    *
    * Only the message that is returned is committed if autoCommit is false.
    * If autoCommit is true then all messages that were polled will be committed.
    *
    * @param topic        the topic to consume a message from
    * @param autoCommit   if false, only the offset for the consumed message will be committed.
    *                     if true, the offset for the last polled message will be committed instead.
    *                     Defaulted to false.
    * @param config       an implicit [[EmbeddedKafkaConfig]]
    * @param keyDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[K]]
    * @param valueDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[V]]
    * @return the first message consumed from the given topic, with a type [[(K, V)]]
    * @throws TimeoutException          if unable to consume a message within 5 seconds
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[TimeoutException])
  @throws(classOf[KafkaUnavailableException])
  def consumeFirstKeyedMessageFrom[K, V](topic: String,
                                         autoCommit: Boolean = false)(
      implicit config: EmbeddedKafkaConfig,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]): (K, V) =
    consumeNumberKeyedMessagesFrom[K, V](topic, 1, autoCommit)(
      config,
      keyDeserializer,
      valueDeserializer).head

  def consumeNumberMessagesFrom[V](topic: String,
                                   number: Int,
                                   autoCommit: Boolean = false)(
      implicit config: EmbeddedKafkaConfig,
      valueDeserializer: Deserializer[V]): List[V] =
    consumeNumberMessagesFromTopics(Set(topic), number, autoCommit)(
      config,
      valueDeserializer)(topic)

  def consumeNumberKeyedMessagesFrom[K, V](topic: String,
                                           number: Int,
                                           autoCommit: Boolean = false)(
      implicit config: EmbeddedKafkaConfig,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]): List[(K, V)] =
    consumeNumberKeyedMessagesFromTopics(Set(topic), number, autoCommit)(
      config,
      keyDeserializer,
      valueDeserializer)(topic)

  /**
    * Consumes the first n messages available in given topics, deserializes them as type [[V]], and returns
    * the n messages in a Map from topic name to List[V].
    *
    * Only the messages that are returned are committed if autoCommit is false.
    * If autoCommit is true then all messages that were polled will be committed.
    *
    * @param topics                    the topics to consume messages from
    * @param number                    the number of messages to consume in a batch
    * @param autoCommit                if false, only the offset for the consumed messages will be committed.
    *                                  if true, the offset for the last polled message will be committed instead.
    *                                  Defaulted to false.
    * @param timeout                   the interval to wait for messages before throwing TimeoutException
    * @param resetTimeoutOnEachMessage when true, throw TimeoutException if we have a silent period
    *                                  (no incoming messages) for the timeout interval; when false,
    *                                  throw TimeoutException after the timeout interval if we
    *                                  haven't received all of the expected messages
    * @param config                    an implicit [[EmbeddedKafkaConfig]]
    * @param                           valueDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]]
    *                                  for the type [[V]]
    * @return the List of messages consumed from the given topics, each with a type [[V]]
    * @throws TimeoutException          if unable to consume messages within specified timeout
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  def consumeNumberMessagesFromTopics[V](topics: Set[String],
                                         number: Int,
                                         autoCommit: Boolean = false,
                                         timeout: Duration = 5.seconds,
                                         resetTimeoutOnEachMessage: Boolean =
                                           true)(
      implicit config: EmbeddedKafkaConfig,
      valueDeserializer: Deserializer[V]): Map[String, List[V]] = {
    consumeNumberKeyedMessagesFromTopics(topics,
                                         number,
                                         autoCommit,
                                         timeout,
                                         resetTimeoutOnEachMessage)(
      config,
      new StringDeserializer(),
      valueDeserializer)
      .mapValues(_.map(_._2))
  }

  /**
    * Consumes the first n messages available in given topics, deserializes them as type [[(K, V)]], and returns
    * the n messages in a Map from topic name to List[(K, V)].
    *
    * Only the messages that are returned are committed if autoCommit is false.
    * If autoCommit is true then all messages that were polled will be committed.
    *
    * @param topics       the topics to consume messages from
    * @param number       the number of messages to consume in a batch
    * @param autoCommit   if false, only the offset for the consumed messages will be committed.
    *                     if true, the offset for the last polled message will be committed instead.
    *                     Defaulted to false.
    * @param timeout      the interval to wait for messages before throwing TimeoutException
    * @param resetTimeoutOnEachMessage when true, throw TimeoutException if we have a silent period
    *                                  (no incoming messages) for the timeout interval; when false,
    *                                  throw TimeoutException after the timeout interval if we
    *                                  haven't received all of the expected messages
    * @param config       an implicit [[EmbeddedKafkaConfig]]
    * @param keyDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[K]]
    * @param valueDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[V]]
    * @return the List of messages consumed from the given topics, each with a type [[(K, V)]]
    * @throws TimeoutException          if unable to consume messages within specified timeout
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  def consumeNumberKeyedMessagesFromTopics[K, V](
      topics: Set[String],
      number: Int,
      autoCommit: Boolean = false,
      timeout: Duration = 5.seconds,
      resetTimeoutOnEachMessage: Boolean = true)(
      implicit config: EmbeddedKafkaConfig,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]): Map[String, List[(K, V)]] = {

    import scala.collection.JavaConverters._

    val props = baseConsumerConfig
    props.put("enable.auto.commit", autoCommit.toString)

    var timeoutNanoTime = System.nanoTime + timeout.toNanos
    val consumer =
      new KafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)

    val messages = Try {
      val messagesBuffers = topics.map(_ -> ListBuffer.empty[(K, V)]).toMap
      var messagesRead = 0
      consumer.subscribe(topics.asJava)
      topics.foreach(consumer.partitionsFor)

      while (messagesRead < number && System.nanoTime < timeoutNanoTime) {
        val records = consumer.poll(1000)
        val recordIter = records.iterator()
        if (resetTimeoutOnEachMessage && recordIter.hasNext) {
          timeoutNanoTime = System.nanoTime + timeout.toNanos
        }
        while (recordIter.hasNext && messagesRead < number) {
          val record = recordIter.next()
          val topic = record.topic()
          val message = (record.key(), record.value())
          messagesBuffers(topic) += message
          val tp = new TopicPartition(topic, record.partition())
          val om = new OffsetAndMetadata(record.offset() + 1)
          consumer.commitSync(Map(tp -> om).asJava)
          messagesRead += 1
        }
      }
      if (messagesRead < number) {
        throw new TimeoutException(
          s"Unable to retrieve $number message(s) from Kafka in $timeout")
      }
      messagesBuffers.map { case (topic, messages) => topic -> messages.toList }
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
        (baseProducerConfig + (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
          StringSerializer].getName,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> serializer.getName)).asJava)
      producers :+= producer
      producer
    }

    def apply[V](implicit valueSerializer: Serializer[V],
                 config: EmbeddedKafkaConfig): KafkaProducer[String, V] = {
      val producer = new KafkaProducer[String, V](
        baseProducerConfig(config).asJava,
        new StringSerializer,
        valueSerializer)
      producers :+= producer
      producer
    }
  }

  // Make sure to start Schema Registry after Kafka and ZooKeeper
  def startSchemaRegistry(schemaRegistryPort: Int,
                          zooKeeperPort: Int,
                          avroCompatibilityLevel: AvroCompatibilityLevel =
                            AvroCompatibilityLevel.NONE,
                          properties: Properties = new Properties): RestApp = {
    val server = new RestApp(schemaRegistryPort,
                             s"localhost:$zooKeeperPort",
                             "_schemas",
                             avroCompatibilityLevel.name,
                             properties)
    server.start()
    server
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

  private def startKafka(kafkaPort: Int,
                         zooKeeperPort: Int,
                         customBrokerProperties: Map[String, String],
                         kafkaLogDir: Directory) = {
    val zkAddress = s"localhost:$zooKeeperPort"
    val listener = s"PLAINTEXT://localhost:$kafkaPort"

    val properties = new Properties
    properties.setProperty("zookeeper.connect", zkAddress)
    properties.setProperty("broker.id", "0")
    properties.setProperty("listeners", listener)
    properties.setProperty("advertised.listeners", listener)
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)
    properties.setProperty("offsets.topic.replication.factor", 1.toString)
    properties.setProperty("offsets.topic.num.partitions", 1.toString)
    properties.setProperty("transaction.state.log.replication.factor",
                           1.toString)
    properties.setProperty("transaction.state.log.min.isr", 1.toString)

    // The total memory used for log deduplication across all cleaner threads, keep it small to not exhaust suite memory
    properties.setProperty("log.cleaner.dedupe.buffer.size", "1048577")

    customBrokerProperties.foreach {
      case (key, value) => properties.setProperty(key, value)
    }

    val broker = new KafkaServer(new KafkaConfig(properties))
    broker.startup()
    broker
  }

  def startKafka(config: EmbeddedKafkaConfig,
                 kafkaLogDir: Directory): KafkaServer = {
    startKafka(config.kafkaPort,
               config.zooKeeperPort,
               config.customBrokerProperties,
               kafkaLogDir)
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
    val adminClient = AdminClient.create(
      Map[String, Object](
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
        AdminClientConfig.CLIENT_ID_CONFIG -> "embedded-kafka-admin-client",
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> zkSessionTimeoutMs.toString,
        AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG -> zkConnectionTimeoutMs.toString
      ).asJava)
    val newTopic = new NewTopic(topic, partitions, replicationFactor.toShort)
      .configs(topicConfig.asJava)

    try {
      adminClient
        .createTopics(Seq(newTopic).asJava)
        .all
        .get(2, SECONDS)
    } finally adminClient.close()
  }

}
