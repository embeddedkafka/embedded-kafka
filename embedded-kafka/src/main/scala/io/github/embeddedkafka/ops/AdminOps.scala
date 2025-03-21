package io.github.embeddedkafka.ops

import io.github.embeddedkafka.{EmbeddedKafkaConfig, duration2JavaDuration}
import org.apache.kafka.clients.admin.{
  AdminClient,
  AdminClientConfig,
  DeleteTopicsOptions,
  DescribeTopicsOptions,
  ListTopicsOptions,
  NewTopic,
  TopicDescription
}

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import scala.util.Try

/**
  * Trait for admin-level actions on Kafka components. Relies on
  * `org.apache.kafka.clients.admin.AdminClient`.
  *
  * @tparam C
  *   an [[EmbeddedKafkaConfig]]
  */
trait AdminOps[C <: EmbeddedKafkaConfig] {
  val zkSessionTimeoutMs                                = 10000
  val zkConnectionTimeoutMs                             = 10000
  protected val topicCreationTimeout: FiniteDuration    = 2.seconds
  protected val topicDeletionTimeout: FiniteDuration    = 2.seconds
  protected val topicListTimeout: FiniteDuration        = 2.seconds
  protected val topicDescribeTimeout: FiniteDuration    = 2.seconds
  protected val adminClientCloseTimeout: FiniteDuration = 2.seconds

  /**
    * Creates a topic with a custom configuration.
    *
    * @param topic
    *   the topic name
    * @param topicConfig
    *   per topic configuration `Map`
    * @param partitions
    *   number of partitions
    * @param replicationFactor
    *   replication factor
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    */
  def createCustomTopic(
      topic: String,
      topicConfig: Map[String, String] = Map.empty,
      partitions: Int = 1,
      replicationFactor: Int = 1
  )(implicit config: C): Try[Unit] = {
    val newTopic = new NewTopic(topic, partitions, replicationFactor.toShort)
      .configs(topicConfig.asJava)

    withAdminClient { adminClient =>
      adminClient
        .createTopics(Seq(newTopic).asJava)
        .all
        .get(topicCreationTimeout.length, topicCreationTimeout.unit)
    }.map(_ => ())
  }

  /**
    * Either deletes or marks for deletion a list of topics.
    *
    * @param topics
    *   the topic names
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    */
  def deleteTopics(topics: List[String])(implicit config: C): Try[Unit] = {
    val opts = new DeleteTopicsOptions()
      .timeoutMs(topicDeletionTimeout.toMillis.toInt)

    withAdminClient { adminClient =>
      adminClient
        .deleteTopics(topics.asJava, opts)
        .all
        .get(topicDeletionTimeout.length, topicDeletionTimeout.unit)
    }.map(_ => ())
  }

  /**
    * Lists the topics available.
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    * @return
    *   the list of topic names
    */
  def listTopics()(implicit config: C): Try[Set[String]] = {
    val opts = new ListTopicsOptions()
      .timeoutMs(topicListTimeout.toMillis.toInt)

    withAdminClient { adminClient =>
      adminClient
        .listTopics(opts)
        .names()
        .get(topicListTimeout.length, topicListTimeout.unit)
        .asScala
        .toSet
    }
  }

  /**
    * Describe the topics.
    *
    * @param topics
    *   the topic names to describe
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    * @return
    *   the list of topic names
    */
  def describeTopics(
      topics: Seq[String]
  )(implicit config: C): Try[Map[String, TopicDescription]] = {
    val opts = new DescribeTopicsOptions()
      .timeoutMs(topicDescribeTimeout.toMillis.toInt)

    withAdminClient { adminClient =>
      adminClient
        .describeTopics(topics.asJavaCollection, opts)
        .allTopicNames()
        .get(topicDescribeTimeout.length, topicDescribeTimeout.unit)
        .asScala
        .toMap
    }
  }

  /**
    * Creates an `AdminClient`, then executes the body passed as a parameter.
    *
    * @param body
    *   the function to execute
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    */
  protected def withAdminClient[T](
      body: AdminClient => T
  )(implicit config: C): Try[T] = {
    val adminClient = AdminClient.create(
      Map[String, Object](
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
        AdminClientConfig.CLIENT_ID_CONFIG -> "embedded-kafka-admin-client",
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> zkSessionTimeoutMs.toString,
        AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG -> zkConnectionTimeoutMs.toString
      ).asJava
    )

    val res = Try(body(adminClient))
    adminClient.close(duration2JavaDuration(adminClientCloseTimeout))

    res
  }
}
