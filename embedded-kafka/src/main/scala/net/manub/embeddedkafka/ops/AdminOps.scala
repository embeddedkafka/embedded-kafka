package net.manub.embeddedkafka.ops

import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import scala.concurrent.duration._

/**
  * Trait for admin-level actions on Kafka components.
  * Relies on [[AdminClient]].
  *
  * @tparam C an [[EmbeddedKafkaConfig]]
  */
trait AdminOps[C <: EmbeddedKafkaConfig] {

  val zkSessionTimeoutMs = 10000
  val zkConnectionTimeoutMs = 10000
  protected val topicCreationTimeout: FiniteDuration = 2.seconds

  /**
    * Creates a topic with a custom configuration.
    *
    * @param topic             the topic name
    * @param topicConfig       per topic configuration [[Map]]
    * @param partitions        number of partitions [[Int]]
    * @param replicationFactor replication factor [[Int]]
    * @param config            an implicit [[EmbeddedKafkaConfig]]
    */
  def createCustomTopic(
      topic: String,
      topicConfig: Map[String, String] = Map.empty,
      partitions: Int = 1,
      replicationFactor: Int = 1)(implicit config: C): Unit = {
    import scala.collection.JavaConverters._

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
        .get(topicCreationTimeout.length, topicCreationTimeout.unit)
    } finally adminClient.close()
  }

}
