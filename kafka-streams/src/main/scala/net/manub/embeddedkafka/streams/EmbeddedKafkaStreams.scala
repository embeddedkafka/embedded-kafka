package net.manub.embeddedkafka.streams

import java.util.Properties

import net.manub.embeddedkafka.ops.AdminOps
import net.manub.embeddedkafka.{
  EmbeddedKafka,
  EmbeddedKafkaConfig,
  EmbeddedKafkaSupport,
  UUIDs
}
import org.apache.kafka.streams.{KafkaStreams, Topology}

import scala.jdk.CollectionConverters._

/** Helper trait for running Kafka Streams.
  * Use [[EmbeddedKafkaStreamsSupport.runStreams]] to execute your streams.
  *
  * @see [[EmbeddedKafkaStreamsSupport]]
  */
trait EmbeddedKafkaStreams
    extends EmbeddedKafkaStreamsSupport[EmbeddedKafkaConfig]
    with EmbeddedKafka {
  override protected[embeddedkafka] val streamsConfig =
    new EmbeddedStreamsConfigImpl
}

private[embeddedkafka] trait EmbeddedKafkaStreamsSupport[
    C <: EmbeddedKafkaConfig
] {
  this: EmbeddedKafkaSupport[C] with AdminOps[C] =>

  protected[embeddedkafka] def streamsConfig: EmbeddedStreamsConfig[C]

  /** Execute Kafka streams and pass a block of code that can
    * operate while the streams are active.
    * The code block can be used for publishing and consuming messages in Kafka.
    *
    * @param topicsToCreate the topics that should be created in Kafka before launching the streams.
    * @param topology       the streams topology that will be used to instantiate the streams with
    *                       a default configuration (all state directories are different and
    *                       in temp folders)
    * @param extraConfig    additional KafkaStreams configuration (overwrite existing keys in
    *                       default config)
    * @param block          the code block that will executed while the streams are active.
    *                       Once the block has been executed the streams will be closed.
    */
  def runStreams[T](
      topicsToCreate: Seq[String],
      topology: Topology,
      extraConfig: Map[String, AnyRef] = Map.empty
  )(block: => T)(implicit config: C): T =
    withRunningKafka {
      topicsToCreate.foreach(topic => createCustomTopic(topic))
      val streamId = UUIDs.newUuid().toString
      val streams = new KafkaStreams(
        topology,
        map2Properties(streamsConfig.config(streamId, extraConfig))
      )
      streams.start()
      try {
        block
      } finally {
        streams.close()
      }
    }(config)

  private def map2Properties(map: Map[String, AnyRef]): Properties = {
    val props = new Properties
    props.putAll(map.asJava)
    props
  }
}

object EmbeddedKafkaStreams extends EmbeddedKafkaStreams
