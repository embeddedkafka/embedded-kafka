package net.manub.embeddedkafka

import net.manub.embeddedkafka.EmbeddedKafka._
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.tagobjects.Slow

class EmbeddedKafkaUnavailableSpec
    extends EmbeddedKafkaSpecSupport
    with BeforeAndAfterAll {
  "the publishToKafka method" should {
    "throw a KafkaUnavailableException when Kafka is unavailable when trying to publish" in {
      a[KafkaUnavailableException] shouldBe thrownBy {
        implicit val serializer: StringSerializer = new StringSerializer()
        publishToKafka("non_existing_topic", "a message")
      }
    }
  }

  "the consumeFirstStringMessageFrom method" should {
    "throw a KafkaUnavailableException when there's no running instance of Kafka" taggedAs Slow ignore {
      // TODO: This test is *really* slow. The request.max.timeout.ms in the underlying consumer should be changed.
      a[KafkaUnavailableException] shouldBe thrownBy {
        consumeFirstStringMessageFrom("non_existing_topic")
      }
    }
  }
}
