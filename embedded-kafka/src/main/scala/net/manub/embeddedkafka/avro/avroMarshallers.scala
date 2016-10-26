package net.manub.embeddedkafka.avro

import java.io.ByteArrayOutputStream

import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.io._
import org.apache.avro.specific.{
  SpecificDatumReader,
  SpecificDatumWriter,
  SpecificRecord
}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class KafkaAvroDecoder[T <: SpecificRecord](schema: Schema,
                                            props: VerifiableProperties = null)
    extends Decoder[T] {
  private val NoInstanceReuse = null.asInstanceOf[T]
  private val NoDecoderReuse = null.asInstanceOf[BinaryDecoder]
  private val reader = new SpecificDatumReader[T](schema)

  override def fromBytes(bytes: Array[Byte]): T = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, NoDecoderReuse)
    reader.read(NoInstanceReuse, decoder)
  }
}

class KafkaAvroEncoder[T <: SpecificRecord](props: VerifiableProperties = null)
    extends Encoder[T] {
  private val NoEncoderReuse = null.asInstanceOf[BinaryEncoder]

  override def toBytes(nullableData: T): Array[Byte] = {
    Option(nullableData).fold[Array[Byte]](null) { data =>
      val writer: DatumWriter[T] = new SpecificDatumWriter[T](data.getSchema)
      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.binaryEncoder(out, NoEncoderReuse)

      writer.write(data, encoder)
      encoder.flush()
      out.close()

      out.toByteArray
    }
  }
}

class KafkaAvroDeserializer[T <: SpecificRecord](schema: Schema)
    extends Deserializer[T]
    with NoOpConfiguration
    with NoOpClose {

  private val decoder = new KafkaAvroDecoder[T](schema = schema)

  override def deserialize(topic: String, data: Array[Byte]): T =
    decoder.fromBytes(data)
}

class KafkaAvroSerializer[T <: SpecificRecord]()
    extends Serializer[T]
    with NoOpConfiguration
    with NoOpClose {

  private val encoder = new KafkaAvroEncoder[T]()

  override def serialize(topic: String, data: T): Array[Byte] =
    encoder.toBytes(data)
}

sealed trait NoOpConfiguration {
  def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
}

sealed trait NoOpClose {
  def close(): Unit = ()
}
