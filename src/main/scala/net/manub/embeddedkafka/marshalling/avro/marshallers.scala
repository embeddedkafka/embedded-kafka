package net.manub.embeddedkafka.marshalling.avro

import java.io.ByteArrayOutputStream

import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class KafkaAvroDecoder[T <: SpecificRecord](schema: Schema, props: VerifiableProperties = null) extends Decoder[T]{
  private[this] val NoInstanceReuse = null.asInstanceOf[T]
  private[this] val NoDecoderReuse = null.asInstanceOf[BinaryDecoder]
  private[this] val reader: SpecificDatumReader[T] = new SpecificDatumReader[T](schema)

  override def fromBytes(bytes: Array[Byte]): T = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, NoDecoderReuse)
    reader.read(NoInstanceReuse, decoder)
  }
}

class KafkaAvroEncoder[T <: SpecificRecord](props: VerifiableProperties = null) extends Encoder[T] {
  private[this] val NoEncoderReuse = null.asInstanceOf[BinaryEncoder]

  override def toBytes(nullableData: T): Array[Byte] = {
    Option(nullableData).fold[Array[Byte]](null) { data  =>
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

class KafkaAvroDeserializer[T <: SpecificRecord](schema: Schema) extends Deserializer[T]{
  private[this] val decoder = new KafkaAvroDecoder[T](schema = schema)

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): T = decoder.fromBytes(data)
}

class KafkaAvroSerializer[T <: SpecificRecord]() extends Serializer[T] {
  private[this] val encoder = new KafkaAvroEncoder[T]()

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: T): Array[Byte] = encoder.toBytes(data)

  override def close(): Unit = {}
}