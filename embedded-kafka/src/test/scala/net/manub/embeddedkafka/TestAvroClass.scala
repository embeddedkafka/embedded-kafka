package net.manub.embeddedkafka

import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.{AvroRuntimeException, Schema}

case class TestAvroClass(var name: String) extends SpecificRecordBase {
  def this() = this("")

  override def get(i: Int): AnyRef = i match {
    case 0 => name
    case _ => throw new AvroRuntimeException("Bad index")
  }

  override def put(i: Int, v: scala.Any): Unit = i match {
    case 0 => name = v match {
      case (utf8: org.apache.avro.util.Utf8) => utf8.toString
      case _ => v.asInstanceOf[String]
    }
    case _ => throw new AvroRuntimeException("Bad index")
  }

  override def getSchema: Schema = TestAvroClass.SCHEMA$
}

object TestAvroClass {
  val SCHEMA$ = (new Schema.Parser).parse(
    """
      |{"namespace": "example",
      | "type": "record",
      | "namespace": "net.manub.embeddedkafka",
      | "name": "TestAvroClass",
      | "fields": [
      |     {"name": "name", "type": "string"}
      | ]
      |}
    """.stripMargin)
}