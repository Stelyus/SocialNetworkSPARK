package com.sn.spark.core.model

import java.io.ByteArrayOutputStream
import java.time.Instant

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import scala.io.Source

case class Message(id: Id[Message], updatedOn: Instant, receiver: Id[User], author: Id[User], text: String, deleted: Boolean) {
  override def toString: String = {
    "id: " + id.value + System.lineSeparator() +
      "updatedOn" + updatedOn.getEpochSecond + System.lineSeparator() +
      "author: " + author.value + System.lineSeparator() +
      "receiver: " + receiver.value + System.lineSeparator() + 
      "text: " + text + System.lineSeparator()
  }
}
object Message {
  implicit val schema: Schema = new Parser().parse(Source.fromFile("src/resources/message.avsc").mkString)
  def serialize(msg: Message): Array[Byte] = {
    // Create avro generic record object
    val genericMessage: GenericRecord = new GenericData.Record(schema)
    //Put data in that generic record
    genericMessage.put("id", msg.id.value)
    genericMessage.put("updatedOn" , msg.updatedOn.toEpochMilli)
    genericMessage.put("author", msg.author.value)
    genericMessage.put("receiver", msg.receiver.value)
    genericMessage.put("text", msg.text)
    genericMessage.put("deleted", msg.deleted)
    // Serialize generic record into byte array
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(genericMessage, encoder)
    encoder.flush()
    out.close()
    out.toByteArray()
  }

  def deserialize(raw: Array[Byte]): Message = {
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(raw, null)
    val msgData: GenericRecord = reader.read(null, decoder)
    Message(Id[Message](msgData.get("id").toString), Instant.ofEpochMilli(msgData.get("updatedOn").toString.toLong),
      Id[User](msgData.get("receiver").toString), Id[User](msgData.get("author").toString),
      msgData.get("text").toString, msgData.get("deleted").toString.toBoolean)
  }
}
