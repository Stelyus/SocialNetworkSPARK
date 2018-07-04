package com.sn.spark.core.model

import java.io.ByteArrayOutputStream
import java.time.Instant

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import scala.io.Source

case class Like (id: Id[Like], author: Id[User], creationTime: Instant, postId: Id[Post]) {
  override def toString(): String = {
      "id: " + id.value + System.lineSeparator() +
      "author: " + author.value + System.lineSeparator() +
      "creationTime:" + creationTime.getEpochSecond() + System.lineSeparator() +
      "postId: " + postId.value + System.lineSeparator()
  }
}

object Like {
  implicit val schema: Schema = new Parser().parse(Source.fromFile("src/resources/like.avsc").mkString)
  def serialize(like: Like): Array[Byte] = {
    // Create avro generic record object
    val genericPost: GenericRecord = new GenericData.Record(schema)
    //Put data in that generic record

    genericPost.put("id", like.id.value)
    genericPost.put("author", like.author.value)
    genericPost.put("creationTime" , like.creationTime.toEpochMilli)
    genericPost.put("postId", like.postId.value)
    // Serialize generic record into byte array
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(genericPost, encoder)
    encoder.flush()
    out.close()
    out.toByteArray()
  }

  def deserialize(raw: Array[Byte]): Like = {
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(raw, null)
    val likeData: GenericRecord = reader.read(null, decoder)

    Like(Id[Like](likeData.get("id").toString), Id[User](likeData.get("author").toString),
      Instant.ofEpochMilli(likeData.get("creationTime").toString.toLong), Id[Post](likeData.get("postId").toString))
  }
};
