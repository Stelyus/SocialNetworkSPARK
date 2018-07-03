package com.sn.spark.core.model

import java.io.ByteArrayOutputStream
import java.time.Instant

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import scala.io.Source

case class Id[Resource](value: String) extends AnyVal

case class Post(id: Id[Post], updatedOn: Instant, author: Id[User], text: String, deleted: Boolean) {
  override def toString(): String = {
    "id: " + id.value + System.lineSeparator() +
    "updatedOn: " + updatedOn.getEpochSecond() + System.lineSeparator() +
    "author: " + author.value + System.lineSeparator() +
    "text: " + text + System.lineSeparator()
  }
}

object Post {
  implicit val schema: Schema = new Parser().parse(Source.fromFile("src/resources/post.avsc").mkString)
  def serializePost(post: Post): Array[Byte] = {
    // Create avro generic record object
    val genericPost: GenericRecord = new GenericData.Record(schema)
    //Put data in that generic record

    genericPost.put("id", post.id.value)
    genericPost.put("updatedOn" , post.updatedOn.toEpochMilli)
    genericPost.put("author", post.author.value)
    genericPost.put("text", post.text)
    genericPost.put("deleted", post.deleted)
    // Serialize generic record into byte array
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(genericPost, encoder)
    encoder.flush()
    out.close()
    out.toByteArray()
  }

  def deserialize(raw: Array[Byte]): Post = {
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(raw, null)
    val postData: GenericRecord = reader.read(null, decoder)

    Post(Id[Post](postData.get("id").toString), Instant.ofEpochMilli(postData.get("updatedOn").toString.toLong), Id[User](postData.get("author").toString),
      postData.get("text").toString, postData.get("deleted").toString.toBoolean)
  }
}