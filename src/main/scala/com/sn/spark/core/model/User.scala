package com.sn.spark.core.model

import java.io.ByteArrayOutputStream
import java.time.Instant

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import scala.io.Source

case class User(id: Id[User], firstName: String, lastName: String,
                email: String, nickname: String, creationTime: Instant, verified: Boolean = true) {
  override def toString(): String = {
    "id: " + id.value + System.lineSeparator() +
    "fistName: " + firstName + System.lineSeparator() +
    "lastName: " + firstName + System.lineSeparator() +
    "email: " + firstName + System.lineSeparator() +
    "nickname: " + firstName + System.lineSeparator() +
    "author: " + creationTime.getEpochSecond + System.lineSeparator() +
    "verified: " + verified + System.lineSeparator()
  }
}

object User {
  implicit val schema: Schema = new Parser().parse(Source.fromFile("src/resources/user.avsc").mkString)
  def serialize(user: User): Array[Byte] = {
    // Create avro generic record object
    val genericPost: GenericRecord = new GenericData.Record(schema)
    //Put data in that generic record

    genericPost.put("id", user.id.value)
    genericPost.put("firstName", user.firstName)
    genericPost.put("lastName", user.lastName)
    genericPost.put("email" , user.email)
    genericPost.put("nickname", user.nickname)
    genericPost.put("creationTime", user.creationTime.toEpochMilli)
    genericPost.put("verified", user.verified)

    // Serialize generic record into byte array
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(genericPost, encoder)
    encoder.flush()
    out.close()
    out.toByteArray()
  }

  def deserialize(raw: Array[Byte]): User = {
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(raw, null)
    val userData: GenericRecord = reader.read(null, decoder)

    User(Id[User](userData.get("id").toString), userData.get("firstName").toString,
      userData.get("lastName").toString, userData.get("email").toString, userData.get("nickname").toString,
      Instant.ofEpochMilli(userData.get("creationTime").toString.toLong), userData.get("verified").toString.toBoolean)
  }
}
