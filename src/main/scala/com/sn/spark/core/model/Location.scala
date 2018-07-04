package com.sn.spark.core.model

import java.io.ByteArrayOutputStream
import java.time.Instant

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import scala.io.Source

case class Location(id: Id[Location], creationTime: Instant, author: Id[User], city: String, country: String) {
  override def toString(): String = {
    "id: " + id.value + System.lineSeparator() +
    "creationTime" + creationTime.getEpochSecond() + System.lineSeparator() +
    "author: " + author.value + System.lineSeparator() +
    "city: " + city + System.lineSeparator() +
    "country: " + country + System.lineSeparator()
  }
}

object Location {
  implicit val schema: Schema = new Parser().parse(Source.fromFile("src/resources/location.avsc").mkString)
  def serialize(location: Location): Array[Byte] = {
    // Create avro generic record object
    val genericPost: GenericRecord = new GenericData.Record(schema)
    //Put data in that generic record

    genericPost.put("id", location.id.value)
    genericPost.put("creationTime" , location.creationTime.toEpochMilli)
    genericPost.put("author", location.author.value)
    genericPost.put("city", location.city)
    genericPost.put("country", location.country)
    // Serialize generic record into byte array
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(genericPost, encoder)
    encoder.flush()
    out.close()
    out.toByteArray()
  }

  def deserialize(raw: Array[Byte]): Location = {
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(raw, null)
    val locationData: GenericRecord = reader.read(null, decoder)

    Location(Id[Location](locationData.get("id").toString), Instant.ofEpochMilli(locationData.get("creationTime").toString.toLong), Id[User](locationData.get("author").toString),
      locationData.get("city").toString, locationData.get("country").toString)
  }
}
