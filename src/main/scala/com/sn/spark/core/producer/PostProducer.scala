package com.sn.spark.core.producer

import java.time.Instant
import java.util.concurrent.Future
import java.util.{Properties, UUID}

import com.sn.spark.core.model.{Id, Post, User}
import com.sn.spark.core.model.Post._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object PostProducer extends Producer {
  override def send[T](topic: String, data: T, producer: KafkaProducer[String, Array[Byte]]): Unit = {
    data match {
      case x: Post => val queueMessage = new ProducerRecord[String, Array[Byte]](topic, serializePost(x))
        val f: Future[RecordMetadata] = producer.send(queueMessage)
        System.out.println(f.isCancelled)
        System.out.println(f.isDone);
      case _ => System.err.print("Data Type Error")
    }
  }
}
