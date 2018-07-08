package com.sn.spark.core.producer
import java.util.concurrent.Future

import com.sn.spark.core.model.{Post}
import com.sn.spark.core.model.Post._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

object PostProducer extends Producer {
  override def send[T](topic: String, data: T, producer: KafkaProducer[String, Array[Byte]]): Unit = {
    data match {
      case x: Post => val queueMessage = new ProducerRecord[String, Array[Byte]](topic, serialize(x))
        val f: Future[RecordMetadata] = producer.send(queueMessage)
      case _ => System.err.print("Data Type Error")
    }
  }
}
