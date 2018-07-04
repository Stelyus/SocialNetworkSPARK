package com.sn.spark.core.producer
import java.util.concurrent.Future

import com.sn.spark.core.model.Like
import com.sn.spark.core.model.Like.serialize
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

object LikeProducer extends Producer {
  override def send[T](topic: String, data: T, producer: KafkaProducer[String, Array[Byte]]): Unit = {
    data match {
      case x: Like => val queueMessage = new ProducerRecord[String, Array[Byte]](topic, serialize(x))
        val f: Future[RecordMetadata] = producer.send(queueMessage)
        System.out.println(f.isCancelled)
        System.out.println(f.isDone);
      case _ => System.err.print("Data Type Error")
    }
  }
}
