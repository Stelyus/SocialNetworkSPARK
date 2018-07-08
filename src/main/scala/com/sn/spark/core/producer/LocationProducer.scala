package com.sn.spark.core.producer
import java.util.concurrent.Future

import com.sn.spark.core.model.Location
import com.sn.spark.core.model.Location.serialize
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

object LocationProducer extends Producer {
  override def send[T](topic: String, data: T, producer: KafkaProducer[String, Array[Byte]]): Unit = {
    data match {
      case x: Location => val queueMessage = new ProducerRecord[String, Array[Byte]](topic, serialize(x))
        val f: Future[RecordMetadata] = producer.send(queueMessage)
      case _ => System.err.print("Data Type Error")
    }
  }
}
