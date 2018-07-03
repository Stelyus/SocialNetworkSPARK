package com.sn.spark.core.consumer
import java.util.Collections
import java.util.concurrent.Executors

import com.sn.spark.core.model.Post
import com.sn.spark.core.model.Post.deserialize
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._

object PostConsumer extends Consumer {
  override def read(topic: String, consumer: KafkaConsumer[String, Array[Byte]]): Unit = {
    consumer.subscribe(Collections.singletonList(topic))
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, Array[Byte]] = consumer.poll(1000)
          for (record <- records) {
            System.out.println("key: " + record.key())
            val p: Post = deserialize(record.value())
            System.out.println(p.toString())
          }
        }
      }
    })
  }
}
