package com.sn.spark.core.consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig

trait Consumer {
  def createConsumer(): KafkaConsumer[String, Array[Byte]] = {
    val props = new Properties()
    val groupId = "demo-topic-consumer"
    props.put("group.id", groupId)
    props.put("zookeeper.connect", "localhost:2181")
    props.put("auto.offset.reset", "latest")
    props.put("consumer.timeout.ms", "120000")
    props.put("auto.commit.interval.ms", "10000")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    new KafkaConsumer[String, Array[Byte]](props)
  }
  def read(topic: String, consumer: KafkaConsumer[String, Array[Byte]]): Unit
}
