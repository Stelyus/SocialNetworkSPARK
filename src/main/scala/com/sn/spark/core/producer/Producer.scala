package com.sn.spark.core.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

trait Producer {

  def createProducer(): KafkaProducer[String, Array[Byte]] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    new KafkaProducer[String, Array[Byte]](props)
  }
  def send[T](topic: String, data: T, producer: KafkaProducer[String, Array[Byte]]): Unit
}
