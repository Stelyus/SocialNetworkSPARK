package com.sn.spark.core.consumer
import java.util.Collections
import java.util.concurrent.Executors

import com.sn.spark.core.model.Like
import com.sn.spark.core.model.Like.deserialize
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._

object LikeConsumer extends Consumer {}
