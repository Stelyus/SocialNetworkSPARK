import java.time.Instant

import com.sn.spark.core.model.{Id, Post, User}
import org.apache.kafka.clients.producer.KafkaProducer
import com.sn.spark.core.producer.PostProducer
import com.sn.spark.core.consumer.PostConsumer
import org.apache.log4j.BasicConfigurator

import scala.util.Random

object Main extends App {
  override def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    println("Hello World")
    val postConsumer = PostConsumer.createConsumer()
    PostConsumer.read("post-topics", postConsumer)
    val postProducer: KafkaProducer[String, Array[Byte]] = PostProducer.createProducer()
    var i: Int = 0
    // Send 100 Post on Topic posts-topic
    while (i < 100) {
      PostProducer.send[Post]("posts-topic", Post(Id[Post]("post0"), Instant.now(), Id[User]("user0"), "Some text" + i, false), postProducer)
      Thread.sleep(2000)
      i += 1
    }
    postProducer.close()
  }
}